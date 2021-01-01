/*
   Build:

     % go get -v gopkg.in/yaml.v2
     % go get -v github.com/eclipse/paho.mqtt.golang

     % go build -o mg.`uname -m` mg.go

   Description

     This program implements a Majority Gate by listening for actions POST'ed
     from SDS entities. This program maintains a global shared list where
     actions POST'ed are stored, called "G_actions". The web handler adds
     actions to this list.

     A separate thread "f_pubThread()" scans through this list, identifying
     successful actions into the "G_pub" list. Note that each POST'ed action
     may contain multiple commands to an EC. The "f_doPublish()" function
     takes commands from this list and publishes them to ECs via MQTT.

     This program's main thread simply blocks forever at http.ListenAndServe().

   REST

     All requests need to be authenticated by setting the following HTTP
     headers :
       X-Username
       X-Password

     This program responds to the following URI :

       POST /v1?action

     Eg,

       % curl -X POST \
           --header "X-Username: scott" \
           --header "X-Password: tiger" \
           --data "@action.json" \
           http://localhost:8090/v1?action

     Where "action.json" contains

       {
         "app": "<name>"
         "validity": <seconds>,
         "timeout": <seconds>,
         "ec": "<hostname>",
         "cmd": [ "<cmd>", ... ]
       }

   Action Tags

     Once an action has achieved majority, its command(s) are published via
     MQTT. Presumably, an Edge Controller (EC) will execute it and publish a
     response. If no response is received, we need to identify this as a
     fault condition. In order to identify commands that we publish, we need
     to supply a unique tag to each command, and verify that its response
     includes the same tag.

     Thus, each command in the "G_pub" list has a state and a unique tag
     identifying it. Entries in "G_pub" are only removed after receiving a
     response, or timeout.
*/

package main

import (
  "os"
  "fmt"
  "time"
  "sync"
  "strings"
  "reflect"
  "strconv"
  "net/http"
  "math/rand"
  "io/ioutil"
  "crypto/md5"
  "encoding/json"

  "gopkg.in/yaml.v2"
  "github.com/eclipse/paho.mqtt.golang"
)

/* action received, but has not passed the majority gate */
const STATE_PENDING = 1

/*
   action won majority vote and (probably) published to mqtt, awaiting
   expiration (in case more show up)
*/
const STATE_PUBLISHED = 2

/* This data structure tracks an HTTP POST from an SDS entity */

type S_action struct {
  Username string                       // the SDS entity POST'ing this
  PostTime int64                        // wall clock time from time.UnixNano()
  Payload map[string]interface{}        // unmarshal'ed JSON structure
  ActionChecksum string                 // MD5 checksum of action in "Payload"
  State int                             // pending, published
}

/*
   This data structure tracks a single command published to an EC. We know
   we've published if PubTime is > 0.
*/

type S_pub struct {
  Msg string                            // the command
  Topic string                          // MQTT topic EC subscribes to
  PubTime int64                         // wall clock time of our MQTT publish
  TimeoutNs int64                       // how long we'll wait for a response
  Checksum string                       // MD5 checksum of this EC's command
}

var G_debug int
var G_cfg map[interface{}]interface{}
var G_event = make(chan int, 1)         // a channel to indicate event arrived

var G_mt_actions sync.Mutex             // Lock() before accessing "G_actions"
var G_actions = []S_action{}            // list of S_action structures

var G_mt_pub sync.Mutex                 // Lock() before accessing "G_pub"
var G_pub = []S_pub{}                   // messages to publish

/* ========================================================================= */

/*
   This function parses "filename" and returns a data structure of a config
   that meets some essential checks. If something goes wrong, it returns nil.
*/

func f_getConfig (filename string) map[interface{}]interface{} {

  /* load up YAML in "filename" into "cfg" */

  content, err := ioutil.ReadFile(filename)
  if (err != nil) {
    fmt.Printf ("WARNING: Cannot read %s - %s\n", filename, err)
    return(nil)
  }
  cfg := make(map[interface{}]interface{})
  err = yaml.Unmarshal(content,  &cfg)
  if (err != nil) {
    fmt.Printf ("WARNING: Cannot parse %s - %s\n", filename, err)
    return(nil)
  }

  /* validate that "cfg" has some essential items for us to function */

  if (G_debug > 0) {
    fmt.Printf ("DEBUG: f_getConfig() cfg: %v\n", cfg)
  }

  /* check the "web" config block */

  if (cfg["web"] == nil) {
    fmt.Printf ("WARNING: 'web:' not configured.\n")
    return(nil)
  }
  web := cfg["web"].(map[interface{}]interface{})
  if (web["listen"] == nil) {
    fmt.Printf ("WARNING: 'web:listen' not configured.\n")
    return(nil)
  }
  if (reflect.TypeOf(web["listen"]).String() != "int") {
    fmt.Printf ("WARNING: 'web:listen' must be int.\n")
    return(nil)
  }

  /* check the "mqtt" config block */

  if (cfg["mqtt"] == nil) {
    fmt.Printf ("WARNING: 'mqtt:' not configured.\n")
    return(nil)
  }
  mqtt_cfg := cfg["mqtt"].(map[interface{}]interface{})
  if (mqtt_cfg["server"] == nil) {
    fmt.Printf ("WARNING: 'mqtt:server' not configured.\n")
    return(nil)
  }
  if (mqtt_cfg["port"] == nil) {
    fmt.Printf ("WARNING: 'mqtt:port' not configured\n")
    return(nil)
  }
  if (reflect.TypeOf(mqtt_cfg["port"]).String() != "int") {
    fmt.Printf ("WARNING: 'mqtt:port' must be int.\n")
    return(nil)
  } 
  if (mqtt_cfg["publish_prefix"] == nil) {
    fmt.Printf ("WARNING: 'mqtt:publish_prefix' not configured\n")
    return(nil)
  }
  if (mqtt_cfg["response_prefix"] == nil) {
    fmt.Printf ("WARNING: 'mqtt:response_prefix' not configured\n")
    return(nil)
  }

  /* check that user accounts are defined */

  if (cfg["users"] == nil) {
    fmt.Printf ("WARNING: 'users:' not configured.\n")
    return(nil)
  }

  return(cfg)
}

/*
   This function checks the supplied http headers for user credentials. If
   the password's MD5 hash matches the user's account in G_cfg["users"], then
   we return the username as a string, otherwise we return an empty string.
*/

func f_authenticate(hdr http.Header) string {

  if ((hdr["X-Username"] == nil) || (hdr["X-Password"] == nil)) {
    if (G_debug > 0) {
      fmt.Printf ("DEBUG: f_authenticate() credentials not set.\n")
    }
    return("")
  }

  user := hdr["X-Username"][0]
  passwd := hdr["X-Password"][0]
  all_users := G_cfg["users"].(map[interface{}]interface{})

  if (all_users[user] == nil) {
    fmt.Printf ("NOTICE: user '%s' does not exist.\n", user)
    return("")
  }

  pw_hash := md5.Sum([]byte(passwd))
  if (fmt.Sprintf("%x", pw_hash) != all_users[user].(string)) {
    fmt.Printf ("NOTICE: incorrect password for '%s'.\n", user)
    return("")
  }

  if (G_debug > 0) {
    fmt.Printf ("NOTICE: f_authenticate() validated user '%s'.\n", user)
  }
  return(user)
}

/*
   This function is supplied the HTTP payload from a client. We expect this
   to be JSON, so we'll parse it and sanity check it (note that integers are
   reported as "float64"). If it's good, we'll add this to the global action
   list and return true, otherwise false.
*/

func f_addAction(user string, payload string) bool {

  var action map[string]interface{}
  json.Unmarshal ([]byte(payload), &action)
  if (len(action) < 1) {
    fmt.Printf ("WARNING: Could not parse JSON: %s\n", payload)
    return(false)
  }

  if (G_debug > 0) {
    fmt.Printf ("DEBUG: f_addAction() parsed elements: %d\n", len(action))
  }

  /* syntax and sanity checks */

  if (action["app"] == nil) {
    fmt.Printf ("WARNING: 'app' not specified by '%s'.\n", user)
    return(false)
  }
  if (reflect.TypeOf(action["app"]).String() != "string") {
    fmt.Printf ("WARNING: 'app' by '%s' is '%s', expecting string.\n",
                user, reflect.TypeOf(action["app"]).String())
    return(false)
  }

  if (action["validity"] == nil) {
    fmt.Printf ("WARNING: 'validity' not specified by '%s'.\n", user)
    return(false)
  }
  if (reflect.TypeOf(action["validity"]).String() != "float64") {
    fmt.Printf ("WARNING: 'validity' by '%s' is '%s', expecting float64.\n",
                user, reflect.TypeOf(action["validity"]).String())
    return(false)
  }

  if (action["ec"] == nil) {
    fmt.Printf ("WARNING: 'ec' not specified by '%s'.\n", user)
    return(false)
  }
  if (reflect.TypeOf(action["ec"]).String() != "string") {
    fmt.Printf ("WARNING: 'ec' by '%s' is '%s', expecting string.\n",
                user, reflect.TypeOf(action["ec"]).String())
    return(false)
  }

  if (action["timeout"] == nil) {
    fmt.Printf ("WARNING: 'timeout' not specified by '%s'.\n", user)
    return(false)
  }
  if (reflect.TypeOf(action["timeout"]).String() != "float64") {
    fmt.Printf ("WARNING: 'timeout' by '%s' is '%s', expecting float64.\n",
                user, reflect.TypeOf(action["timeout"]).String())
    return(false)
  }

  /* make sure that "cmd" is a string array, not array or something else */

  if (action["cmd"] == nil) {
    fmt.Printf ("WARNING: 'cmd' array not specified by '%s'.\n", user)
    return(false)
  }
  if (reflect.TypeOf(action["cmd"]).String() != "[]interface {}") {
    fmt.Printf ("WARNING: 'cmd' by '%s' is '%s', expecting []interface {}.\n",
                user, reflect.TypeOf(action["cmd"]).String())
    return(false)
  }
  cmd := action["cmd"].([]interface{})
  if (len(cmd) < 1) {
    fmt.Printf ("WARNING: 'cmd' by '%s' is empty.\n", user)
    return(false)
  }
  for i:=0 ; i < len(cmd) ; i++ {
    if (reflect.TypeOf(cmd[i]).String() != "string") {
      fmt.Printf ("WARNING: 'cmd[%d]' by '%s' is '%s', expecting 'string'.\n",
                  i, user, reflect.TypeOf(cmd[i]).String())
      return(false)
    }
  }

  /*
     calculate the MD5 checksum of the action's fields, use it to check for
     repeat POSTs (from the same "user").
  */

  raw_data := fmt.Sprintf("%s:%f:%s:%s",
                          action["app"], action["validity"],
                          action["ec"], action["cmd"])
  raw_hash := fmt.Sprintf("%x", md5.Sum([]byte(raw_data)))
  if (G_debug > 0) {
    fmt.Printf ("DEBUG: f_addAction() action checksum %s\n", raw_hash)
  }

  a := S_action{Username:user}
  a.PostTime = time.Now().UnixNano()
  a.Payload = action
  a.ActionChecksum = raw_hash
  a.State = STATE_PENDING

  repeat := false
  G_mt_actions.Lock()

  for i:=0 ; i < len(G_actions) ; i++ {
    if ((G_actions[i].Username == user) &&
        (G_actions[i].ActionChecksum == raw_hash)) {
      repeat = true
      break
    }
  }
  if (repeat) {
    fmt.Printf ("WARNING: '%s' sent repeat action, ignoring.\n", user)
  } else {
    G_actions = append(G_actions, a)
  }

  G_mt_actions.Unlock()
  return(true)
}

/*
   This function is invoked magically when an HTTP request arrives because
   we're declared as a handler in main().
*/

func f_handleWeb(w http.ResponseWriter, r *http.Request) {

  var body string
  hdr := r.Header
  if (G_debug > 0) {
    fmt.Printf ("DEBUG: f_handleWeb() method(%s) url(%s) query(%s) hdr{%s}\n",
                r.Method, r.URL.Path, r.URL.RawQuery, hdr)
  }
  var payload string
  if (r.Body != nil) {
    buf, _ := ioutil.ReadAll(r.Body)
    payload = string(buf)
  }
  if (G_debug > 0) && (len(body) > 0) {
    fmt.Printf ("DEBUG: f_handleWeb() body:%s\n", body)
  }

  switch (r.Method) {
    case "POST":
      user := f_authenticate(hdr)
      if (len(user) == 0) {
        w.WriteHeader(http.StatusForbidden)
        w.Write([]byte("Forbidden\n"))
        return
      }
      if ((r.URL.RawQuery == "action") && (len(payload) > 0) &&
          (f_addAction(user, payload))) {
        w.Write([]byte("OK\n"))
        G_event <- 1
        return
      } else {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("Bad Request\n"))
        return
      }
    default:
      w.WriteHeader(http.StatusNotImplemented)
      w.Write([]byte("Not implemented, sorry.\n"))
  }
}

/*
   This function's job is to inspect the elements in the G_actions list,
   identify similar actions that form a majority and publish their commands
   via MQTT.
*/

func f_evalActions() {

  G_mt_actions.Lock()

  for idx:=0 ; idx < len(G_actions) ; idx++ {

    if (G_debug > 0) {
      fmt.Printf ("DEBUG: f_evelActions() examining - %s@%d state:%d\n",
                  G_actions[idx].Username, G_actions[idx].PostTime,
                  G_actions[idx].State)
    }

    if (G_actions[idx].State == STATE_PENDING) {

      /* look for other SDS entities that may have POST'ed the same action */

      var match_e []int
      match_e = append(match_e, idx)
      for i:=0 ; i < len(G_actions) ; i++ {
        if (i != idx) {

          /* another SDS entity matching our action and still pending */

          if ((G_actions[i].State == STATE_PENDING) &&
              (G_actions[i].ActionChecksum == G_actions[idx].ActionChecksum)) {
            if (G_debug > 0) {
              fmt.Printf ("DEBUG: f_evalActions() %s@%d matches %s@%d\n",
                          G_actions[idx].Username, G_actions[idx].PostTime,
                          G_actions[i].Username, G_actions[i].PostTime)
            }
            match_e = append(match_e, i)
          }

          /* another SDS entity matching our action but already published */

          if ((G_actions[i].State == STATE_PUBLISHED) &&
              (G_actions[i].ActionChecksum == G_actions[idx].ActionChecksum)) {
            if (G_debug > 0) {
              fmt.Printf ("DEBUG: f_evalActions() %s@%d already published.\n",
                          G_actions[i].Username, G_actions[i].PostTime)
            }
            G_actions[idx].State = STATE_PUBLISHED
          }
        }
      }

      if (G_debug > 0) {
        fmt.Printf ("DEBUG: f_evalActions() matches:%d\n", len(match_e))
      }

      if (len(match_e) >= 2) {

        /* we have a majority ! tag matching actions with STATE_PUBLISHED */

        fmt.Printf ("NOTICE: majority action %s <%d>\n",
                    G_actions[idx].ActionChecksum, len(match_e))
        for i:=0 ; i < len(match_e) ; i++ {
          G_actions[i].State = STATE_PUBLISHED
        }

        /* add command(s) to "G_pub" queue */

        mqtt_cfg := G_cfg["mqtt"].(map[interface{}]interface{})
        pub_topic := mqtt_cfg["publish_prefix"].(string) + "/" +
                     G_actions[idx].Payload["ec"].(string)
        cmd := G_actions[idx].Payload["cmd"].([]interface{})

        G_mt_pub.Lock()
        for i:=0 ; i < len(cmd) ; i++ {
          a := S_pub{Topic:pub_topic}
          a.TimeoutNs = int64 (G_actions[idx].Payload["timeout"].(float64) *
                                1000000000.0)
          a.Msg = cmd[i].(string)
          G_pub = append(G_pub, a)
          if (G_debug > 0) {
            fmt.Printf ("DEBUG: f_evalActions() appending S_pub{%s}\n", a)
          }
        }
        G_mt_pub.Unlock()
      }
    }
  }

  if (G_debug > 0) {
    for i:= 0 ; i < len(G_actions) ; i++ {
      fmt.Printf ("DEBUG: f_evalActions() report - %s@%d %s state:%d\n",
                  G_actions[i].Username,
                  G_actions[i].PostTime,
                  G_actions[i].ActionChecksum,
                  G_actions[i].State)
    }
  }

  G_mt_actions.Unlock()
}

/*
   This function inspects the "G_pub" array, if there are any messages to
   publish, it is done now. If we haven't received a response after the
   timeout period, report a fault and remove the entry.
*/

func f_doPublish(client mqtt.Client) {

  now := time.Now().UnixNano()
  G_mt_pub.Lock()
  for idx := 0 ; idx < len(G_pub) ; idx++ {

    /* if this entry has not been published yet ... */

    if (G_pub[idx].PubTime == 0) {
      G_pub[idx].PubTime = time.Now().UnixNano()
      s := fmt.Sprintf ("%d.%s.%s", G_pub[idx].PubTime,
                        G_pub[idx].Topic, G_pub[idx].Msg)
      G_pub[idx].Checksum = fmt.Sprintf("%x", md5.Sum([]byte(s)))

      if (G_debug > 0) {
        fmt.Printf ("DEBUG: f_doPublish() publish %s->%s cksum:%s\n",
                    G_pub[idx].Msg, G_pub[idx].Topic,
                    G_pub[idx].Checksum)
      }

      msg := fmt.Sprintf("%s|%s", G_pub[idx].Checksum, G_pub[idx].Msg)
      token := client.Publish(G_pub[idx].Topic, 0, false, msg)
      token.Wait()
      if (token.Error() != nil) {
        fmt.Printf("WARNING: MQTT publish to %s failed - %s\n",
                   G_pub[idx].Topic, token.Error())
        G_pub[idx].PubTime = 0
      }
    }

    /* if this entry has been around too long ... it's a fault condition */

    if (G_pub[idx].PubTime + G_pub[idx].TimeoutNs < now) {
      fmt.Printf ("WARNING: no response from %s for '%s' after %.3fs.\n",
                  G_pub[idx].Topic, G_pub[idx].Msg,
                  float64(G_pub[idx].TimeoutNs) / 1000000000.0)
      G_pub = append(G_pub[:idx], G_pub[idx+1:]...)
      idx--
    }
  }
  G_mt_pub.Unlock()
}

func f_subscribeCallback(client mqtt.Client, msg mqtt.Message) {

  /* try locate "<tag>" */

  G_mt_pub.Lock()
  payload := string(msg.Payload())
  pos := strings.Index(payload, "|")

  if (G_debug > 0) {
    fmt.Printf("DEBUG: f_subscribeCallback() topic:%s payload:%s pos:%d\n",
               msg.Topic(), payload, pos)
  }


  if (pos > 0) {
    checksum := payload[:pos]
    response := payload[pos+1:]

    for idx:=0 ; idx < len(G_pub) ; idx++ {
      if (G_pub[idx].Checksum == checksum) {
        duration_ns := time.Now().UnixNano() - G_pub[idx].PubTime
        fmt.Printf ("NOTICE: response %s->'%s' duration:%.3fs\n",
                    msg.Topic(), response,
                    (float64)(duration_ns) / 1000000000.0)
        G_pub = append(G_pub[:idx], G_pub[idx+1:]...)
        break
      }
    }
  }
  G_mt_pub.Unlock()
}

/*
   This thread's main job is to examine incoming events and publish to MQTT.
   Idealy, it never returns, but will if a fatal error occurs
*/

func f_pubThread() {

  /* setup an mqtt connection so we can publish to it */

  seed := rand.NewSource(time.Now().UnixNano() * int64(os.Getpid()))
  rgenerator := rand.New(seed)
  clientId := strconv.Itoa(rgenerator.Int())

  mqtt_cfg := G_cfg["mqtt"].(map[interface{}]interface{})
  broker := mqtt_cfg["server"].(string) + ":" +
            strconv.Itoa(mqtt_cfg["port"].(int))
  opts := mqtt.NewClientOptions()
  opts.AddBroker("tcp://" + broker)
  opts.SetClientID(clientId)

  if (mqtt_cfg["username"] != nil) {
    opts.SetUsername(mqtt_cfg["username"].(string))
  }
  if (mqtt_cfg["password"] != nil) {
    opts.SetPassword(mqtt_cfg["password"].(string))
  }

  client := mqtt.NewClient(opts)
  result := client.Connect()
  result.Wait()
  if (result.Error() != nil) {
    fmt.Printf("FATAL! Cannot connect to %s - %s\n", broker, result.Error())
    os.Exit(1)
  }
  fmt.Printf("NOTICE: MQTT connected to %s.\n", broker)

  result = client.Subscribe(mqtt_cfg["response_prefix"].(string), 0,
                            f_subscribeCallback)
  result.Wait()
  if (result.Error() != nil) {
    fmt.Printf("FATAL! Cannot subscribe to %s - %s\n",
               mqtt_cfg["response_prefix"].(string), result)
    os.Exit(1)
  }
  fmt.Printf("NOTICE: MQTT subscribed to %s.\n",
             mqtt_cfg["response_prefix"].(string))

  /* now enter our main loop ... wait for events POST'ed, or do admin tasks */

  for (true) {
    select {
      case _ = <- G_event:
        if (G_debug > 0) {
          fmt.Printf ("DEBUG: f_pubThread() got an event\n")
        }
        f_evalActions()
        f_doPublish(client)

      case <- time.After(1 * time.Second):

        /* if MQTT is disconnected, try connect now */

        if (client.IsConnectionOpen() == false) {
          result := client.Connect()
          result.Wait()
          if (result.Error() == nil) {
            fmt.Printf("NOTICE: reconnected to %s.\n", broker)
          } else {
            fmt.Printf("WARNING: reconnect to %s failed - %s\n",
                       broker, result.Error())
          }

          result = client.Subscribe(mqtt_cfg["response_prefix"].(string), 0,
                                    f_subscribeCallback)
          result.Wait()
          if (result.Error != nil) {
            fmt.Printf("WARNING: Cannot subscribe to %s - %s\n",
                       mqtt_cfg["response_prefix"].(string), result)
          }
        }

        /* inspect G_actions, clear out retired (or faulted) entries */

        now := time.Now().UnixNano()
        G_mt_actions.Lock()

        for idx:=0 ; idx < len(G_actions) ; idx++ {
          validity := G_actions[idx].Payload["validity"].(float64)
          cutoff := G_actions[idx].PostTime + int64(validity * 1000000000.0)
          if (now > cutoff) {
            if (G_debug > 0) {
              fmt.Printf ("DEBUG: f_pubThread() retired %s@%d state:%d {%v}\n",
                          G_actions[idx].Username, G_actions[idx].PostTime,
                          G_actions[idx].State, G_actions[idx].Payload)
            }
            if (G_actions[idx].State == STATE_PENDING) {
              fmt.Printf ("WARNING: no majority vote for %s@%d {%v}\n",
                          G_actions[idx].Username,
                          G_actions[idx].PostTime,
                          G_actions[idx].Payload)
            }

            G_actions = append(G_actions[:idx], G_actions[idx+1:]...)
          }
        }

        G_mt_actions.Unlock()

        /* inspect G_pub, see if anything needs to be done */

        if (client.IsConnectionOpen()) {
          f_doPublish(client)
        }
    }
  }
}

/* ========================================================================= */

func main () {
  G_debug = 0
  if (len(os.Getenv("DEBUG")) > 0) {
    G_debug, _ = strconv.Atoi(os.Getenv("DEBUG"))
  }

  if (len(os.Args) != 2) {
    fmt.Printf("Usage: %s <config.yaml>\n", os.Args[0])
    os.Exit(1)
  }

  G_cfg = f_getConfig (os.Args[1])
  if (G_cfg == nil) {
    fmt.Printf ("FATAL! No working configuration.\n")
    os.Exit(1)
  }

  /* run the background thread that performs publish logic */

  go f_pubThread()

  /* now setup our webserver */

  web := G_cfg["web"].(map[interface{}]interface{})
  listen_port := ":" + strconv.Itoa (web["listen"].(int))
  http.HandleFunc ("/v1", f_handleWeb)

  fmt.Printf ("NOTICE: Starting webserver on %d.\n", web["listen"])
  err := http.ListenAndServe (listen_port, nil)
  if (err != nil) {
    fmt.Printf ("FATAL! Cannot start webserver - %s\n", err)
    os.Exit (1)
  }
}

