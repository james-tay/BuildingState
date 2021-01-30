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

     This program responds to the following URIs :

       GET /health              # for kubernetes readiness/liveness checks
       GET /metrics             # for prometheus to scrape metrics
       POST /v1?action          # for SDS entities to POST requests

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

   Configuration

     The MG is supplied a single YAML config file. The following elements
     are implemented.

     mqtt:
       server: <string>
       port: <num>
       username: <string>
       password: <string>
       publish_prefix: <string>
       response_prefix: <string>
     users:
       <string>: <string>               eg, joe: <md5 hash>
       ...
     web:
       listen: <num>
     apps:
       - name: <string>
         majority: <num>
         ec:
           - hostname: <string>
             commands:
               - <regex>
               - ...
           - ...
       - ...
*/

package main

import (
  "os"
  "fmt"
  "time"
  "sync"
  "regexp"
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

/* logging levels */

const LOG_FATAL = 1             // program to exit immediately
const LOG_WARN = 2              // unusual condition, can be retried or ignored
const LOG_NOTICE = 3            // normal condition, but worth taking note
const LOG_DEBUG = 4             // function call tracing

/* action received, but has not passed the majority gate */
const STATE_PENDING = 1

/*
   action won majority vote and (probably) published to mqtt, awaiting
   expiration (in case more show up)
*/
const STATE_PUBLISHED = 2

/*
   The f_pubThread()'s main loop should run every 1 second, which updates
   "G_pubThread_heartbeat". Fire a watchdog timeout if no updates occur after
   this number of seconds.
*/
const WATCHDOG_TIMEOUT = 5

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

/*
   This data structure tracks performance counters that will be displayed
   at our /metrics URI.
*/

type S_metrics struct {
  mg_auth_unset int64                   // f_authenticate() no creds supplied
  mg_auth_success int64                 // f_authenticate() success
  mg_auth_fail int64                    // f_authenticate() fails
  mg_cmd_allowed int64                  // f_cmdAllowed() returned true
  mg_cmd_denied int64                   // f_cmdAllowed() returned false
  mg_cmd_acked int64                    // commands ACK'ed by ECs
  mg_action_posts int64                 // calls to f_addAction()
  mg_action_no_majority int64           // actions with no majority votes
  mg_action_retired int64               // successfully completed actions
  mg_mqtt_published int64               // messages published via MQTT
  mg_mqtt_faults int64                  // number of MQTT publish faults
  mg_mqtt_received int64                // number of MQTT messages received
  mg_mqtt_reconnects int64              // number of MQTT reconnects
}

var G_debug int
var G_cfg map[interface{}]interface{}
var G_logFile *os.File                  // if defined, write logs to this file
var G_logSize int64                     // current size of our log file
var G_logMax int64                      // threshold to rotate our logfile
var G_event = make(chan int, 1)         // a channel to indicate event arrived
var G_metrics = S_metrics{}             // runtime metrics
var G_pubThread_heartbeat int64         // f_pubThread() writes wallclock time

var G_mt_actions sync.Mutex             // Lock() before accessing "G_actions"
var G_actions = []S_action{}            // list of S_action structures

var G_mt_pub sync.Mutex                 // Lock() before accessing "G_pub"
var G_pub = []S_pub{}                   // messages to publish

/* ========================================================================= */

/* Our general purpose logging function */

func f_log (level int, msg string) {
  t := time.Now()
  timestamp := fmt.Sprintf("%d-%d-%d %02d:%02d:%02d.%03d",
                           t.Year(), t.Month(), t.Day(),
                           t.Hour(), t.Minute(), t.Second(),
                           t.Nanosecond() / 1000000)
  loglevel := string("UNKNOWN")
  if (level == LOG_FATAL) { loglevel = "FATAL" }
  if (level == LOG_WARN) { loglevel = "WARNING" }
  if (level == LOG_NOTICE) { loglevel = "NOTICE" }
  if (level == LOG_DEBUG) { loglevel = "DEBUG" }

  s := fmt.Sprintf ("%s [%s] %s\n", timestamp, loglevel, msg)
  if (G_logFile == nil) {
    fmt.Print (s)
  } else {
    amt, err := G_logFile.WriteString(s)
    if (err != nil) {
      fmt.Printf("WARNING: Cannot write log - %s\n", err)
    } else {
      G_logSize = G_logSize + int64(amt)
      if (G_logSize > G_logMax) {

        /* log file is too big, time to rotate */

        logging := G_cfg["logging"].(map[interface{}]interface{})
        G_logFile.Close()
        old := logging["file"].(string) + ".old"
        err := os.Rename(logging["file"].(string), old)
        if (err != nil) {
          f_log(LOG_FATAL, fmt.Sprintf("Cannot rename %s to %s - %s",
                                       logging["file"].(string), old, err))
          os.Exit(1)
        }

        /* open a new file for logging, die if we can't */

        f, err := os.OpenFile (logging["file"].(string),
                               os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if (err != nil) {
          f_log(LOG_FATAL, fmt.Sprintf("Cannot append to %s - %s",
                                       logging["file"].(string), err))
          os.Exit(1)
        }
        G_logFile = f
        G_logSize = 0
      }
    }
  }
}

/*
   This function parses "filename" and returns a data structure of a config
   that meets some essential checks. If something goes wrong, it returns nil.
*/

func f_getConfig (filename string) map[interface{}]interface{} {

  /* load up YAML in "filename" into "cfg" */

  content, err := ioutil.ReadFile(filename)
  if (err != nil) {
    f_log(LOG_WARN, fmt.Sprintf("Cannot read %s - %s", filename, err))
    return(nil)
  }
  cfg := make(map[interface{}]interface{})
  err = yaml.Unmarshal(content,  &cfg)
  if (err != nil) {
    f_log(LOG_WARN, fmt.Sprintf("Cannot parse %s - %s", filename, err))
    return(nil)
  }

  /* validate that "cfg" has some essential items for us to function */

  if (G_debug > 0) {
    f_log(LOG_DEBUG, fmt.Sprintf("f_getConfig() cfg: %v", cfg))
  }

  /* check the "web" config block */

  if (cfg["web"] == nil) {
    f_log(LOG_WARN, "'web:' not configured.")
    return(nil)
  }
  web := cfg["web"].(map[interface{}]interface{})
  if (web["listen"] == nil) {
    f_log(LOG_WARN, "'web:listen' not configured.")
    return(nil)
  }
  if (reflect.TypeOf(web["listen"]).String() != "int") {
    f_log(LOG_WARN, "'web:listen' must be int.")
    return(nil)
  }

  /* check the "mqtt" config block */

  if (cfg["mqtt"] == nil) {
    f_log(LOG_WARN, "'mqtt:' not configured.")
    return(nil)
  }
  mqtt_cfg := cfg["mqtt"].(map[interface{}]interface{})
  if (mqtt_cfg["server"] == nil) {
    f_log(LOG_WARN, "'mqtt:server' not configured.")
    return(nil)
  }
  if (mqtt_cfg["port"] == nil) {
    f_log(LOG_WARN, "'mqtt:port' not configured")
    return(nil)
  }
  if (reflect.TypeOf(mqtt_cfg["port"]).String() != "int") {
    f_log(LOG_WARN, "'mqtt:port' must be int.")
    return(nil)
  } 
  if (mqtt_cfg["publish_prefix"] == nil) {
    f_log(LOG_WARN, "'mqtt:publish_prefix' not configured")
    return(nil)
  }
  if (mqtt_cfg["response_prefix"] == nil) {
    f_log(LOG_WARN, "'mqtt:response_prefix' not configured")
    return(nil)
  }

  /* check that user accounts are defined */

  if (cfg["users"] == nil) {
    f_log (LOG_WARN, "'users:' not configured.")
    return(nil)
  }

  /* check that apps are defined */

  if (cfg["apps"] == nil) {
    f_log(LOG_WARN, "'apps:' not configured.")
    return(nil)
  }
  if (reflect.TypeOf(cfg["apps"]).String() != "[]interface {}") {
    f_log(LOG_WARN, fmt.Sprintf("'apps:' must be []interface {}, not '%s'",
                                reflect.TypeOf(cfg["apps"]).String()))
    return(nil)
  }

  /* check that each app has its required fields */

  apps := cfg["apps"].([]interface{})
  for a_idx := 0 ; a_idx < len(apps) ; a_idx++ {
    cur_app := apps[a_idx].(map[interface{}]interface{})

    if (cur_app["name"] == nil) {
      f_log(LOG_WARN, fmt.Sprintf("'apps[%d]:name' not configured", a_idx))
      return(nil)
    }
    if (reflect.TypeOf(cur_app["name"]).String() != "string") {
      f_log(LOG_WARN, fmt.Sprintf("'apps[%d]:name' must be string", a_idx))
      return(nil)
    }

    if (cur_app["majority"] == nil) {
      f_log(LOG_WARN,
            fmt.Sprintf("'apps[%d]:majority' not configured", a_idx))
      return(nil)
    }
    if (reflect.TypeOf(cur_app["majority"]).String() != "int") {
      f_log(LOG_WARN, fmt.Sprintf("'apps[%d]:majority' must be int", a_idx))
      return(nil)
    }

    if (cur_app["ec"] == nil) {
      f_log(LOG_WARN, fmt.Sprintf("'apps[%d]:ec' not configured", a_idx))
      return(nil)
    }
    if (reflect.TypeOf(cur_app["ec"]).String() != "[]interface {}") {
      f_log(LOG_WARN,
            fmt.Sprintf("'apps[%d]:majority ' must be []interface {}", a_idx))
      return(nil)
    }

    ecs := cur_app["ec"].([]interface{})
    for e_idx := 0 ; e_idx < len(ecs) ; e_idx++ {

      /* check that this ec has its required fields */

      cur_ec := ecs[e_idx].(map[interface{}]interface{})
      if (cur_ec["hostname"] == nil) {
        f_log(LOG_WARN,
              fmt.Sprintf("'apps[%d]:ec[%d]:hostname' not configured",
                          a_idx, e_idx))
        return(nil)
      }
      if (reflect.TypeOf(cur_ec["hostname"]).String() != "string") {
        f_log(LOG_WARN,
              fmt.Sprintf("'apps[%d]:ec[%d]:hostname' must be string",
                          a_idx, e_idx))
        return(nil)
      }

      if (cur_ec["commands"] == nil) {
        f_log(LOG_WARN,
              fmt.Sprintf("'apps[%d]:ec[%d]:commands' not configured",
                          a_idx, e_idx))
        return(nil)
      }
      if (reflect.TypeOf(cur_ec["commands"]).String() != "[]interface {}") {
        f_log(LOG_WARN,
              fmt.Sprintf("'apps[%d]:ec[%d]:commands' must be []interface {}",
                          a_idx, e_idx))
        return(nil)
      }

      cmds := cur_ec["commands"].([]interface{})
      for c_idx := 0 ; c_idx < len(cmds) ; c_idx++ {

        /* check that each command (regex) compiles successfully */

        _, err := regexp.Compile(cmds[c_idx].(string))
        if (err != nil) {
          f_log(LOG_WARN,
                fmt.Sprintf("apps[%d]:ec[%d]:commands[%d] bad regex - %s",
                            a_idx, e_idx, c_idx, err))
          return(nil)
        }
      }
    }
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
      f_log(LOG_DEBUG, "f_authenticate() credentials not set.")
    }
    G_metrics.mg_auth_unset++
    return("")
  }

  user := hdr["X-Username"][0]
  passwd := hdr["X-Password"][0]
  all_users := G_cfg["users"].(map[interface{}]interface{})

  if (all_users[user] == nil) {
    f_log(LOG_NOTICE, fmt.Sprintf("user '%s' does not exist.", user))
    G_metrics.mg_auth_fail++
    return("")
  }

  pw_hash := md5.Sum([]byte(passwd))
  if (fmt.Sprintf("%x", pw_hash) != all_users[user].(string)) {
    f_log(LOG_NOTICE, fmt.Sprintf("incorrect password for '%s'.", user))
    G_metrics.mg_auth_fail++
    return("")
  }

  if (G_debug > 0) {
    f_log(LOG_NOTICE, fmt.Sprintf("f_authenticate() validated user '%s'",
                                  user))
    G_metrics.mg_auth_success++
  }
  return(user)
}

/*
   This function is called from f_addAction(), its job is to verified if
   "cmd" may be execute by "app", on "ec". It returns true if so, otherwise
   false.
*/

func f_cmdAllowed(app string, ec string, cmd string) bool {

  /* iterate through all "apps" in our configuration */

  apps := G_cfg["apps"].([]interface{})
  for a_idx := 0 ; a_idx < len(apps) ; a_idx++ {
    cur_app := apps[a_idx].(map[interface{}]interface{})
    if (cur_app["name"].(string) == app) {

      /* iterate through all "ec"s used by "cur_app" */

      ecs := cur_app["ec"].([]interface{})
      for e_idx := 0 ; e_idx < len(ecs) ; e_idx++ {
        cur_ec := ecs[e_idx].(map[interface{}]interface{})
        if (cur_ec["hostname"].(string) == ec) {

          /* iterate through all "commands" allowed on this ec */

          cmds := cur_ec["commands"].([]interface{})
          for c_idx := 0 ; c_idx < len(cmds) ; c_idx++ {
            cmd_regex := cmds[c_idx].(string)
            match, _ := regexp.MatchString(cmd_regex, cmd)
            if (match) {
              if (G_debug > 0) {
                f_log(LOG_DEBUG,
                      fmt.Sprintf("f_cmdAllowed() cmd:'%s' matches regex '%s'",
                                  cmd, cmd_regex))
              }
              G_metrics.mg_cmd_allowed++ ;
              return(true)
            }
          }
        }
      }
    }
  }

  f_log(LOG_WARN, fmt.Sprintf("app:%s ec:%s '%s' not allowed.",
                              app, ec, cmd))
  G_metrics.mg_cmd_denied++ ;
  return(false)
}

/*
   This function is called from f_handleWeb() and supplied the HTTP payload
   from a client. We expect this to be JSON, so we'll parse it and sanity
   check it (note that integers are reported as "float64"). If it's good,
   we'll add this to the global action list and return true, otherwise false.
*/

func f_addAction(user string, payload string) bool {

  var action map[string]interface{}
  json.Unmarshal ([]byte(payload), &action)
  if (len(action) < 1) {
    f_log(LOG_WARN, fmt.Sprintf("Could not parse JSON: %s", payload))
    return(false)
  }
  G_metrics.mg_action_posts++

  if (G_debug > 0) {
    f_log(LOG_DEBUG, fmt.Sprintf("f_addAction() parsed elements: %d",
                                 len(action)))
  }

  /* syntax and sanity checks */

  if (action["app"] == nil) {
    f_log(LOG_WARN, fmt.Sprintf("'app' not specified by '%s'.", user))
    return(false)
  }
  if (reflect.TypeOf(action["app"]).String() != "string") {
    f_log(LOG_WARN, fmt.Sprintf("'app' by '%s' is '%s', expecting string.",
                                user, reflect.TypeOf(action["app"]).String()))
    return(false)
  }

  if (action["validity"] == nil) {
    f_log(LOG_WARN, fmt.Sprintf("'validity' not specified by '%s'.", user))
    return(false)
  }
  if (reflect.TypeOf(action["validity"]).String() != "float64") {
    f_log(LOG_WARN,
          fmt.Sprintf("'validity' by '%s' is '%s', expecting float64.",
                      user, reflect.TypeOf(action["validity"]).String()))
    return(false)
  }

  if (action["ec"] == nil) {
    f_log(LOG_WARN, fmt.Sprintf("'ec' not specified by '%s'.", user))
    return(false)
  }
  if (reflect.TypeOf(action["ec"]).String() != "string") {
    f_log(LOG_WARN, fmt.Sprintf("'ec' by '%s' is '%s', expecting string.",
                                user, reflect.TypeOf(action["ec"]).String()))
    return(false)
  }

  if (action["timeout"] == nil) {
    f_log(LOG_WARN, fmt.Sprintf("'timeout' not specified by '%s'.", user))
    return(false)
  }
  if (reflect.TypeOf(action["timeout"]).String() != "float64") {
    f_log(LOG_WARN,
         fmt.Sprintf("'timeout' by '%s' is '%s', expecting float64.",
                     user, reflect.TypeOf(action["timeout"]).String()))
    return(false)
  }

  /* make sure that "cmd" is a string array, not array or something else */

  if (action["cmd"] == nil) {
    f_log(LOG_WARN, fmt.Sprintf("'cmd' array not specified by '%s'.", user))
    return(false)
  }
  if (reflect.TypeOf(action["cmd"]).String() != "[]interface {}") {
    f_log(LOG_WARN,
          fmt.Sprintf("'cmd' by '%s' is '%s', expecting []interface {}.",
                      user, reflect.TypeOf(action["cmd"]).String()))
    return(false)
  }
  cmd := action["cmd"].([]interface{})
  if (len(cmd) < 1) {
    f_log(LOG_WARN, fmt.Sprintf("'cmd' by '%s' is empty.", user))
    return(false)
  }

  /* examine each of the "cmd" entries, type check and ACL check them */

  for i:=0 ; i < len(cmd) ; i++ {
    if (reflect.TypeOf(cmd[i]).String() != "string") {
      f_log(LOG_WARN,
            fmt.Sprintf("'cmd[%d]' by '%s' is '%s', expecting 'string'.",
                        i, user, reflect.TypeOf(cmd[i]).String()))
      return(false)
    }
    if (f_cmdAllowed(action["app"].(string),
                     action["ec"].(string),
                     cmd[i].(string)) == false) {
      f_log(LOG_WARN,
            fmt.Sprintf("'cmd[%d]' by '%s', '%s' is not allowed.",
                        i, user, cmd[i]))
      return(false)
    }
  }

  /*
     calculate the MD5 checksum of the action's fields, use it to check for
     repeat POSTs (from the same "user"). This MD5 checksum is considered
     opaque.
  */

  raw_data := fmt.Sprintf("%s:%f:%s:%s",
                          action["app"], action["validity"],
                          action["ec"], action["cmd"])
  raw_hash := fmt.Sprintf("%x", md5.Sum([]byte(raw_data)))
  if (G_debug > 0) {
    f_log(LOG_DEBUG, fmt.Sprintf("f_addAction() action checksum %s", raw_hash))
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
    f_log(LOG_WARN, fmt.Sprintf("'%s' sent repeat action, ignoring.", user))
  } else {
    G_actions = append(G_actions, a)
  }

  G_mt_actions.Unlock()
  return(true)
}

/*
   This function is called when kubernetes is trying to perform readiness
   and liveness HTTP checks. What's important is for us to return 200 to
   indicate good health, otherwise 500 to indicate a fault condition. The
   fact that we were called indicates the webserver is running. Thus check
   that we're able to acquire mutex locks and that f_pubThread() is alive.
*/

func f_health(w http.ResponseWriter) {

  G_mt_actions.Lock()
  G_mt_actions.Unlock()
  G_mt_pub.Lock()
  G_mt_pub.Unlock()

  last_heartbeat := time.Now().UnixNano() - G_pubThread_heartbeat
  if (last_heartbeat / 1000 / 1000 / 1000 < WATCHDOG_TIMEOUT) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK\n"))
  } else {
    w.WriteHeader(http.StatusInternalServerError)
    w.Write([]byte("Failed\n"))
  }
}

/*
   This function is invoked from f_handleWeb(). Our job is to prove that we're
   alive and to print some internal performance metrics. Note that part of
   proving that we're alive, is to try acquiring mutex locks
*/

func f_metrics(w http.ResponseWriter) {

  /* print the current number of G_actions and G_pub entries */

  G_mt_actions.Lock()
  s := fmt.Sprintf ("mg_action_entries %d\n", len(G_actions))
  G_mt_actions.Unlock()

  G_mt_pub.Lock()
  s = s + fmt.Sprintf ("mg_pub_entries %d\n", len(G_pub))
  G_mt_pub.Unlock()

  s = s + fmt.Sprintf("mg_cur_log_size %d\n", G_logSize)

  /* now print out everything in G_metrics using reflection*/

  values := reflect.ValueOf(&G_metrics).Elem()
  mtypes := values.Type()

  for i:=0 ; i < values.NumField() ; i++ {
    field := values.Field(i)
    s = s + fmt.Sprintf ("%v %v\n", mtypes.Field(i).Name, field)
  }

  w.Write([]byte(s))
}

/*
   This function is invoked magically when an HTTP request arrives because
   we're declared as a handler in main().
*/

func f_handleWeb(w http.ResponseWriter, r *http.Request) {

  var body string
  hdr := r.Header
  if (G_debug > 0) {
    f_log(LOG_DEBUG,
          fmt.Sprintf("f_handleWeb() method(%s) url(%s) query(%s) hdr{%s}",
                      r.Method, r.URL.Path, r.URL.RawQuery, hdr))
  }
  var payload string
  if (r.Body != nil) {
    buf, _ := ioutil.ReadAll(r.Body)
    payload = string(buf)
  }
  if (G_debug > 0) && (len(body) > 0) {
    f_log(LOG_DEBUG, fmt.Sprintf("f_handleWeb() body:%s", body))
  }

  switch (r.Method) {
    case "GET":
      if (r.URL.Path == "/health") {
        f_health(w)
      }
      if (r.URL.Path == "/metrics") {
        f_metrics(w)
      }
      return
    case "POST":
      user := f_authenticate(hdr)
      if (len(user) == 0) {
        w.WriteHeader(http.StatusForbidden)
        w.Write([]byte("Forbidden\n"))
        return
      }
      if ((r.URL.RawQuery == "action") && (len(payload) > 0) &&
          (f_addAction(user, payload))) {
        w.WriteHeader(http.StatusAccepted)
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
   This function scans the G_cfg structure to location "app" and returns the
   number required to be considered a majority vote. If "app" cannot be
   found, we return -1.
*/

func f_getAppMajority(app string) int {

  /* iterate through "apps" */

  apps := G_cfg["apps"].([]interface{})
  for idx:=0 ; idx < len(apps) ; idx++ {

    a := apps[idx].(map[interface{}]interface {})
    if ((a["name"] != nil) && (a["majority"] != nil)) {
      if (app == a["name"].(string)) {
        if (G_debug > 0) {
          f_log(LOG_DEBUG, fmt.Sprintf("f_getAppMajority() app:%s majority:%d",
                                       app, a["majority"].(int)))
        }
        return (a["majority"].(int))
      }
    }
  }
  f_log(LOG_WARN, fmt.Sprintf("app '%s' majority is unspecified", app))
  return(-1)
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
      f_log(LOG_DEBUG,
            fmt.Sprintf("f_evalActions() examining %d: %s@%d state:%d app:%s",
                        idx, G_actions[idx].Username, G_actions[idx].PostTime,
                        G_actions[idx].State, G_actions[idx].Payload["app"]))
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
              f_log(LOG_DEBUG,
                    fmt.Sprintf ("f_evalActions() %d: %s@%d matches %s@%d", i,
                                 G_actions[idx].Username,
                                 G_actions[idx].PostTime,
                                 G_actions[i].Username,
                                 G_actions[i].PostTime))
            }
            match_e = append(match_e, i)
          }

          /* another SDS entity matching our action but already published */

          if ((G_actions[i].State == STATE_PUBLISHED) &&
              (G_actions[i].ActionChecksum == G_actions[idx].ActionChecksum)) {
            if (G_debug > 0) {
              f_log(LOG_DEBUG,
                    fmt.Sprintf("f_evalActions() %d: %s@%d already published",
                                i, G_actions[i].Username,
                                G_actions[i].PostTime))
            }
            G_actions[idx].State = STATE_PUBLISHED
          }
        }
      }

      if (G_debug > 0) {
        f_log(LOG_DEBUG, fmt.Sprintf("f_evalActions() idx:%d match_e:%v",
                                     idx, match_e))
      }

      majority := f_getAppMajority(G_actions[idx].Payload["app"].(string))
      if ((majority > 0) && (len(match_e) >= majority)) {

        /* we have a majority ! tag matching actions with STATE_PUBLISHED */

        f_log(LOG_NOTICE, fmt.Sprintf("majority action %s idx:%d match_e:%d",
                                      G_actions[idx].ActionChecksum, idx,
                                      len(match_e)))
        for i:=0 ; i < len(match_e) ; i++ {
          x := match_e[i]
          if (G_debug > 0) {
            f_log(LOG_DEBUG,
                  fmt.Sprintf("f_evalActions() G_actions[%d] STATE_PUBLISHED",
                              x))
          }
          G_actions[x].State = STATE_PUBLISHED
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
            f_log(LOG_DEBUG, fmt.Sprintf("f_evalActions() appending S_pub:%v",
                                         a))
          }
        }
        G_mt_pub.Unlock()
      }
    }
  }

  if (G_debug > 0) {
    for i:= 0 ; i < len(G_actions) ; i++ {
      f_log(LOG_DEBUG,
            fmt.Sprintf("f_evalActions() report - %s@%d %s state:%d",
                        G_actions[i].Username,
                        G_actions[i].PostTime,
                        G_actions[i].ActionChecksum,
                        G_actions[i].State))
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
        f_log(LOG_DEBUG, fmt.Sprintf("f_doPublish() publish '%s'->%s cksum:%s",
                                     G_pub[idx].Msg, G_pub[idx].Topic,
                                     G_pub[idx].Checksum))
      }

      msg := fmt.Sprintf("%s|%s", G_pub[idx].Checksum, G_pub[idx].Msg)
      token := client.Publish(G_pub[idx].Topic, 0, false, msg)
      token.Wait()
      if (token.Error() != nil) {
        f_log(LOG_WARN, fmt.Sprintf("MQTT publish to %s failed - %s",
                                    G_pub[idx].Topic, token.Error()))
        G_pub[idx].PubTime = 0
        G_metrics.mg_mqtt_faults++
      } else {
        G_metrics.mg_mqtt_published++
      }
    }

    /* if this entry has been around too long ... it's a fault condition */

    if (G_pub[idx].PubTime + G_pub[idx].TimeoutNs < now) {
      f_log(LOG_WARN, fmt.Sprintf("no response from %s for '%s' after %.3fs.",
                                  G_pub[idx].Topic, G_pub[idx].Msg,
                                  float64(G_pub[idx].TimeoutNs) / 1000000000.0))
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
    f_log(LOG_DEBUG,
          fmt.Sprintf("f_subscribeCallback() topic:%s payload:%s pos:%d",
                      msg.Topic(), payload, pos))
  }
  G_metrics.mg_mqtt_received++

  if (pos > 0) {
    checksum := payload[:pos]
    response := payload[pos+1:]

    for idx:=0 ; idx < len(G_pub) ; idx++ {
      if (G_pub[idx].Checksum == checksum) {
        duration_ns := time.Now().UnixNano() - G_pub[idx].PubTime
        f_log(LOG_NOTICE, fmt.Sprintf("response %s->'%s' duration:%.3fs",
                                      msg.Topic(), response,
                                      (float64)(duration_ns) / 1000000000.0))
        G_pub = append(G_pub[:idx], G_pub[idx+1:]...)
        G_metrics.mg_cmd_acked++
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
    f_log(LOG_FATAL, fmt.Sprintf("Cannot connect to %s - %s",
                                 broker, result.Error()))
    os.Exit(1)
  }
  f_log(LOG_NOTICE, fmt.Sprintf("MQTT connected to %s.", broker))

  result = client.Subscribe(mqtt_cfg["response_prefix"].(string), 0,
                            f_subscribeCallback)
  result.Wait()
  if (result.Error() != nil) {
    f_log(LOG_FATAL, fmt.Sprintf("Cannot subscribe to %s - %s",
                                 mqtt_cfg["response_prefix"].(string), result))
    os.Exit(1)
  }
  f_log(LOG_NOTICE, fmt.Sprintf("MQTT subscribed to %s.",
                                mqtt_cfg["response_prefix"].(string)))

  /* now enter our main loop ... wait for events POST'ed, or do admin tasks */

  for (true) {
    G_pubThread_heartbeat = time.Now().UnixNano()
    select {
      case _ = <- G_event:
        if (G_debug > 0) {
          f_log(LOG_DEBUG, fmt.Sprintf("f_pubThread() got an event"))
        }
        f_evalActions()
        f_doPublish(client)

      case <- time.After(1 * time.Second):

        /* if MQTT is disconnected, try connect now */

        if (client.IsConnectionOpen() == false) {
          G_metrics.mg_mqtt_reconnects++
          result := client.Connect()
          result.Wait()
          if (result.Error() == nil) {
            f_log(LOG_NOTICE, fmt.Sprintf("reconnected to %s.", broker))
          } else {
            f_log(LOG_WARN, fmt.Sprintf("reconnect to %s failed - %s",
                                        broker, result.Error()))
          }

          result = client.Subscribe(mqtt_cfg["response_prefix"].(string), 0,
                                    f_subscribeCallback)
          result.Wait()
          if (result.Error != nil) {
            f_log(LOG_WARN, fmt.Sprintf("Cannot subscribe to %s - %s",
                                        mqtt_cfg["response_prefix"].(string),
                                        result))
          }
        }

        /* inspect G_actions, clear out retired (or faulted) entries */

        now := time.Now().UnixNano()
        G_mt_actions.Lock()

        for idx:=0 ; idx < len(G_actions) ; idx++ {
          validity := G_actions[idx].Payload["validity"].(float64)
          cutoff := G_actions[idx].PostTime + int64(validity * 1000000000.0)
          if (now > cutoff) {
            if (G_actions[idx].State == STATE_PUBLISHED) {
              if (G_debug > 0) {
                f_log(LOG_DEBUG,
                      fmt.Sprintf("f_pubThread() retired %s@%d state:%d {%v}",
                                  G_actions[idx].Username,
                                  G_actions[idx].PostTime,
                                  G_actions[idx].State,
                                  G_actions[idx].Payload))
              }
              G_metrics.mg_action_retired++
            }
            if (G_actions[idx].State == STATE_PENDING) {
              f_log(LOG_WARN, fmt.Sprintf("no majority vote for %s@%d {%v}",
                                          G_actions[idx].Username,
                                          G_actions[idx].PostTime,
                                          G_actions[idx].Payload))
              G_metrics.mg_action_no_majority++
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
    fmt.Printf("Environment:\n")
    fmt.Printf("  DEBUG=<0|1>\n")
    os.Exit(1)
  }

  G_cfg = f_getConfig (os.Args[1])
  if (G_cfg == nil) {
    f_log(LOG_FATAL, "No working configuration.")
    os.Exit(1)
  }

  /* if logging is configured, get ready log file now */
  fmt.Printf("[%s]\n", reflect.TypeOf(G_cfg["logging"]).String())
  if ((G_cfg["logging"] != nil) &&
      (reflect.TypeOf(G_cfg["logging"]).String() ==
        "map[interface {}]interface {}")) {

    logging := G_cfg["logging"].(map[interface{}]interface{})
    if (logging["file"] == nil) {
      f_log(LOG_FATAL, "No 'file:' defined in 'logging:'")
      os.Exit(1)
    }
    if (logging["maxsize"] == nil) {
      f_log(LOG_FATAL, "No 'maxsize:' defined in 'logging:'")
      os.Exit(1)
    }
    f, err := os.OpenFile (logging["file"].(string),
                           os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if (err != nil) {
      f_log(LOG_FATAL, fmt.Sprintf("Cannot append to %s - %s",
                                   logging["file"].(string), err))
      os.Exit(1)
    }
    if (G_debug > 0) {
      f_log(LOG_DEBUG, fmt.Sprintf("main() logging to '%s'",
                                   logging["file"].(string)))
    }
    sbuf, err := f.Stat()
    if (err != nil) {
      f_log(LOG_FATAL, fmt.Sprintf("Cannot stat() %s - %s",
                                   logging["file"].(string), err))
      os.Exit(1)
    }
    G_logFile = f
    G_logSize = sbuf.Size()
    G_logMax = int64(logging["maxsize"].(int))
  }

  /* run the background thread that performs publish logic */

  go f_pubThread()

  /* now setup our webserver */

  web := G_cfg["web"].(map[interface{}]interface{})
  listen_port := ":" + strconv.Itoa (web["listen"].(int))
  http.HandleFunc ("/health", f_handleWeb)
  http.HandleFunc ("/metrics", f_handleWeb)
  http.HandleFunc ("/v1", f_handleWeb)

  f_log(LOG_NOTICE, fmt.Sprintf ("Starting webserver on %d.", web["listen"]))
  err := http.ListenAndServe (listen_port, nil)
  if (err != nil) {
    f_log(LOG_FATAL, fmt.Sprintf("Cannot start webserver - %s", err))
    os.Exit (1)
  }
}

