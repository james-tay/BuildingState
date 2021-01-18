/*
   Description:

    This program subscribes to a topic (wildcards accepted). Whenever it
    receives an event, it verifies that it's a legit prometheus metric and
    loads it into influxdb.

   Build:

     % go get -v github.com/eclipse/paho.mqtt.golang
     % go build -o eventLoader.`uname -m` eventLoader.go

   Run:

     % DRY_RUN=1 MQTT_USER=admin MQTT_PW=secret ./eventLoader.x86_64 \
         app-mqtt:1883 lawrence/ec/sensors/# moby:8086 promDemo 8091

   References:

     https://github.com/eclipse/paho.mqtt.golang
     https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang
*/

package main

import (
  "os"
  "fmt"
  "bytes"
  "time"
  "strings"
  "strconv"
  "reflect"
  "net/http"
  "math/rand"
  "io/ioutil"
  "github.com/eclipse/paho.mqtt.golang"
)

type S_metrics struct {
  evtloader_mqtt_connects int64         // calls to f_mqtt_connect()
  evtloader_mqtt_recvs int64            // MQTT messages received
  evtloader_mqtt_invalid int64          // invalid MQTT messages
  evtloader_mqtt_no_value int64         // MQTT messages with no value
  evtloader_load_success int64          // events loaded into influxdb
  evtloader_load_failed int64           // events not loaded for some reason
}

var cfg_mqtt_server string
var cfg_sub_topic string
var cfg_influxdb string
var cfg_db_name string
var cfg_web_port string
var cfg_dry_run int

var G_mqtt_client mqtt.Client
var G_metrics = S_metrics{}

/*
   This function connects to our MQTT server and subscribes to the topic
   of interest. We return true on success, otherwise false.
*/

func f_mqtt_connect() bool {

  seed := rand.NewSource(time.Now().UnixNano() * int64(os.Getpid()))
  rgenerator := rand.New(seed)
  clientId := strconv.Itoa(rgenerator.Int())

  opts := mqtt.NewClientOptions()
  opts.AddBroker("tcp://" + cfg_mqtt_server)
  opts.SetClientID(clientId)
  opts.SetCleanSession(false)   // server remembers topic(s) we subscribed

  if (len(os.Getenv("MQTT_USER")) > 0) {
    opts.SetUsername(os.Getenv("MQTT_USER"))
  }
  if (len(os.Getenv("MQTT_PW")) > 0) {
    opts.SetPassword(os.Getenv("MQTT_PW"))
  }

  G_mqtt_client = mqtt.NewClient(opts)
  result := G_mqtt_client.Connect()
  result.Wait()
  if (result.Error() != nil) {
    fmt.Printf("WARNING: Cannot connect to %s - %s\n",
               cfg_mqtt_server, result.Error())
    return(false)
  }

  result = G_mqtt_client.Subscribe(cfg_sub_topic, 0, f_callback)
  result.Wait()
  if (result.Error() != nil) {
    fmt.Printf("WARNING: Cannot subscribe to %s - %s\n",
               cfg_sub_topic, result.Error())
    return(false)
  }

  G_metrics.evtloader_mqtt_connects++
  fmt.Printf("NOTICE: Connected to %s as %s, subscribed to %s.\n",
             cfg_mqtt_server, clientId, cfg_sub_topic)
  return(true)
}

/*
   This function is called when an MQTT event occurs on the topic (with
   wildcard) we subscribe to in client.Subscribe(). Our job is to examine
   the "topic" and "payload", making sure it's suitably prometheus'like
   before loading it into influxdb.
*/

func f_callback(client mqtt.Client, msg mqtt.Message) {

  topic := string(msg.Topic ())
  payload := string(msg.Payload ())
  G_metrics.evtloader_mqtt_recvs++

  /* make sure "payload" is in the format "<metric>{<tags>} <value>" */

  tag_start := strings.Index(payload, "{")
  if (tag_start < 0) {
    G_metrics.evtloader_mqtt_invalid++
    fmt.Printf("WARNING: invalid payload, missing '{' - %s \n", payload)
    return
  }
  metric := payload[:tag_start]
  tag_end := strings.LastIndex(payload, "}")
  if (tag_end < 0) {
    G_metrics.evtloader_mqtt_invalid++
    fmt.Printf("WARNING: invalid payload, missing '}' - %s\n", payload)
    return
  }
  if (len(metric) < 1) {
    fmt.Printf("WARNING: metric is missing - %s\n", payload)
    return
  }
  tags := payload[tag_start+1:tag_end]
  val_idx := strings.LastIndex(payload, " ")
  if (val_idx <= tag_end) {
    G_metrics.evtloader_mqtt_no_value++
    fmt.Printf("WARNING: value is missing - %s\n", payload)
    return
  }
  value := payload[val_idx+1:]

  /* parse "tags" as a map, make sure "instance" is defined as <src>:<port> */

  tag_map := make(map[string]string)
  for _, one_tag := range strings.Split(tags, ",") {
    t := strings.Split(one_tag, "=")           // eg, "instance=\"foo:9100\""
    k := t[0]
    v := strings.Trim(t[1], "\"")
    tag_map[k] = v
  }
  if (len(tag_map["instance"]) < 1) {
    G_metrics.evtloader_mqtt_invalid++
    fmt.Printf("WARNING: no 'instance' tag - %s\n", payload)
    return
  }
  t := strings.Split(tag_map["instance"], ":")
  if (len(t) != 2) {
    G_metrics.evtloader_mqtt_invalid++
    fmt.Printf("WARNING: 'instance' tag bad format - %s\n", payload)
    return
  }
  src := t[0]
  port, _ := strconv.Atoi(t[1])
  if (len(src) < 1) {
    G_metrics.evtloader_mqtt_invalid++
    fmt.Printf("WARNING: instance src host is invalid - %s\n", payload)
    return
  }
  if (port < 1) || (port > 65534) {
    G_metrics.evtloader_mqtt_invalid++
    fmt.Printf("WARNING: instance port is invalid - %s\n", payload)
    return
  }

  /* make sure "job" is present too */

  if (len(tag_map["job"]) < 1) {
    G_metrics.evtloader_mqtt_invalid++
    fmt.Printf("WARNING: no 'job' tag - %s\n", payload)
    return
  }

  /*
     Recall that a typical metric scraped from prometheus in influxdb looks
     like this :

       > select * from node_disk_io_now order by time desc limit 1
       name: node_disk_io_now
       time                __name__         device instance  job     value
       ----                --------         ------ --------  ---     -----
       1607203470521000000 node_disk_io_now sr0    moby:9100 prod-vm 7

     Thus, we need to package our http POST data as :

       node_disk_io_now,device=sr0,instance=moby:9100,job=prod-vm value=7
  */

  payload = metric + ",__name__=" + metric
  for k, v := range tag_map {
    payload += "," + k + "=" + v
  }
  payload += " value=" + value

  influxdb_url := "http://" + cfg_influxdb + "/write?db=" + cfg_db_name
  content := bytes.NewBuffer([]byte(payload))

  if (cfg_dry_run > 0) {
    fmt.Printf("DRYRUN: %s -> POST %s\n  %s\n", topic, influxdb_url, payload)

  } else {
    resp, err := http.Post(influxdb_url, "x-www-form-urlencoded", content)
    defer resp.Body.Close()

    if (err != nil) {
      G_metrics.evtloader_load_failed++
      fmt.Printf("WARNING: POST %s failed - %s\n", influxdb_url, err)
      return
    }
    body, err := ioutil.ReadAll(resp.Body)
    if (err == nil) {
      G_metrics.evtloader_load_success++
      fmt.Printf("NOTICE: status:%d '%s'\n", resp.StatusCode, payload)
    } else {
      G_metrics.evtloader_load_failed++
      fmt.Printf("WARNING: status:%d - %s\n", resp.StatusCode, string(body))
    }
  }
}

/*
   This function is invoked magically when an HTTP request arrives because
   we're declared as a handler in main().
*/

func f_handleWeb (w http.ResponseWriter, r *http.Request) {

  switch (r.Method) {
    case "GET":
      if (r.URL.Path == "/health") {

        /* do a quick check on our MQTT connection, report its status */

        if (G_mqtt_client.IsConnectionOpen() == false) {
          if (f_mqtt_connect() == false) {
            w.WriteHeader(http.StatusInternalServerError)
            w.Write([]byte("Failed\n"))
            return
          }
        }
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("OK\n"))
      }
      if (r.URL.Path == "/metrics") {

        /* print out performance metrics using reflection */

        w.WriteHeader(http.StatusOK)
        values := reflect.ValueOf(&G_metrics).Elem()
        mtypes := values.Type()
        for i:=0 ; i < values.NumField() ; i++ {
          field := values.Field(i)
          s := fmt.Sprintf("%v %v\n", mtypes.Field(i).Name, field)
          w.Write([]byte(s))
        }
      }
      return
    default:
      w.WriteHeader(http.StatusNotImplemented)
      w.Write([]byte("Not implemented, sorry.\n"))
  }
}

/* ------------------------------------------------------------------------- */

func main() {
  if (len(os.Args) != 6) {
    fmt.Printf("Usage: %s <mqtt_server:port> <topic> <influxdb:port> <db> " +
               "<web_port>\n" +
               "Optional Env:\n" +
               "  MQTT_USER=user\n" +
               "  MQTT_PW=pw\n" +
               "  DRY_RUN=[0|1]\n", os.Args[0])
    os.Exit(1)
  }
  cfg_mqtt_server = os.Args[1]
  cfg_sub_topic = os.Args[2]
  cfg_influxdb = os.Args[3]
  cfg_db_name = os.Args[4]
  cfg_web_port = os.Args[5]
  cfg_dry_run = 0 ;

  if (len(os.Getenv("DRY_RUN")) > 0) {
    cfg_dry_run, _ = strconv.Atoi(os.Getenv("DRY_RUN"))
  }

  if (f_mqtt_connect() == false) {
    fmt.Printf("FATAL! program terminating.\n")
    os.Exit(1)
  }

  /* setup a webserver, this exposes our health and metrics */

  fmt.Printf("NOTICE: Starting webserver on %s.\n", cfg_web_port)
  http.HandleFunc("/health", f_handleWeb)
  http.HandleFunc("/metrics", f_handleWeb)
  err := http.ListenAndServe(":" + cfg_web_port, nil)
  if (err != nil) {
    fmt.Printf("FATAL! Cannot start webserver - %s\n", err)
    os.Exit(1)
  }
}

