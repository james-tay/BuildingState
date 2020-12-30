/*
   Description:

    This program subscribes to a topic (wildcards accepted). Whenever it
    receives an event, it verifies that it's a legit prometheus metric and
    loads it into influxdb.

   Build:

     % go get -v github.com/eclipse/paho.mqtt.golang
     % go build -o eventLoader.`uname -m` eventLoader.go

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
  "net/http"
  "math/rand"
  "io/ioutil"
  "github.com/eclipse/paho.mqtt.golang"
)

var cfg_mqtt_server string
var cfg_sub_topic string
var cfg_influxdb string
var cfg_db_name string
var cfg_dry_run int

/*
   This function is called when an MQTT event occurs on the topic (with
   wildcard) we subscribe to in client.Subscribe(). Our job is to examine
   the "topic" and "payload", making sure it's suitably prometheus'like
   before loading it into influxdb.
*/

func f_callback(client mqtt.Client, msg mqtt.Message) {

  topic := string (msg.Topic ())
  payload := string (msg.Payload ())

  /* make sure "payload" is in the format "<metric>{<tags>} <value>" */

  tag_start := strings.Index (payload, "{")
  if (tag_start < 0) {
    fmt.Printf ("WARNING: invalid payload, missing '{' - %s \n", payload)
    return
  }
  metric := payload[:tag_start]
  tag_end := strings.LastIndex (payload, "}")
  if (tag_end < 0) {
    fmt.Printf ("WARNING: invalid payload, missing '}' - %s\n", payload)
    return
  }
  if (len(metric) < 1) {
    fmt.Printf ("WARNING: metric is missing - %s\n", payload)
    return
  }
  tags := payload[tag_start+1:tag_end]
  val_idx := strings.LastIndex (payload, " ")
  if (val_idx <= tag_end) {
    fmt.Printf ("WARNING: value is missing - %s\n", payload)
    return
  }
  value := payload[val_idx+1:]

  /* parse "tags" as a map, make sure "instance" is defined as <src>:<port> */

  tag_map := make(map[string]string)
  for _, one_tag := range strings.Split (tags, ",") {
    t := strings.Split (one_tag, "=")           // eg, "instance=\"foo:9100\""
    k := t[0]
    v := strings.Trim (t[1], "\"")
    tag_map[k] = v
  }
  if (len(tag_map["instance"]) < 1) {
    fmt.Printf ("WARNING: no 'instance' tag - %s\n", payload)
    return
  }
  t := strings.Split (tag_map["instance"], ":")
  if (len (t) != 2) {
    fmt.Printf ("WARNING: 'instance' tag bad format - %s\n", payload)
    return
  }
  src := t[0]
  port, _ := strconv.Atoi (t[1])
  if (len(src) < 1) {
    fmt.Printf ("WARNING: instance src host is invalid - %s\n", payload)
    return
  }
  if (port < 1) || (port > 65534) {
    fmt.Printf ("WARNING: instance port is invalid - %s\n", payload)
    return
  }

  /* make sure "job" is present too */

  if (len(tag_map["job"]) < 1) {
    fmt.Printf ("WARNING: no 'job' tag - %s\n", payload)
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
  content := bytes.NewBuffer ([]byte(payload))

  if (cfg_dry_run > 0) {
    fmt.Printf ("DRYRUN: %s -> POST %s\n  %s\n", topic, influxdb_url, payload)

  } else {
    resp, err := http.Post (influxdb_url, "x-www-form-urlencoded", content)
    defer resp.Body.Close ()

    if (err != nil) {
      fmt.Printf ("WARNING: POST %s failed - %s\n", influxdb_url, err)
      return
    }
    body, err := ioutil.ReadAll (resp.Body)
    if (err == nil) {
      fmt.Printf ("NOTICE: status:%d '%s'\n", resp.StatusCode, payload)
    } else {
      fmt.Printf ("WARNING: status:%d - %s\n", resp.StatusCode, string(body))
    }
  }
}

/* ------------------------------------------------------------------------- */

func main() {
  if (len(os.Args) != 5) {
    fmt.Printf ("Usage: %s <server:port> <topic> <influxdb:port> <db>\n" +
                "Optional Env:\n" +
                "  MQTT_USER=user\n" +
                "  MQTT_PW=pw\n" +
                "  DRY_RUN=[0|1]\n", os.Args[0])
    os.Exit (1)
  }
  cfg_mqtt_server = os.Args[1]
  cfg_sub_topic = os.Args[2]
  cfg_influxdb = os.Args[3]
  cfg_db_name = os.Args[4]
  cfg_dry_run = 0 ;

  seed := rand.NewSource(time.Now().UnixNano() * int64(os.Getpid()))
  rgenerator := rand.New(seed)
  clientId := strconv.Itoa (rgenerator.Int())

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
  if (len(os.Getenv("DRY_RUN")) > 0) {
    cfg_dry_run, _ = strconv.Atoi (os.Getenv("DRY_RUN"))
  }

  client := mqtt.NewClient(opts)
  token := client.Connect()
  token.Wait()
  if (token.Error() != nil) {
    fmt.Printf ("FATAL! Cannot connect to %s - %s\n",
                cfg_mqtt_server, token.Error())
    os.Exit (1)
  }

  token = client.Subscribe(cfg_sub_topic, 0, f_callback)
  token.Wait()
  if (token.Error() != nil) {
    fmt.Printf ("FATAL! Cannot subscribe to %s - %s\n",
                cfg_sub_topic, token.Error())
    os.Exit (1)
  }

  fmt.Printf ("Connected to %s as %s.\n", cfg_mqtt_server, clientId)

  for true {
    time.Sleep (10 * time.Second)
  }
}

