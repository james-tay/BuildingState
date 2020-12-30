/*
   Description:

    This program subscribes to a topic (wildcards accepted). Whenever it
    receives an event, it exec the (optionally) supplied command. The event's
    message is written to the child process's stdin and the topic is set in
    the MQTT_TOPIC environment variable.

   Build:

     % go get -v github.com/eclipse/paho.mqtt.golang
     % go build -o mqtt_sub.`uname -m` mqtt_sub.go

   References:

     https://github.com/eclipse/paho.mqtt.golang
     https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang
*/

package main

import (
  "os"
  "os/exec"
  "fmt"
  "time"
  "strings"
  "strconv"
  "math/rand"
  "github.com/eclipse/paho.mqtt.golang"
)

var cfg_cmd string

func f_callback(client mqtt.Client, msg mqtt.Message) {
  if (len(cfg_cmd) > 0) {

    /* user provided us with "<cmd...>", set MQTT_TOPIC and exec a child */

    os.Setenv("MQTT_TOPIC", msg.Topic())
    tv_start := time.Now().UnixNano()
    full_cmd := strings.Fields(cfg_cmd)
    cmd, args := full_cmd[0], full_cmd[1:]
    child := exec.Command (cmd, args...)
    stdin, err := child.StdinPipe()
    if (err != nil) {
      fmt.Printf("WARNING: Cannot get stdin for '%s' - %s\n", cmd, err)
      return
    }
    err = child.Start()
    if (err != nil) {
      fmt.Printf("WARNING: Cannot exec '%s' - %s\n", cmd, err)
      return
    }
    stdin.Write(msg.Payload())
    stdin.Close()
    err = child.Wait()
    tv_end := time.Now().UnixNano()
    t := time.Now()
    if (err == nil) {
      fmt.Printf("NOTICE: %s '%s' exited successfully after %dms.\n",
                 t.Format(time.StampMilli), cmd,
                 (tv_end-tv_start) / 1000 / 1000)
    } else {
      fmt.Printf("WARNING: %s '%s' %s, after %dms.\n",
                 t.Format(time.StampMilli), cmd, err,
                 (tv_end-tv_start) / 1000 / 1000)
    }

  } else {
    t := time.Now ()
    fmt.Printf("%s [%s] %s\n",
               t.Format(time.StampMilli), msg.Topic(), msg.Payload())
  }
}

func main() {
  if (len(os.Args) < 3) {
    fmt.Printf ("Usage: %s <server:port> <topic> [<cmd...>]\n" +
                "Optional Env:\n" +
                "  MQTT_USER=user\n" +
                "  MQTT_PW=pw\n", os.Args[0]) ;
    os.Exit (1)
  }

  topic := os.Args[2]
  if (len(os.Args) == 4) {
    cfg_cmd = os.Args[3]
  } else {
    cfg_cmd = ""
  }

  seed := rand.NewSource(time.Now().UnixNano() * int64(os.Getpid()))
  rgenerator := rand.New(seed)
  clientId := strconv.Itoa (rgenerator.Int())

  opts := mqtt.NewClientOptions()
  opts.AddBroker("tcp://" + os.Args[1])
  opts.SetClientID(clientId)
  opts.SetCleanSession(false)   // server remembers topic(s) we subscribed

  if (len(os.Getenv("MQTT_USER")) > 0) {
    opts.SetUsername(os.Getenv("MQTT_USER"))
  }
  if (len(os.Getenv("MQTT_PW")) > 0) {
    opts.SetPassword(os.Getenv("MQTT_PW"))
  }

  client := mqtt.NewClient(opts)
  token := client.Connect()
  token.Wait()
  if (token.Error() != nil) {
    fmt.Printf ("FATAL! Connect() failed - %s\n", token.Error())
    os.Exit (1)
  }

  token = client.Subscribe(topic, 0, f_callback)
  token.Wait()
  if (token.Error() != nil) {
    fmt.Printf ("FATAL! Subscribe() failed - %s\n", token.Error())
    os.Exit (1)
  }

  fmt.Printf ("Connected as %s.\n", clientId)

  for true {
    time.Sleep (10 * time.Second)
  }
}
