/*
   Build:

     % go build -o gpio_mqtt_pub.`uname -m` -v gpio_mqtt_pub.go

   Description:

     This program monitors a GPIO pin for state changes. This is done by
     interacting with "gpio" utility. In event of a state change, it publishes
     a message to MQTT.
*/

package main

import (
  "os"
  "os/exec"
  "fmt"
  "time"
  "bufio"
  "strconv"
  "math/rand"
  "github.com/eclipse/paho.mqtt.golang"
)

const POLL_CYCLE_MS = 200

/* ========================================================================= */

func f_publish (opts *mqtt.ClientOptions, topic string, msg string) {
  client := mqtt.NewClient(opts)
  token := client.Connect()
  token.Wait()
  if (token.Error() != nil) {
    fmt.Printf ("WARNING: Connect() failed - %s\n", token.Error())
    return
  }

  token = client.Publish(topic, 0, false, msg)
  token.Wait()
  client.Disconnect(1)

  if (token.Error() != nil) {
    fmt.Printf ("WARNING: Publish() failed - %s\n", token.Error())
    return
  }
}

func f_getValue (gpio_num string) int {
  args := [] string { "read", gpio_num }
  cmd := exec.Command ("gpio", args...)
  stdout, err := cmd.StdoutPipe()
  if (err != nil) {
    fmt.Printf("FATAL! Cannot get stdout from 'gpio' - %s\n", err)
    os.Exit(1)
  }
  err = cmd.Start()
  if (err != nil) {
    fmt.Printf ("FATAL! Cannot exec 'gpio' - %s\n", err)
    os.Exit(1)
  }
  reader := bufio.NewReader(stdout)
  buf, _, _ := reader.ReadLine()
  v, _ := strconv.Atoi(string(buf))
  cmd.Wait()
  return (v)
}

/* ========================================================================= */

func main () {
  if (len(os.Args) != 4) {
    fmt.Printf ("Usage: %s <server:port> <topic> <gpio number>\n" +
                "Optional Env:\n" +
                "  MQTT_USER=user\n" +
                "  MQTT_PW=pw\n", os.Args[0]) ;
    os.Exit (1)
  }

  topic := os.Args[2]
  gpio_num := os.Args[3]

  /* use a random number as our MQTT client ID */

  seed := rand.NewSource(time.Now().UnixNano() * int64(os.Getpid()))
  rgenerator := rand.New(seed)
  clientId := strconv.Itoa(rgenerator.Int())

  opts := mqtt.NewClientOptions()
  opts.AddBroker("tcp://" + os.Args[1])
  opts.SetClientID(clientId)

  if (len(os.Getenv("MQTT_USER")) > 0) {
    opts.SetUsername(os.Getenv("MQTT_USER"))
  }
  if (len(os.Getenv("MQTT_PW")) > 0) {
    opts.SetPassword(os.Getenv("MQTT_PW"))
  }

  fmt.Printf ("Watching GPIO%s.\n", gpio_num) ;

  prev := -1 /* detect state changes, but only after the first loop */
  for true {
    v := f_getValue(gpio_num)
    if ((prev != -1) && (prev != v)) {
      msg := fmt.Sprintf("clientId:%s gpio_num:%s value:%d",
                         clientId, gpio_num, v)
      f_publish(opts, topic, msg)

      t := time.Now()
      fmt.Printf("%s gpio_num:%s %d->%d\n",
                 t.Format(time.StampMilli), gpio_num, prev, v)
    }
    time.Sleep (POLL_CYCLE_MS * time.Millisecond)
    prev = v
  }
}

