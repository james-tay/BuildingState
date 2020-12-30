/*
   Build:

    % go build -o mqtt_pub.`uname -m` -v mqtt_pub.go

   Description:

     This program publishes a message to a given MQTT topic, optionally
     authenticating using credentials from environment variables.
*/

package main

import (
  "os"
  "fmt"
  "time"
  "strconv"
  "math/rand"
  "github.com/eclipse/paho.mqtt.golang"
)

func main () {

  if (len(os.Args) != 4) {
    fmt.Printf ("Usage: %s <server:port> <topic> <message>\n" +
                "Optional Env:\n"+
                "  MQTT_USER=user\n" +
                "  MQTT_PW=pw\n", os.Args[0])
    os.Exit (1)
  }

  topic := os.Args[2]
  msg := os.Args[3]

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

  client := mqtt.NewClient(opts)
  token := client.Connect()
  token.Wait()
  if (token.Error() != nil) {
    fmt.Printf ("WARNING: Connect() failed - %s\n", token.Error())
    os.Exit(1)
  }

  token = client.Publish(topic, 0, false, msg)
  token.Wait()
  client.Disconnect(1)

  if (token.Error() != nil) {
    fmt.Printf ("WARNING: Publish() failed - %s\n", token.Error())
    os.Exit(1)
  }
}

