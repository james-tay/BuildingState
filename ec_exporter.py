#!/usr/bin/env python3
#
# DESCRIPTION
#
#  In order to ensure reliable operation, it is important to monitor the
#  responsiveness of Edge Controllers (ECs). In particular, an EC may go
#  unresponsive due to software bugs, intermittent networking issues, etc.
#  Our role is to periodically contact ECs via REST and MQTT, measure their
#  command latency or warn of a timeout. To do this, we are supplied
#  configuration as a YAML file. 
#
# PREREQUISITES
#
#  % apt install python3-yaml
#
# OPERATION
#
#  This program's main loop performs HTTP requests and MQTT requests to each
#  EC in its list, recording each command's latency. To test MQTT, each loop
#  begins with spawning,
#
#    mqtt_sub <server>:<port> "<sub_topic_prefix>/#"
#
#  With the above running in the background, for each EC, this program runs,
#
#    mqtt_pub <server>:<port> "<pub_topic_prefix>/<hostname>" "<token>|<cmd>"
#
#  Where <token> is a random char sequence used to identifying the response
#  from our <cmd>.
#
#  All metrics gathered by this program are stored in a "G_metrics" dict,
#  where the key is the EC's hostname, and the value is another dict, which
#  holds the actual metrics that will be exposed to prometheus for scraping.
#
# USAGE
#
#  % ec_exporter <config.yaml>
#
#  Although it can run standalone, the expectation is that this program runs
#  as a side car in the pod which runs the Majority Gate (MG). Once running,
#  this program responds to the "/metrics" URI.

import os
import sys
import time
import yaml
import select
import random
import threading
import subprocess
import http.server
import urllib.request

import pprint

# Global variables

G_metrics = {}
G_cfg = {}

# -----------------------------------------------------------------------------

# This function is invoked whenever we receive an HTTP request.

class c_web(http.server.BaseHTTPRequestHandler):
  def do_GET(self):

    if (self.path == "/"):
      self.send_response(200)
      self.send_header("Content-type", "text/plain")
      self.end_headers()
      self.wfile.write(bytes("OK\n", "utf-8"))
      return

    if (self.path == "/metrics"):
      self.send_response(200)
      self.send_header("Content-type", "text/plain")
      self.end_headers()
      self.wfile.write(bytes("Hmm ...\n", "utf-8"))

      if (G_cfg["config"]["debug"]):
        pprint.pprint (G_metrics)

      report = ""
      for ec in G_metrics.keys():
        m = G_metrics[ec]
        for k in m.keys():
          if k != "last_update":
            report = report + "%s{hostname=\"%s\"} %f\n" % (k, ec, m[k])

      self.wfile.write(bytes(report, "utf-8"))
      return

    # for any other request, return 404

    self.send_response(404)
    self.end_headers()

# This function forms the thread which will be the webserver

def f_webserver():
  server = http.server.HTTPServer(("0.0.0.0", G_cfg["config"]["listen"]), c_web)
  print("INFO: Started webserver on port %d." % G_cfg["config"]["listen"])
  try:
    server.serve_forever()
  except:
    print("INFO: webserver terminating.")
    pass
  server.server_close()

# -----------------------------------------------------------------------------

# Generate some random string which will be used as an ID in our command to
# an EC. This is to identify the specific response to our command.

def f_gen_id():
  charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  id = ""
  for i in range(0, 12):
    r = random.randint(0, len(charset)-1)
    id = id + charset[r]
  return(id)

# This is a convenience function which manipulates the "G_metrics" hash. The
# "metric" with a "value" is added for "hostname". This function creates the
# necessary hash if required and also updates the "last_update" to reflect
# data freshness.

def f_add_metric(hostname, metric, value):

  global G_metrics
  if hostname not in G_metrics:
    x = {}
    x["ec_mqtt_timed_out"] = 0
    G_metrics[hostname] = x
  G_metrics[hostname][metric] = value
  G_metrics[hostname]["last_update"] = time.time()

# This is a convenience function which increments a (counter) "metric" for
# "hostname".

def f_inc_metric(hostname, metric):
  G_metrics[hostname][metric] = G_metrics[hostname][metric] + 1

# -----------------------------------------------------------------------------

# Parse commandline and load configuration

if (len(sys.argv) != 2):
  print ("Usage: %s <config.yaml>" % sys.argv[0])
  sys.exit(1)

try:
  fd = open (sys.argv[1], "r")
except:
  e = sys.exc_info()
  print("FATAL! Cannot read '%s' - %s" % (sys.argv[1], e[1]))
  sys.exit(1)

contents = fd.read()
fd.close()

try:
  G_cfg = yaml.load(contents)
except:
  e = sys.exc_info()
  print("FATAL! Cannot parse '%s' - %s" % (sys.argv[1], e[1]))
  sys.exit(1)

# set our MQTT server credentials as environment variables, which will be
# inherited by child processes.

os.environ["MQTT_USER"] = G_cfg["mqtt"]["user"]
os.environ["MQTT_PW"] = G_cfg["mqtt"]["pw"]

# prepare the arg list when we spawn the MQTT Subscriber and Publisher.

mqtt_pub_args = []
for s in G_cfg["mqtt"]["pub_tool"].split(" "):
  if (len(s) > 0):
    mqtt_pub_args.append(s)
mqtt_pub_args.append("%s:%d" % (G_cfg["mqtt"]["server"], G_cfg["mqtt"]["port"]))

mqtt_sub_args = []
for s in G_cfg["mqtt"]["sub_tool"].split(" "):
  if (len(s) > 0):
    mqtt_sub_args.append(s)
mqtt_sub_args.append("%s:%d" % (G_cfg["mqtt"]["server"], G_cfg["mqtt"]["port"]))
mqtt_sub_args.append("%s/#" % G_cfg["mqtt"]["response_prefix"])

# fire off a background thread to be our webserver

web_tid = threading.Thread (target=f_webserver, args=())
web_tid.start()

# this is our main loop ... poll all ECs and then wait around if we have time

cycle_start = time.time ()
while (1):

  # spawn a new MQTT Subscriber.

  try:
    sub_proc = subprocess.Popen(mqtt_sub_args, stdout=subprocess.PIPE)
  except:
    e = sys.exc_info()
    print("FATAL! Cannot run %s - %s" % (mqtt_sub_args[0], e[1]))
    sys.exit(1)
  if (G_cfg["config"]["debug"]):
    print("DEBUG: spawned %s pid:%d" % (mqtt_sub_args[0], sub_proc.pid))

  for idx in range(0, len(G_cfg["ec_list"])):
    ec = G_cfg["ec_list"][idx]
    if (G_cfg["config"]["debug"]):
      print ("DEBUG: Testing '%s' at '%s'." % (ec["alias"], ec["hostname"]))

    # do HTTP test

    tv_start = time.time()
    resp = None
    url = "http://%s/v1?cmd=%s" % (ec["hostname"], G_cfg["config"]["test_cmd"])
    try:
      resp = urllib.request.urlopen (url)
    except:
      e = sys.exc_info()
      print("WARNING: '%s' failed - %s" % (url, e[1]))
    tv_end = time.time()

    if (resp is not None):
      if (resp.status != 200):
        printf("WARNING: '%s' returned %d." % (url, resp.status))

      latency = tv_end - tv_start
      f_add_metric (ec["hostname"], "ec_rest_latency", latency)
      if (G_cfg["config"]["debug"]):
        print("DEBUG: HTTP - %.3fs" % latency)
      resp.close()

    # do MQTT test - fire off Publish in the background

    id = f_gen_id()
    ec_cmd = "%s|%s" % (id, G_cfg["config"]["test_cmd"])
    cmd_topic = "%s/%s" % (G_cfg["mqtt"]["command_prefix"], ec["hostname"])
    cmd_args = mqtt_pub_args + [ cmd_topic, ec_cmd ]
    pub_proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE)
    tv_start = time.time()
    cutoff = tv_start + G_cfg["config"]["timeout"]

    # wait here for a response

    while(1):
      time_left = cutoff - time.time()
      if (time_left < 0):
        f_inc_metric(ec["hostname"], "ec_mqtt_timed_out")
        print("WARNING: MQTT timed out")
        break
      rfds, wfds, efds = select.select ([ sub_proc.stdout ], [], [], time_left)
      if sub_proc.stdout in rfds:
        s = os.read(sub_proc.stdout.fileno(), 256)
        if (len(s) < 0):
          sub_proc.wait()
          print("FATAL! %s pid:%d exited %d." % \
                (mqtt_sub_args[0], sub_proc.pid), sub_proc.returncode)
          break
        else:
          if (str(s).find(id) > 0):
            tv_end = time.time()
            latency = tv_end - tv_start
            f_add_metric (ec["hostname"], "ec_mqtt_latency", latency)
            if (G_cfg["config"]["debug"]):
              print("DEBUG: MQTT - %.3fs" % latency)
            break

    # reap our Publish process

    pub_proc.wait()
    if (pub_proc.returncode != 0):
      print("WARNING: %s exited %d." % (mqtt_pub_args[0], pub_proc.returncode))

  # terminate MQTT Subscriber

  sub_proc.stdout.close()
  sub_proc.terminate()
  sub_proc.wait()
  if (G_cfg["config"]["debug"]):
    print("DEBUG: %s pid:%d exited." % (mqtt_sub_args[0], sub_proc.pid))

  # sleep until it's time to do work again

  cycle_end = time.time()
  duration = cycle_end - cycle_start
  cycle_start = cycle_start + G_cfg["config"]["interval"]
  nap_time = G_cfg["config"]["interval"] - duration
  print("INFO: cycle:%.3fs nap:%.3fs" % (duration, nap_time))
  if (nap_time > 0):
    time.sleep (nap_time)

