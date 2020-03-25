#!/bin/bash
#
# Usage:
#  ./run.sh [optional JAR filename]
#
# If no jar filename is given, "./acp.jar" will be used.
#
# run.sh - run a working set of Adaptive City Platform modules in 'production' mode
#
# Find the directory this script is being run from, because that will contain the JAR files
# typically "/home/acp_prod/acp_prod/"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# set jar filename to arg given on command line OR default to "acp.jar"
ACP_JAR=${1:-acp.jar}

cd $SCRIPT_DIR

# #############################################################################################
# ################   CONSOLE                      #############################################
# #############################################################################################

nohup java -cp "$ACP_JAR:secrets:configs" io.vertx.core.Launcher run "console.A" -cluster >/dev/null 2>>/var/log/acp_prod/console.A.err & disown

# #############################################################################################
# ################   TTN MQTT FEED HANDLER        #############################################
# #############################################################################################

nohup java -cp "$ACP_JAR:secrets:configs" -Xmx100m -Xms10m -Xmn2m -Xss10m io.vertx.core.Launcher run "service:feedmqtt.ttn" -cluster >/dev/null 2>>/var/log/acp_prod/feedmqtt.ttn.err & disown

nohup java -cp "$ACP_JAR:secrets:configs" -Xmx100m -Xms10m -Xmn2m -Xss10m io.vertx.core.Launcher run "service:msgfiler.ttn" -cluster >/dev/null 2>>/var/log/acp_prod/msgfiler.ttn.err & disown

# #############################################################################################
# ################  LOCAL MQTT FEED HANDLER       #############################################
# #############################################################################################

nohup java -cp "$ACP_JAR:secrets:configs" -Xmx100m -Xms10m -Xmn2m -Xss10m io.vertx.core.Launcher run "service:feedmqtt.local" -cluster >/dev/null 2>>/var/log/acp_prod/feedmqtt.local.err & disown

nohup java -cp "$ACP_JAR:secrets:configs" -Xmx100m -Xms10m -Xmn2m -Xss10m io.vertx.core.Launcher run "service:msgfiler.local" -cluster >/dev/null 2>>/var/log/acp_prod/msgfiler.local.err & disown

# #############################################################################################
# ################   RTMONITOR                    #############################################
# #############################################################################################

# RTMONITOR.A
nohup java -cp "$ACP_JAR:secrets:configs" -Xmx100m -Xms10m -Xmn2m -Xss10m io.vertx.core.Launcher run "service:rtmonitor.A" -cluster >/dev/null 2>>/var/log/acp_prod/rtmonitor.A.err & disown

