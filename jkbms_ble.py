#!/usr/bin/python3
"""
Script to interface to a JK-BMS to get relevant information

Inputs: cell voltages, SOC, temps, power, ...
Outputs: values to a mqtt server
-------------------------------------------------------------------------
"""

import os
from socket import socket
from stat import FILE_ATTRIBUTE_DIRECTORY
import string
import sys
import time
import logging
import logging.handlers
import argparse
from xmlrpc.client import boolean
from bluepy import btle
from bluepy.btle import Scanner, DefaultDelegate


import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import MQTT_ERR_NO_CONN
from paho.mqtt.client import MQTT_ERR_QUEUE_SIZE

from collections import OrderedDict


# delegate class for BLE
class BLEDelegate(DefaultDelegate):
    def __init__(self):
        DefaultDelegate.__init__(self)

    def handleDiscovery(self, dev, isNewDev, isNewData):
        if isNewDev:
            log.info("Discovered device " +  dev.addr)
        elif isNewData:
            log.info("Received new data from " +  dev.addr)




# --------------------------------------------------------------------------- #
# configure the client logging
# --------------------------------------------------------------------------- #
# create logger
log = logging.getLogger('jkbms_ble')
log.setLevel(logging.WARNING)
# create file handler which logs even debug messages
fh = logging.handlers.TimedRotatingFileHandler('hm_mb_control.log','D', 1, 5)
fh.setLevel(logging.INFO)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(fh)
log.addHandler(ch)

# parse arguments
parser = argparse.ArgumentParser(description = 'Victron modbus control test')
parser.add_argument('--version', action='version', version='%(prog)s v')
parser.add_argument('--debug', action="store_true", help='enable DEBUG logging')
parser.add_argument('--info', action="store_true", help='enable INFO logging')
# parser.add_argument('--power', default=100, type=int, help='set the output of all MIs to xx percent')
# parser.add_argument('--max', action="store_true", help='set the output of all MIs to 100 percent')
# parser.add_argument('--min', action="store_true", help='set the output of all MIs to 2 percent')
# parser.add_argument('--on', action="store_true", help='switch all MIs ON')
# parser.add_argument('--off', action="store_true", help='switch all MIs OFF')
# parser.add_argument('--mqtt', action="store_true", help= 'enable mqtt data output')
requiredArguments = parser.add_argument_group('required arguments')
args = parser.parse_args()

if args.info: # switch to info level
    log.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)
    fh.setLevel(logging.INFO)

if args.debug: # switch to debug level
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(threadName)s - '
                                    '%(levelname)s - %(module)s:%(lineno)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    log.addHandler(fh)
    log.addHandler(ch)
    
    log.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)
    fh.setLevel(logging.DEBUG)

# setup mqtt infos
PORT = 1883
BROKER = "mosquitto.fritz.box"

# global variables
soc = 0				    # state of charge of the battery
gridTotal = 0		    # grid power
acOutTotal = 0		    # power on AC output
acPVTotal = 0		    # PV power
charge = 0			    # charge power (multi to battery)
veBusState = 0          # VE.Bus state 
                        # 0=Off;1=Low Power;2=Fault;3=Bulk;4=Absorption;5=Float;6=Storage;
                        # 7=Equalize;8=Passthru;9=Inverting;10=Power assist;
                        # 11=Power supply;252=Bulk protection
setPercentage = 100     # setpoint (in %) for PV inverter power (calculated by control algo)
isControlling = False   # control loop is running?
autoControl = True      # automatic HM control on/off (via mqtt remote control)
setpoint = 0            # calculated setpoint in W for control algo
setpointPrev = 0        # setpoint in W from last control algo run
setDeltas = [0, 0, 0, 0, 0, 0]
                        # delta PV-power setpoint to delivered PV-power for the last 6 runs
powerSetpoint = 100     # power setpoint for manual control (via mqtt remote control)

# The callback for when the mqtt client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    log.info("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("$SYS/#")
    # client.subscribe("HM-Control/AutoControl")


# The callback for when a PUBLISH message is received from the mqtt server.
def on_message(client, userdata, msg):
    log.info("Publish message received")
    log.debug(msg.topic+" "+str(msg.payload))
    

# setup mqtt client
mqttClient = mqtt.Client()
mqttClient.on_connect = on_connect
mqttClient.on_message = on_message

mqttClient.enable_logger(logger=log)
mqttClient.connect(BROKER, PORT)
mqttClient.loop_start()


# ---
# startup sequence to reset everything to "normal" if script was interrupted in an undefined state
# ---
def startupSequence():
    # nothing here yet
    return(0)

# ---
# main part of the script
# ---
if __name__ == "__main__":

    interval = 60.0
    lastrun = time.time() - 55

    try:            
        log.info("Startup; wait 10s to initialize communication")
        time.sleep(10)      # wait 10s to give mqtt connection time to initiates
        startupSequence()   # make shure after 1st start everything is in order
        
        'connect to device and get service information'
        bms = btle.Peripheral('C8:47:8C:E2:81:41')
        log.debug("pheripheral object: %s" % (bms))
        bms.withDelegate(BLEDelegate())
        
        services = bms.getServices()
        log.debug("services: %s" % (services))
            
        
           
        while True:
            actualrun = time.time()
            # log.debug('actualtime: ' + str(nowTime))
            # log.debug('nigttime: ' + str(nightTime))
            # log.debug('morningtime: ' + str(morningTime))
            if actualrun - lastrun > interval:
                time.sleep(1)
                
            else:
                time.sleep(1)

    except:
        log.error('exeption raised waiting for 2 minutes before retrying')
        log.exception(sys.exc_info())
        # raise
        bms.disconnect()
        time.sleep(120)
        mqttClient.reconnect()
        bms.connect()

    finally:
        log.info("finished")

        # close the clients
        mqttClient.loop_stop()
        mqttClient.disconnect()
        bms.disconnect()
