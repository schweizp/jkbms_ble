#!/usr/bin/python3
"""
Script to scan for BLE devices and show the relevant information

--> has to be run as root!

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

from collections import OrderedDict


# scan delegate class for BLE
class ScanDelegate(DefaultDelegate):
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
formatter = logging.Formatter('%(asctime)s - %(name)s - %(threadName)s - '
                                '%(levelname)s - %(module)s:%(lineno)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(fh)
log.addHandler(ch)

# parse arguments
parser = argparse.ArgumentParser(description = 'Victron modbus control test')
parser.add_argument('--version', action='version', version='%(prog)s v')
parser.add_argument('--debug', action="store_true", help='enable DEBUG logging')
# parser.add_argument('--info', action="store_true", help='enable INFO logging')
# parser.add_argument('--power', default=100, type=int, help='set the output of all MIs to xx percent')
# parser.add_argument('--max', action="store_true", help='set the output of all MIs to 100 percent')
# parser.add_argument('--min', action="store_true", help='set the output of all MIs to 2 percent')
# parser.add_argument('--on', action="store_true", help='switch all MIs ON')
# parser.add_argument('--off', action="store_true", help='switch all MIs OFF')
# parser.add_argument('--mqtt', action="store_true", help= 'enable mqtt data output')
requiredArguments = parser.add_argument_group('required arguments')
args = parser.parse_args()

# switch to info level
log.setLevel(logging.INFO)
ch.setLevel(logging.INFO)
fh.setLevel(logging.INFO)

if args.debug: # switch to debug level
    log.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)
    fh.setLevel(logging.DEBUG)


# ---
# startup sequence to reset everything to "normal" if script was interrupted in an undefined state
# and to check for correct environment (check if root/sudo)
# ---
def startupSequence():
    log.debug("Startup sequence")
    if os.getuid() != 0:
        log.error("### In order to scan for BLE devices this script has to be run as root/sudo ###")
        exit()
        

# ---
# main part of the script
# ---
if __name__ == "__main__":

    interval = 60.0
    lastrun = time.time() - 55
    
    while True:
        try:            
            startupSequence()   # make shure after 1st start everything is in order
            
            scanner = Scanner().withDelegate(ScanDelegate())
            
            while True:
                actualrun = time.time()
                if actualrun - lastrun > interval:
                    
                    devices = scanner.scan(10.0)
                    for dev in devices:
                        log.info("## Device %s (%s), RSSI=%d dB" % (dev.addr, dev.addrType, dev.rssi))
                        for (adtype, desc, value) in dev.getScanData():
                            log.info("    %s = %s" % (desc, value))
                else:
                    time.sleep(1)

        except:
            log.error('exeption raised waiting for 2 minutes before retrying')
            log.exception(sys.exc_info())
            # raise
            time.sleep(120)
            
        finally:
            scanner.stop()
            log.debug("finished")
            