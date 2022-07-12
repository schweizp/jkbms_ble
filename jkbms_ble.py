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
        self.jkbms = jkbms
        log.debug('Delegate {}'.format(str(jkbms)))
        self.notificationData = bytearray()

    def handleDiscovery(self, dev, isNewDev, isNewData):
        if isNewDev:
            log.info("Discovered device " +  dev.addr)
        elif isNewData:
            log.info("Received new data from " +  dev.addr)

    def recordIsComplete(self):
        '''
        '''
        log.debug('Notification Data {}'.format(self.notificationData))
        # check for 'ack' record
        if self.notificationData.startswith(bytes.fromhex('aa5590eb')):
            log.debug('notificationData has ACK')
            self.notificationData = bytearray()
            return False  # strictly record is complete, but we dont process this
        # check record starts with 'SOR'
        SOR = bytes.fromhex('55aaeb90')
        if not self.notificationData.startswith(SOR):
            log.debug('No SOR found in notificationData')
            self.notificationData = bytearray()
            return False
        # check that length one of the valid lengths (300, 320)
        if len(self.notificationData) == 300 or len(self.notificationData) == 320:
            # check the crc/checksum is correct for the record data
            crc = ord(self.notificationData[-1:])
            calcCrc = crc8(self.notificationData[:-1])
            # log.debug (crc, calcCrc)
            if crc == calcCrc:
                return True
        return False

    def processInfoRecord(self, record):
        #log.debug('Processing info record')
        del record[0:5]
        counter = record.pop(0)
        #log.debug('Record number: {}'.format(counter))
        vendorID = bytearray()
        hardwareVersion = bytearray()
        softwareVersion = bytearray()
        uptime = 0
        powerUpTimes = 0
        deviceName = bytearray()
        passCode = bytearray()
        # start at byte 7, go till 0x00 for device model
        while len(record) > 0:
            _int = record.pop(0)
            # log.debug (_int)
            if _int == 0x00:
                break
            else:
                vendorID += bytes(_int.to_bytes(1, byteorder='big'))
        # consume remaining null bytes
        _int = record.pop(0)
        while _int == 0x00:
            _int = record.pop(0)
        # process hardware version
        hardwareVersion += bytes(_int.to_bytes(1, byteorder='big'))
        while len(record) > 0:
            _int = record.pop(0)
            # log.debug (_int)
            if _int == 0x00:
                break
            else:
                hardwareVersion += bytes(_int.to_bytes(1, byteorder='big'))
        # consume remaining null bytes
        _int = record.pop(0)
        while _int == 0x00:
            _int = record.pop(0)
        # process software version
        softwareVersion += bytes(_int.to_bytes(1, byteorder='big'))
        while len(record) > 0:
            _int = record.pop(0)
            # log.debug (_int)
            if _int == 0x00:
                break
            else:
                softwareVersion += bytes(_int.to_bytes(1, byteorder='big'))
        # consume remaining null bytes
        _int = record.pop(0)
        while _int == 0x00:
            _int = record.pop(0)
        # process uptime version
        upTimePos = 0
        uptime = _int * 256**upTimePos
        while len(record) > 0:
            _int = record.pop(0)
            upTimePos += 1
            # log.debug (_int)
            if _int == 0x00:
                break
            else:
                uptime += _int * 256**upTimePos
        # consume remaining null bytes
        _int = record.pop(0)
        while _int == 0x00:
            _int = record.pop(0)
        # power up times
        powerUpTimes = _int
        # consume remaining null bytes
        _int = record.pop(0)
        while _int == 0x00:
            _int = record.pop(0)
        # device name
        deviceName += bytes(_int.to_bytes(1, byteorder='big'))
        while len(record) > 0:
            _int = record.pop(0)
            # log.debug (_int)
            if _int == 0x00:
                break
            else:
                deviceName += bytes(_int.to_bytes(1, byteorder='big'))
        # consume remaining null bytes
        _int = record.pop(0)
        while _int == 0x00:
            _int = record.pop(0)
        # Passcode
        passCode += bytes(_int.to_bytes(1, byteorder='big'))
        while len(record) > 0:
            _int = record.pop(0)
            # log.debug (_int)
            if _int == 0x00:
                break
            else:
                passCode += bytes(_int.to_bytes(1, byteorder='big'))

        #log.debug('VendorID: {}'.format(vendorID.decode('utf-8')))
        #publish({'VendorID': vendorID.decode('utf-8')}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
        #log.debug('Device Name: {}'.format(deviceName.decode('utf-8')))
        #publish({'DeviceName': deviceName.decode('utf-8')}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
        #log.debug('Pass Code: {}'.format(passCode.decode('utf-8')))
        # #publish({'PassCode': passCode.decode('utf-8')}, format=self.jkbms.format, broker=self.jkbms.mqttBroker)
        #log.debug('Hardware Version: {}'.format(hardwareVersion.decode('utf-8')))
        #publish({'HardwareVersion': hardwareVersion.decode('utf-8')}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
        #log.debug('Software Version: {}'.format(softwareVersion.decode('utf-8')))
        #publish({'SoftwareVersion': softwareVersion.decode('utf-8')}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
        daysFloat = uptime / (60 * 60 * 24)
        days = math.trunc(daysFloat)
        hoursFloat = (daysFloat - days) * 24
        hours = math.trunc(hoursFloat)
        minutesFloat = (hoursFloat - hours) * 60
        minutes = math.trunc(minutesFloat)
        secondsFloat = (minutesFloat - minutes) * 60
        seconds = math.trunc(secondsFloat)
        #log.debug('Uptime: {}D{}H{}M{}S'.format(days, hours, minutes, seconds))
        #publish({'Uptime': '{}D{}H{}M{}S'.format(days, hours, minutes, seconds)}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
        #log.debug('Power Up Times: {}'.format(powerUpTimes))
        #publish({'Power Up Times: {}'.format(powerUpTimes)}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)

    def processExtendedRecord(self, record):
        #log.debug('Processing extended record')
        del record[0:5]
        counter = record.pop(0)
        #log.debug('Record number: {}'.format(counter))

    def processCellDataRecord(self, record):
        #log.debug('Processing cell data record')
        #log.debug('Record length {}'.format(len(record)))
        del record[0:5]
        counter = record.pop(0)
        #log.debug('Record number: {}'.format(counter))
        # Process cell voltages
        volts = []
        size = 4
        number = 24
        c_high=0
        c_low=10
        c_diff=0
        for i in range(0, number):
            volts.append(record[0:size])
            del record[0:size]
        #log.debug('Volts: {}'.format(volts))
        _totalvolt = 0
        for cell, volt in enumerate(volts):
            _volt = float(decodeHex(volt))
            out['B{:d}'.format(cell+1)]=round(_volt,4)
            #log.debug('Cell: {:02d}, Volts: {:.4f}'.format(cell + 1, _volt))
            #publish({'VoltageCell{:02d}'.format(cell + 1): _volt}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
            _totalvolt += _volt
            if c_high < _volt:
                c_high= _volt
            if c_low > _volt and _volt != 0:
                c_low=_volt
        #publish({'VoltageTotal': _totalvolt}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
        # Process cell wire resistances
        # log.debug (record)
        #log.debug('Processing wire resistances')
        out["Total"]= round(_totalvolt,4)
        out["Cell_High"]= round(c_high,4)
        out["Cell_Low"]= round(c_low,4)
        out["Cell_Diff"]= round(c_high-c_low,4)
        resistances = []
        size = 4
        number = 25
        for i in range(0, number):
            resistances.append(record[0:size])
            del record[0:size]
        for cell, resistance in enumerate(resistances):
            out['R{:d}'.format(cell+1)]=round(decodeHex(resistance),4)
            #log.debug('Cell: {:02d}, Resistance: {:.4f}'.format(cell, decodeHex(resistance)))
            #publish({'ResistanceCell{:02d}'.format(cell): float(decodeHex(resistance))}, format=self.jkbms.format, broker=self.jkbms.mqttBroker, tag=self.jkbms.tag)
        # log.debug (record)

    def processRecord(self, record):
        recordType = record[4]
        # counter = record[5]
        if recordType == INFO_RECORD:
            self.processInfoRecord(record)
        elif recordType == EXTENDED_RECORD:
            self.processExtendedRecord(record)
        elif recordType == CELL_DATA:
            self.processCellDataRecord(record)
        else:
            log.debug('Unknown record type')

    def handleNotification(self, handle, data):
        # handle is the handle of the characteristic / descriptor that posted the notification
        # data is the data in this notification - may take multiple notifications to get all of a message
        #log.debug('From handle: {:#04x} Got {} bytes of data'.format(handle, len(data)))
        self.notificationData += bytearray(data)
        if self.recordIsComplete():
            record = self.notificationData
            self.notificationData = bytearray()
            self.processRecord(record)






# --------------------------------------------------------------------------- #
# configure the client logging
# --------------------------------------------------------------------------- #
# create logger
log = logging.getLogger('jkbms_ble')
log.setLevel(logging.WARNING)
# create file handler which logs even debug messages
fh = logging.handlers.TimedRotatingFileHandler('jkbms_ble.log','D', 1, 5)
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
EXTENDED_RECORD = 1
CELL_DATA = 2
INFO_RECORD = 3

getInfo = b'\xaa\x55\x90\xeb\x97\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11'
getCellInfo = b'\xaa\x55\x90\xeb\x96\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10'

name = 'wtfsection'
model = 'JK-B2A24S'
mac = 'C8:47:8C:E2:81:41'
command = 'command'
tag = 'JKBMS_CellData_bot'
format = 'mqtt'

out=dict()
c_high=0.00
c_low=10.00
c_diff=0.00



class jkbms:
    """
    JK BMS class
    """

#    def __str__(self):
        #return 'JKBMS instance --- name: {}, model: {}, mac: {}, command: {}, tag: {}, format: {}, records: {}, maxConnectionAttempts: {}, mqttBroker: {}'.format(self.name, self.model, self.mac, self.command, self.tag, self.format, self.records, self.maxConnectionAttempts, self.mqttBroker)
#        return out

    def __init__(self, name, model, mac, command, tag, format, records=1, maxConnectionAttempts=30, mqttBroker=None):
        '''
        '''
        self.name = name
        self.model = model
        self.mac = mac
        self.command = command
        self.tag = tag
        self.format = format
        try:
            self.records = int(records)
        except Exception:
            self.records = 1
        self.maxConnectionAttempts = maxConnectionAttempts
        self.mqttBroker = mqttBroker
        self.device = btle.Peripheral(None)
        #log.debug('Config data - name: {}, model: {}, mac: {}, command: {}, tag: {}, format: {}'.format(self.name, self.model, self.mac, self.command, self.tag, self.format))
        #log.debug('Additional config - records: {}, maxConnectionAttempts: {}, mqttBroker: {}'.format(self.records, self.maxConnectionAttempts, self.mqttBroker))
        #log.debug('jkBMS Logging level: {}'.format(log.level))

    def connect(self):
        # Intialise BLE device
        self.device = btle.Peripheral(None)
        log.debug('Connected with device {}'.format(self.device))
        self.device.withDelegate(BLEDelegate(self))
        # Connect to BLE Device
        connected = False
        attempts = 0
        while not connected:
            attempts += 1
            log.debug('Attempt #{} to connect to {}'.format(attempts, self.name))
            if attempts > self.maxConnectionAttempts:
                log.info('Cannot connect to {} with mac {} - exceeded {} attempts'.format(self.name, self.mac, attempts - 1))
                return connected
            try:
                self.device.connect(self.mac)
                connected = True
            except Exception:
                continue
        return connected
    
    def getServices(self):            
            self.services = self.device.getServices()
            log.debug("services: %s" % (self.services))

    def getBLEData(self):
        # Get the device name
        serviceId = self.device.getServiceByUUID(btle.AssignedNumbers.genericAccess)
        deviceName = serviceId.getCharacteristics(btle.AssignedNumbers.deviceName)[0]
        log.debug('Connected to {}'.format(deviceName.read()))

        # Connect to the notify service
        serviceNotifyUuid = 'ffe0'
        serviceNotify = self.device.getServiceByUUID(serviceNotifyUuid)

        # Get the handles that we need to talk to
        # Read
        characteristicReadUuid = 'ffe3'
        characteristicRead = serviceNotify.getCharacteristics(characteristicReadUuid)[0]
        handleRead = characteristicRead.getHandle()
        log.debug('Read characteristic: {}, handle {:x}'.format(characteristicRead, handleRead))

        # ## TODO sort below
        # Need to dynamically find this handle....
        log.debug('Enable 0x0b handle', self.device.writeCharacteristic(0x0b, b'\x01\x00'))
        self.device.writeCharacteristic(0x0b, b'\x01\x00')
        log.debug('Enable read handle', self.device.writeCharacteristic(handleRead, b'\x01\x00'))
        self.device.writeCharacteristic(handleRead, b'\x01\x00')
        log.debug('Write getInfo to read handle', self.device.writeCharacteristic(handleRead, getInfo))
        self.device.writeCharacteristic(handleRead, getInfo)
        secs = 0
        while True:
            if self.device.waitForNotifications(1.0):
                continue
            secs += 1
            if secs > 5:
                break

        log.debug('Write getCellInfo to read handle', self.device.writeCharacteristic(handleRead, getCellInfo))
        self.device.writeCharacteristic(handleRead, getCellInfo)
        loops = 0
        recordsToGrab = self.records
        log.debug('Grabbing {} records (after inital response)'.format(recordsToGrab))

        while True:
            loops += 1
            if loops > recordsToGrab * 15 + 16:
                log.debug('Got {} records'.format(recordsToGrab))
                break
            if self.device.waitForNotifications(1.0):
                continue

    def disconnect(self):
        log.debug('Disconnecting...')
        self.device.disconnect()



# ---
# The callback for when the mqtt client receives a CONNACK response from the server.
# ---
def on_connect(client, userdata, flags, rc):
    log.info("MQTT Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("$SYS/#")
    # client.subscribe("HM-Control/AutoControl")


# ---
# The callback for when a PUBLISH message is received from the mqtt server.
# ---
def on_message(client, userdata, msg):
    log.info("MQTT Publish message received")
    log.debug(msg.topic+" "+str(msg.payload))
    

# ---
# caclulate crc8 checksum
# ---
def crc8(byteData):
    '''
    Generate 8 bit CRC of supplied string
    '''
    CRC = 0
    # for j in range(0, len(str),2):
    for b in byteData:
        # char = int(str[j:j+2], 16)
        # log.debug(b)
        CRC = CRC + b
        CRC &= 0xff
    return CRC


# ---
#  decodeHex values
# ---
def decodeHex(hexToDecode):
    '''
    Code a 4 byte hexString to volts as per jkbms approach (blackbox determined)
    '''
    # hexString = bytes.fromhex(hexToDecode)
    hexString = hexToDecode
    log.debug('hexString: {}'.format(hexString))

    answer = 0.0

    # Make sure supplied String is long enough
    if len(hexString) != 4:
        log.info('Hex encoded value must be 4 bytes long. Was {} length'.format(len(hexString)))
        return 0

    # Process most significant byte (position 3)
    byte1 = hexString[3]
    if byte1 == 0x0:
        return 0
    byte1Low = byte1 - 0x40
    answer = (2**(byte1Low * 2)) * 2
    log.debug('After position 3: {}'.format(answer))
    step1 = answer / 8.0
    step2 = answer / 128.0
    step3 = answer / 2048.0
    step4 = answer / 32768.0
    step5 = answer / 524288.0
    step6 = answer / 8388608.0

    # position 2
    byte2 = hexString[2]
    byte2High = byte2 >> 4
    byte2Low = byte2 & 0xf
    if byte2High & 8:
        answer += ((byte2High - 8) * step1 * 2) + (8 * step1) + (byte2Low * step2)
    else:
        answer += (byte2High * step1) + (byte2Low * step2)
    log.debug('After position 2: {}'.format(answer))
    # position 1
    byte3 = hexString[1]
    byte3High = byte3 >> 4
    byte3Low = byte3 & 0xf
    answer += (byte3High * step3) + (byte3Low * step4)
    log.debug('After position 1: {}'.format(answer))
    # position 0
    byte4 = hexString[0]
    byte4High = byte4 >> 4
    byte4Low = byte4 & 0xf
    answer += (byte4High * step5) + (byte4Low * step6)
    #log.debug('After position 0: {}'.format(answer))

    #log.debug('hexString: {}'.format(hexString))
    #log.debug('hex(byte1): {}'.format(hex(byte1)))
    #log.debug('byte1Low: {}'.format(byte1Low))
    # log.debug ('byte2', byte2)
    #log.debug('hex(byte2): {}'.format(hex(byte2)))
    #log.debug('byte2High: {}'.format(byte2High))
    #log.debug('byte2Low: {}'.format(byte2Low))
    # log.debug ('byte3', byte3)
    #log.debug('hex(byte3): {}'.format(hex(byte3)))
    #log.debug('byte3High: {}'.format(byte3High))
    #log.debug('byte3Low: {}'.format(byte3Low))
    # log.debug ('byte4', byte4)
    #log.debug('hex(byte4): {}'.format(hex(byte4)))
    #log.debug('byte4High: {}'.format(byte4High))
    #log.debug('byte4Low: {}'.format(byte4Low))

    #log.debug('step1: {}'.format(step1))
    #log.debug('step2: {}'.format(step2))
    #log.debug('step3: {}'.format(step3))
    #log.debug('step4: {}'.format(step4))
    #log.debug('step5: {}'.format(step5))
    #log.debug('step6: {}'.format(step6))

    #log.debug('Hex {} decoded to {}'.format(hexString, answer))

    return answer


# ---
# startup sequence to reset everything to "normal" if script was interrupted in an undefined state
# ---
def startupSequence():
    # nothing here yet
    return(0)


# ---
# setup mqtt client
# ---
mqttClient = mqtt.Client()
mqttClient.on_connect = on_connect
mqttClient.on_message = on_message

mqttClient.enable_logger(logger=log)
mqttClient.connect(BROKER, PORT)
mqttClient.loop_start()


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
        
        # connect to device and get service information
        bms = jkbms(name=name, model=model, mac=mac, command=command, tag=tag, format=format, records=1, maxConnectionAttempts=30)
        
        #log.debug(str(jk))
        if bms.connect():
            bms.getServices()
            # bms.getBLEData()
            bms.disconnect()
        else:
            log.debug('Failed to connect to {} {}'.format(name, mac))
        # log.debug(json.dumps(out))
        
        """ while True:
            actualrun = time.time()
            if actualrun - lastrun > interval:
                time.sleep(1)
                
            else:
                time.sleep(1) """

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
