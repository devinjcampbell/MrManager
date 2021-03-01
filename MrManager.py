import os
import sys
import time
import serial
import mysql.connector as mysql
from decimal import Decimal


arduino = serial.Serial(port='/dev/ttyACM0', baudrate=115200, timeout=1)
arduino.dtr = 0  # Reset Arduino.
arduino.dtr = 1
time.sleep(2)  # Wait for Arduino to finish booting.
arduino.reset_input_buffer()  # Delete any stale data.

db = mysql.connect(
    host = "192.168.1.133",
    user = "farmData",
    password = "A1b9C#d4E4",
    database = "farmData"
    )
curs = db.cursor()


def weedVeg(dataStr):

    data = []
    data.append(None)
    for value in dataStr:
        data.append(Decimal(value))

    data.append(100)
    data.append(100)
    data.append(100)
    data.append(100)
    
    return data


def node9(dataStr):

    data = []

    data.append(None)
    for x in range(3):
        if(dataStr[x] == '0.00'):
            data.append(None)
        else:
            data.append(float(dataStr[x].strip()))
    for value in dataStr[3:]:
        data.append(Decimal(value))

    if(data.pop() == 1):
        curs.execute(nodeSQL['0'].format(addr='9'))
        row = list(curs.fetchone())
        row[0] = None
        row[1] = 0
        curs.execute(nodeSQL['9B'], row)
        db.commit()

    return data


def crawler(node):

    try:
        curs.execute(nodeSQL['0'].format(addr = node))
        row = list(curs.fetchone())
    except:
        print("Node {addr} - Could not connect to SP DB".format(addr=node))
        return

    msg = []
    decAddr = Decimal(node)
    if(decAddr < 7):
        sunset = row.pop()
        sunrise = row.pop()
        time.time()
        myTime = time.localtime()[3]
        if(sunrise > sunset):
            if(sunset <= myTime < sunrise):
                msg.append('0')
            else:
                msg.append('1')
        else:
            if(sunrise <= myTime < sunset):
                msg.append('1')
            else:
                msg.append('0')
        msg.append(',')
    for x in row[1:]:
        msg.append(str(x))
        msg.append(',')
    msg.append(node)
    msg.append('\n')
    msgOut = ''.join(msg).encode()
    print(msgOut)

    try:
        arduino.write(msgOut)
        time.sleep(1)
    except:
        print("Node {addr} - Could not write to master".format(addr=node))
        return

    datastr = []
    data = []
    try:
        raw = arduino.readline().decode()
        raw = raw.replace('\r', '')
        raw = raw.replace('\n', '')
        raw = raw.replace('\x00', '')
        dataStr = raw.split(",")
        print(dataStr)
    except:
        print("Node {addr} - Unresponsive".format(addr=node))
        return
    if(dataStr[0] == ''):
        print("Node {addr} - Fail??".format(addr=node))
        return
    
    try:
        data = nodeCMD[node](dataStr)
    except:
        print("Node {addr} - Failed Data Conversion".format(addr=node))
        return
    
    try:
        curs.execute(nodeSQL[node], data) 
        db.commit()
    except:
        print("Node {addr} - Failed Write to Data DB".format(addr=node))
        return

    print("Node {addr} - Complete".format(addr=node))
    return


nodeSQL = {
    '0' : "SELECT * FROM node{addr}_SP ORDER BY timestamp DESC LIMIT 1",
    '5' : "INSERT INTO node5_Data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    '6' : "INSERT INTO node6_Data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    '9' : "INSERT INTO node9_Data VALUES (%s, %s, %s, %s, %s, %s)",
    '9B': "INSERT INTO node9_SP VALUES (%s, %s, %s, %s, %s)",
    '10': "INSERT INTO node10_Data VALUES (%s, %s, %s, %s)",
}

nodeCMD = {
    '5' : weedVeg,
    '6' : weedVeg,
    '9' : node9,
}

nodes = ['5', '6', '9']

time.sleep(2)
while True:
    for x in nodes:

        crawler(x)
        time.sleep(5)
