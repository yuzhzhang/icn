


import os
import socket
import time

ADDR = '0.0.0.0'
PORT = 6669
BUFFER_SIZE = 4096


conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect((ADDR, PORT))

ref = 0
with open('/home/keplercapital2/imbalance/iceConnector/imbalance.csv') as sample:
#with open('imbalance.csv') as sample:
    old_timestamp = ''
    for line in sample:

        words = line.strip().split(',')
        words = [w.strip() for w in words]
        msg = '[IMB,%s,%s,%s,%s,%s,%s,%s]' % (words[1], words[2], words[3], words[4], words[5], words[6][0], words[2])
        
        if words[1] == 'GDXJ':
            ref = words[2]
            print words[-1], msg

        if words[-1].split(' ')[-1][:7] >= '14:58:30':
            break

        conn.send(msg)

        #conn.send('[MKT,%s,%s]' % (words[1], words[2]))
        if words[-1] > old_timestamp:
            time.sleep(1.0)
            old_timestamp = words[-1]
            conn.send('[IMB,AGG,114.17,200000,10000,3000,S,114.17]')
            conn.send('[IMB,$,0,0,0,0,0,0]')


        
    





