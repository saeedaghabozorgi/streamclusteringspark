import socket
import sys
from thread import *
import requests
import requests_oauthlib
import tweepy
from tweepy import OAuthHandler
import json
import oauth2 as oauth
from datetime import datetime
import random
import numpy  as np
HOST = ''   # Symbolic name meaning all available interfaces
PORT = 9998 # Arbitrary non-privileged port

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'

#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error , msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()

print 'Socket bind complete'
import time
s.listen(10)
print 'Socket now listening'

#Function for handling connections. This will be used to create threads
def clientthread(conn):
    count = 0
    while True:
        try:
            if count > 10000000:
                break
            b=random.uniform(0.0, 11.0)
            x= np.random.normal(0) *5
            y= np.random.normal(0) *5
            if b<2:
                post= [20+x,20+y]
            elif b <5:
                post= [20+x,40+y]
            elif b <8:
                post= [40+x,20+y]
            else:
                post =[40+x,40+y]

            count+= 1
            conn.send(str(post)+'\n')
            print (str(post))
        except:
            e = sys.exc_info()[0]
            print( "Error: %s" % e )
        time.sleep(0.2)
    conn.close()


while 1:
    conn, addr = s.accept()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
    start_new_thread(clientthread ,(conn,))

s.close()

