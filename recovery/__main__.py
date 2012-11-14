#!/usr/bin/env python

from MNode import MNode
from ChNode import ChunkInfo
from Client import Client
from time import time
from random import random

def upload(nlist,num):
    """ upload file num """
    for w in range(num):
        for c in iter(nlist):
            c.upload()
    
def download(nlist,num):
    """ upload file num """
    for w in range(num):
        for c in iter(nlist):
            c.download()

def update(nlist,num):
    """ upload file num """
    for w in range(num):
        for c in iter(nlist):
            c.update()

def remove(nlist,num):
    """ upload file num """
    for w in range(num):
        for c in iter(nlist):
            c.remove()
    print 'remove key num:',num
    
def down(*nlist):
    """ Chunk node is down"""
    c = random.choice(nlist)
    print 'nodeid:',c.nodeid,' is down'
    c.down()

PRIORITY_CHUNK = 2
PRIORITY_MASTER = 1
PRIORITY_CLIENT = 3

NUM_CLIENT = 10
NUM_CHUNK = 10
NUM_UPLOAD = 20
NUM_UPDATE = 10
NUM_REMOVE = 2
NUM_DOWNLOAD = 2

if __name__ == "__main__":
    
    master = MNode()
    clist = []
    cclist = []

    for w in range(NUM_CLIENT):
        client = Client(master,NUM_CLIENT,"abcd")
        clist.append(client)

    for w in range(NUM_CHUNK):
        strid = str(w)
        c = ChunkInfo(strid,master)
        master.add(c)
        cclist.append(c)
        
    for w in range(NUM_CHUNK):
        print master.get_chunk(str(w)).nodeid

    upload(clist,NUM_UPLOAD) # num_client * num_upload * 3
    import random
    #download(clist,int(random.uniform(0,100)))
    #update(clist,int(random.uniform(0,40)))
    #remove(clist,int(random.uniform(0,30)))

    import time,sched
    s = sched.scheduler(time.time,time.sleep)
    
    # start Timer for all ChunkServer
    while True:
        #print 'loop in here'
        for w in cclist:

            # print 'next timeout:',w.get_min()
            while True:

                if w.get_min() is 0:
                    break

                relative = int(w.get_min() - time.time())
                if relative > 60:
                    relative = 60

                print 'relative:%s,min:%s' % (relative,w.get_min())
                if relative <= 0:
                    print 'do_action in relative less than 0'
                    w.do_action()
                else:
                    s.enter(relative,PRIORITY_CHUNK,w.do_action,())
                    break
        #s.enter(int(random.uniform(0,40)),PRIORITY_CLIENT,upload,(clist,4))
        s.enter(90,PRIORITY_CLIENT,download,(clist,int(random.uniform(0,50))))
        s.enter(90,PRIORITY_CLIENT,update,(clist,int(random.uniform(0,3))))
        s.enter(90,PRIORITY_CLIENT,remove,(clist,int(random.uniform(0,10))))
        s.enter(60,PRIORITY_MASTER,master.check_migarate,())
        s.enter(120,PRIORITY_CHUNK,down,cclist)
        s.run()
        # register master to collect some info 
        
