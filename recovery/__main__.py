#!/usr/bin/env python

from MNode import MNode
from ChNode import ChunkInfo
from Client import Client
from time import time

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

PRIORITY_CHUNK = 2
PRIORITY_MASTER = 1

if __name__ == "__main__":
    
    master = MNode()
    clist = []
    cclist = []

    for w in range(10):
        client = Client(master,10,"abcd")
        clist.append(client)

    for w in range(10):
        strid = str(w)
        c = ChunkInfo(strid,master)
        master.add(c)
        cclist.append(c)
        
    for w in range(10):
        print master.get_chunk(str(w)).nodeid

    upload(clist,10)

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
                    print 'do_action'
                    w.do_action()
                else:
                    s.enter(relative,PRIORITY_CHUNK,w.do_action,())
                    break
        
        s.enter(60,PRIORITY_MASTER,master.check_migarate,())
        s.run()
        # register master to collect some info 
        
