#!/usr/bin/env python
#
# Client test code
#
#
#
# 

from MNode import MNode
from ChNode import ChunkInfo
import random
import logging

class FileInfo(object):
    """ File information like size,id,number 

        >>> f = FileInfo("abc",2)
        >>> f.strid
        'abc'
        >>> f.size
        2
        >>> f.strid = "cdef"
        >>> f.strid
        'cdef'
        >>> f.size = 20
        >>> f.size
        20

    """
    

    def __init__(self,strid,size = 1):
        self._strid = strid
        self._size = size
        self._num = 0
        self._nlist = []

    @property
    def num(self):
        return self._num

    @num.setter
    def num(self,value):
        self._num = value
        
    @property
    def strid(self):
        return self._strid

    @strid.setter
    def strid(self,value):
        self._strid = value
        
    @property
    def size(self):
        return self._size
    
    @size.setter
    def size(self,value):
        self._size = value

    @property
    def nlist(self):
        return self._nlist

    @nlist.setter
    def nlist(self,value):
        self._nlist = value

class Client(object):
    """ also for testing to client send file operations to server"""
    LENGTH_DATA_ID = 20
    LENGTH_DEFAULT_COPY = 3

    def __init__(self,master,num,prefix):
        self.num = num
        self.prefix = prefix
        self.upload_list = {}
        self.upload_end_list = {}
        self.update_end_list = {}
        self.master = master

    def upload(self):

        b = len(self.prefix)
        strid = self.prefix
        import string
        for i in range(Client.LENGTH_DATA_ID - b):
            strid = strid + random.choice(string.ascii_letters + 
                                          string.digits)
            
        f = FileInfo(strid,random.uniform(0,10))
        self.upload_list[strid] = f
        _nlist = self.master.upload_begin(int(random.uniform(0,MNode.DEFAULT_PRIORITY)))
        
        for w in _nlist:
            obj = self.master.get_chunk(w)
            obj.upload(strid,f.size,self) # file size 2G random
            self.upload_list[strid] = f
        


    def upload_end(self,key,size,nodeid):
        
        f = self.upload_list[key]
        f.num += 1
        f.nlist.append(nodeid)

        if f.num == Client.LENGTH_DEFAULT_COPY:
            self.upload_list.pop(key)
            self.upload_end_list[key] = f
            # print f.nlist
            self.master.upload_end(key,f.nlist)
            # report to master it's over
            

    def download(self):
        _v = self.upload_end_list.values()
        
        if len(_v) < 2:
            logging.info("upload in progress.")
            return
        
        strid = _v[int(random.uniform(0,len(_v)-2))].strid
        _nlist = self.master.download_begin(strid)
        if _nlist is None:
            logging.info("fail to download key:%s",strid)
            return

        for w in _nlist:
            c = self.master.get_chunk(w)
            if c.download(strid,self):
                break
    
    def download_end(self,key,size,nodeid):
        self.master.download_end(key)
    
    def update(self):
        _v = self.upload_end_list.values()
        if len(_v) < 2:
            return

        f = _v[int(random.uniform(0,len(_v)-1))]
        _nlist = self.master.update_begin(f.strid)
        if _nlist is None:
            logging.info("fail to update key:%s",f.strid)
            return
        f.size = random.uniform(2,2)
        import copy
        f.nlist = copy.copy(_nlist)
        self.update_end_list[f.strid] = f
        for w in _nlist:
            c = self.master.get_chunk(w)
            if not c.update(f.strid,f.size,self):
                print "fail to update file:",f.strid
            
    
    def update_end(self,strid,size,nodeid):
        f = self.update_end_list[strid]
        f.num += 1
        if f.num == len(f.nlist):
            self.update_end_list.remove(f)
            self.upload_end_list[strid] = f
            osize = f.size
            f.size = size
            self.master.update_end(f.strid,osize,f.strid,nsize,[nodeid])
    
    def remove(self):
        _v = self.upload_end_list.values()
        if len(_v) is 0:
            return
        
        f = _v[int(random.uniform(0,len(_v) - 1))]
        strid = f.strid
        obj = self.master.remove_begin(strid)
        print 'remove key list:',obj,',type:',type(obj)
        if not isinstance(obj,list):
            logging.info("fail to remove key:%s",strid)
            print 'fail to remove key:',strid
            return

        #i = 0
        #while i < len(obj):
        for w in obj:
            #w = obj[i]
            print 'remove key:',strid,',nodeid:',w
            c = self.master.get_chunk(w)
            if not c.remove(strid):
                print "fail to remove file:",strid
            else:
                self.master.remove_end(strid,c.nodeid)
                # print "client remove key:",strid,',nodeid:',c.nodeid
            
        #self.master.remove_end(strid,f.size)
        self.upload_end_list.pop(strid)

def doctests():
    import doctest
    return doctest.DocTestSuite()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
