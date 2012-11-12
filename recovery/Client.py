#!/usr/bin/env python
#
# Client test code
#
#
#
# 

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

class Client(object):
    """ also for testing to client send file operations to server"""
    LENGTH_DATA_ID = 20
    LENGTH_DEFAULT_COPY = 3

    def __init__(self,master,num,prefix):
        self.num = num
        self.prefix = prefix
        self.upload_list = {}
        self.upload_end_list = {}
        self.master = master

    def upload(self):

        b = len(self.prefix)
        strid = self.prefix

        for i in range(Client.LENGTH_DATA_ID - b):
            strid += 'a' - 'A' + random.uniform(0,25)
            
        f = FileInfo(strid,random.gauss(2,2))
        self.upload_list[strid] = f
        _nlist = self.master.upload_begin(random.gauss(0,MNode.DEFAULT_PRIORITY))
        
        for w in _nlist:
            obj = self.master.get_chunk(w)
            obj.upload(strid,random.gauss(2,2)) # file size 2G random
            self.upload_list.append(strid)      
        


    def upload_end(self,key,size,nodeid):
        
        f = self.upload_list[key]
        f.num += 1
        
        if f.num == Client.LENGTH_DEFAULT_COPY:
            self.upload_list.remove(key)
            self.upload_end_list[key] = f
            # report to master it's over
            

    def download(self):
        _v = self.upload_end_list.values()
        strid = _v[random.uniform(0,len(_v)-1)]
        _nlist = self.master.download_begin(obj.strid)

        for w in _nlist:
            c = self.master.get_chunk(w)
            if c.download(strid,self):
                break
    
    def download_end(self,key,size,nodeid):
        pass
    
    def update(self):
        pass
    
    def update_end(self,nodeid,size):
        pass


def doctests():
    import doctest
    return doctest.DocTestSuite()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
