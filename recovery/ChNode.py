#!/usr/bin/env python
#
#
# Chunk Node Server's responsibility:
# 1.save all data info and system info,like:
#   data guid[key],data status,resource info<disk,network,cpu current visit nuum,etc>
# 2.report info to Management node
# 3.response for client's request
# 4.ready for repair,like positive repair,negative repair
#
#

import MNode
from time import time
import random

class RequestInfo(object):
    """ client send a request to user,then Chunk Server should save the req
    
    
    >>> h = RequestInfo(RequestInfo.CHUNK_REQ_UPLOAD)
    >>> h['nodeid'] = 'abcdef'
    >>> h['nodeid']
    'abcdef'
    >>> h['size'] = 10*1024
    >>> h['op']
    1
    >>> h['size']
    10240

    """

    CHUNK_REQ_DEFAULT = 0
    CHUNK_REQ_UPLOAD = 1
    CHUNK_REQ_DOWNLOAD = 2
    CHUNK_REQ_UPDATE = 3
    CHUNK_REQ_REMOVE = 4
    
    def __init__(self,op):
        self.req_dict = {}
        self.req_dict['op'] = op
        self.req_dict['time'] = time()
    def __getitem__(self,key):
        from MNode import DataInfo
        name = DataInfo._normalize_name(key)
        return self.req_dict.get(name)
    def __setitem__(self,key,value):
        from MNode import DataInfo
        name = DataInfo._normalize_name(key)
        self.req_dict[name] = value
    def __len__(self):
        return len(self.req_dict)

class CDataInfo(object):
    """ chunk server DataInfo in-memory
    
    >>> c = CDataInfo()
    >>> c.nodeid = 'abcdef'
    >>> c.nodeid
    'abcdef'
    >>> c.size = 10*1024
    >>> c.size
    10240
    >>> c.status = CDataInfo.DATA_STATUS_NORMAL
    >>> c.status
    0
    >>> c.dirty
    True

    """
    DATA_STATUS_NORMAL = 0
    DATA_STATUS_READ = 1
    DATA_STATUS_WRITE = 2
    DATA_STATUS_RM = 3
    DATA_STATUS_MIGARATE = 4
    
    def __init__(self):
        self._size = 0
        self._status = CDataInfo.DATA_STATUS_NORMAL
        self._nodeid = None
        self._dirty = False
        pass

    @property
    def dirty(self):
        return self._dirty

    @dirty.setter
    def dirty(self,value):
        self._dirty = value

    @property
    def nodeid(self):
        return self._nodeid

    @nodeid.setter
    def nodeid(self,value):
        self._dirty = False
        self._nodeid = value

    @property
    def status(self):
        return self._status
    
    @status.setter
    def status(self,value):
        self._status = value

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self,value):
        self._dirty = True
        self._size = value

class ChunkInfo(object):
    """ Chunk server implementation"""

    COST_REQ_UPLOAD_CPU  = 0.15
    COST_REQ_DOWNLOAD_CPU = 0.15
    COST_REQ_UPDATE_CPU = 0.15
    COST_REQ_REMOVE_CPU = 0.1
    
    def __init__(self,nodeid,master):
        self.nodeid = nodeid
        self.status = NodeInfo.NODE_UP
        self.res_dict = {}
        self.data_dict = {}
        self.req_list = []
        self.node_list = [] # migarate node list
        self.migarate_list = [] # migarate data list now
        self.res_dict['disk'] = ResourceInfo.RES_DEFAULT_DISK
        self.res_dict['network'] = ResourceInfo.RES_DEFAULT_NETWORK
        self.res_dict['cpu'] = ResourceInfo.RES_DEFAULT_CPU
        self.res_dict['visit'] = ResourceInfo.RES_DEFAULT_VISIT
        self.master = master
    
    def load(self):
        """ load data from db or file"""
        pass
    
    def inc_node_overload(self,cost,size = 0):
        self.__inc_overload(cost,size)
        
    def dec_node_overload(self,cost,size = 0):
        self.dec_overload(cost,size)
    
    def __inc_overload(self,cost,size = 0):
        self.res_dict['cpu'] += cost
        self.res_dict['disk'] += size
        self.res_dict['network'] += gauss(50,50)

    def inc_overload(self,cost,size = 0):
        self.res_dict['visit'] += 1
        __inc_overload(cost,size)

    def dec_overload(self,cost,size = 0):
        self.res_dict['cpu'] -= cost
        self.res_dict['network'] -= gauss(50,50)
        
    def migarate_node(self,nodeid,size,obj=None):
        """ migarate_node"""
        req = RequestInfo(ChunkInfo.CHUNK_REQ_UPLOAD)
        req['size'] = size
        req['dataid'] = nodeid
        req['buf'] = buf
        self.node_list.append(req)
        self.inc_node_overload(ChunkInfo.COST_REQ_UPLOAD_CPU,size)
        
	# also need register timer to do that

        
    def upload(self,nodeid,size,buf=None):
        """ upload file start timer to prompt when it's arrival"""
        req = RequestInfo(ChunkInfo.CHUNK_REQ_UPLOAD)
        req['size'] = size
        req['dataid'] = nodeid
        req['buf'] = buf
        self.req_list.append(req)
        self.inc_overload(ChunkInfo.COST_REQ_UPLOAD_CPU,size)
        # register timer in here

        return True

    def download(self,nodeid):
        """ download file start timer to prompt when it's arrival"""

        if self.data_dict.get(nodeid) is None:
            return False

        req = RequestInfo(ChunkInfo.CHUNK_REQ_DOWNLOAD)        
        req['dataid'] = nodeid
        req['size'] = self.data_dict.get(nodeid).size
        self.req_list.append(req)
        self.inc_overload(ChunkInfo.COST_REQ_DOWNLOAD_CPU,req['size'])
        
        #register timer in here

        return True

    def remove(self,nodeid):
        """ remove file"""
        
        if self.data_dict.get(nodeid) is None:
            return False
        
        req = RequestInfo(ChunkInfo.CHUNK_REQ_REMOVE)
        req['dataid'] = nodeid        
        req['size'] = self.data_dict.get(nodeid) if \
            self.data_dict_get(nodeid).size is None else 0

        self.inc_overload(ChunkInfo.COST_REQ_REMOVE_CPU,0)
        return True

    def update(self,nodeid,size,buf=None):
        
        if self.data_dict.get(nodeid) is None:
            return False

        req = RequestInfo(ChunkInfo.CHUNK_REQ_UPDATE)
        req['dataid'] = nodeid
        req['size'] = size
        req['buf'] = buf
        
        self.inc_overload(ChunkInfo.COST_REQ_UPDATE_CPU,size)

        # register timer in here
        
        return True

    def set_positive(self):
        if not self.status in (NodeInfo.NODE_REPAIR_POS,
                               NodeInfo.NODE_REPAIR_NEG):
            self.status = NodeInfo.NODE_REPAIR_POS
            self.migarate_positive()

    def set_negative(self):
        if not self.status in (NodeInfo.NODE_REPAIR_POS,
                                  NodeInfo.NODE_REPAIR_NEG):
            self.status = NodeInfo.NODE_REPAIR_POS
            self.migarate_positive()

    def __migarate(self):
        """ migarate node carefully"""

        for v in self.migarate_list:
            dst = self.master.migarate_node(self.node)
            dst.migarate_node(v.nodeid,v.size)
            v.status = DATA_STATUS_MIGARATE

        for k,v in self.data_dict:
            dst = self.master.migarate_node(self.nodeid)
            # should read some data in here for migarting
            dst.migarate_node(v.nodeid,v.size)
            v.status = DATA_STATUS_MIGARATE
            self.migarate_list.append(v)
    
    def need_migarate(self):
        return (self.status == NodeInfo.NODE_REPAIR_POS or 
                self.status == NodeInfo.NODE_REPAIR_NEG or
                not self.migarate_list)

    def migarate_end(self,nodeid):
        """ migarate node callback"""
        v = self.data_dict[nodeid]
        v.status = DATA_STATUS_NORMAL
        self.migarate_list.remove(v)

    def migarate_positive(self):
        self.master.update_node_status(self.nodeid,self.status)
        self.__migarate()
        
    def migarate_negative(self):
        self.master.update_node_status(self.nodeid,self.status)
        self.__migarate()
        # start timer to register that 

    def get_res(self):
        return self.res_dict

def doctests():
    import doctest
    return doctest.DocTestSuite()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
