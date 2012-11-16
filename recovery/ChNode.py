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

from time import time
import random
import heapq
import logging

from MNode import NodeInfo,MNode,ResourceInfo

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
    def update_time(self):
        pass
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
        self._visit = 0
        pass

    @property
    def visit(self):
        return self._visit

    @visit.setter
    def visit(self,value):
        self._visit = value

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

    NODE_DEFAULT_TIME = 60
    COST_REQ_UPLOAD_CPU  = 0.15
    COST_REQ_DOWNLOAD_CPU = 0.15
    COST_REQ_UPDATE_CPU = 0.15
    COST_REQ_REMOVE_CPU = 0.1
    RATE_NET = 90 # assume that network rate is 90MB/s

    def __init__(self,nodeid,master):
        self.nodeid = nodeid
        self.status = NodeInfo.NODE_UP
        self.res_dict = {}
        self.data_dict = {}
        self.req_list = []
        self.node_list = [] # migarate node list
        self.migarate_list = [] # migarate data list now
        self.node = NodeInfo(nodeid,self.status)
        self.master = master
        self.counter = 0 # migarate node counter

    def get_priority(self):
        return self.node.get_priority()

    def inc_visit(self):
        return self.node.inc_visit()

    def net_rate(self,size):
        return int(size/ChunkInfo.RATE_NET)
    
    def get_min(self):
        print "request list:",len(self.req_list),",node list:",len(self.node_list)

        import sys
        rmin = sys.maxint if len(self.req_list) is 0 else heapq.nsmallest(1,self.req_list,key=lambda x:x['time'])[0]['time']
        nmin = sys.maxint if len(self.node_list) is 0 else heapq.nsmallest(1,self.node_list,key=lambda x:x['time'])[0]['time']

        return min(int(rmin),int(nmin))
        # return int(min(heapq.nsmallest(1,
        #                               self.req_list,key=lambda x:x['time'])[0] 
        #               if not len(self.req_list) is 0 else 0,
        #               heapq.nsmallest(1,
        #                               self.node_list,key = lambda x:x['time'])[0]
        #               if not len(self.node_list) is 0 else 0))
    
    def load(self):
        """ load data from db or file"""
        pass
    
    def inc_node_overload(self,cost,size = 0):
        self.__inc_overload(cost,size)

    @staticmethod
    def do_check(w):
        w.do_action()
        
    def do_action(self):
        """ check timer event then implement it"""
        now = self.get_min()        

        while True:
            if len(self.req_list) is 0:
                break
            rmin = heapq.nsmallest(1,self.req_list,key=lambda x:x['time'])[0]['time']
            if rmin - now > 1:
                break

            obj = heapq.heappop(self.req_list)
            strid = obj['dataid']

            if obj['op'] == RequestInfo.CHUNK_REQ_UPLOAD:
                obj['obj'].upload_end(obj['dataid'],obj['size'],self.nodeid)
                c = CDataInfo()
                c.size = obj['size']
                c.nodeid = strid
                self.node[strid] = c
                self.data_dict[strid] = c

            elif obj['op'] == RequestInfo.CHUNK_REQ_DOWNLOAD:
                obj['obj'].download_end(obj['dataid'],obj['size'],self.nodeid)

            elif obj['op'] == RequestInfo.CHUNK_REQ_UPDATE:
                obj['obj'].update_end(obj['dataid'],obj['size'],self.nodeid)
                v = self.node.get(strid)
                print 'update file key:',strid
                if v is None:
                    logging.error('fail to update file key:%s,nodeid:%s',strid,self.nodeid)
                    return
                v.size = obj['size']
                self.data_dict[strid] = v
                self.node[strid] = v
            else:
                pass

        while True:

            if len(self.node_list) is 0:
                break
            nmin = heapq.nsmallest(1,self.node_list,key=lambda x:x['time'])[0]['time']
            if nmin - now > 1:
                break

            obj = heapq.heappop(self.node_list)
            if not obj['obj'] is None:
                c = CDataInfo()
                c.size = obj['size']
                c.nodeid = obj['dataid']
                self.node[c.nodeid] = c
                self.data_dict[c.nodeid] = c
                obj['obj'].migarate_end(obj['dataid'],self.nodeid)

    def __len__(self):
        return len(self.data_dict)
            
    def __str__(self):
        
        print "node id:",self.nodeid,',len:',len(self.data_dict),',priority:',self.get_priority()

        for k,v in self.data_dict.iteritems():
            print "key:",k,",size:",v.size
            
        return 'end'

    def update_item(self,key,value):
        self.node.update_item(key,value)
    
    def dec_node_overload(self,cost,size = 0):
        self.dec_overload(cost,size)
    
    def __inc_overload(self,cost,size = 0):
        res_dict = self.node.get_res()
        self.node.update_item('cpu',res_dict['cpu'] + cost)
        self.node.update_item('disk',res_dict['disk'] + size)
        self.node.update_item('network',res_dict['network'] + random.uniform(0,100))
        # self.node['cpu'] += cost
        # self.node['disk'] += size
        # self.node['network'] += gauss(50,50)

    def inc_overload(self,cost,size = 0):
        self.node.inc_visit()
        self.__inc_overload(cost,size)

    def dec_overload(self,cost,size = 0):
        res_dict = self.node.get_res()
        self.node.update_item('cpu',res_dict['cpu'] - cost)
        self.node.update_item('network',res_dict['network'] - random.uniform(0,100))
        self.node.update_item('disk',res_dict['disk'] - size)
        # self.node['network'] -= gauss(50,50)
        
    def migarate_down(self):
        """ node is down,migarate now"""
        self.status = NodeInfo.NODE_DOWN
        self.node.status = self.status
        self.__migarate()

    def migarate_node(self,nodeid,size,obj=None):
        """ migarate_node"""
        req = RequestInfo(RequestInfo.CHUNK_REQ_UPLOAD)
        req['size'] = size
        req['dataid'] = nodeid
        # req['buf'] = buf
        req['obj'] = obj
        req['time'] += self.net_rate(size)

        self.node_list.append(req)
        self.inc_node_overload(ChunkInfo.COST_REQ_UPLOAD_CPU,size)
        print 'migarate_node,node id:%s,from %s,key:%s' % (self.nodeid,obj.nodeid,nodeid)
	# also need register timer to do that when finish to report another Chunk
        # assume that network rates is:90MB/s
        # obj.migarate_end(nodeid)
        
        
    def upload(self,nodeid,size,obj,buf=None):
        """ upload file start timer to prompt when it's arrival"""
        #c = CDataInfo()
        #c.size = size
        #c.nodeid = nodeid
        req = RequestInfo(RequestInfo.CHUNK_REQ_UPLOAD)
        req['size'] = size
        req['dataid'] = nodeid
        req['buf'] = buf
        req['obj'] = obj
        req['time'] += self.net_rate(size)

        # print 'register timer:',req['time'],',relative:',req['time'] - time()
        #self.node[nodeid] = c
        #self.data_dict[nodeid] = c
        self.req_list.append(req)
        self.inc_overload(ChunkInfo.COST_REQ_UPLOAD_CPU,size)

        # register timer in here when finish to report client,like RPC
        # self.timer.add_timer(int(time() + self.net_rate(size)))
        return True

    def download(self,nodeid,obj):
        """ download file start timer to prompt when it's arrival"""

        if self.data_dict.get(nodeid) is None:
            return False

        req = RequestInfo(RequestInfo.CHUNK_REQ_DOWNLOAD)        
        req['dataid'] = nodeid
        req['size'] = self.data_dict.get(nodeid).size
        req['obj'] = obj
        self.req_list.append(req)
        self.inc_overload(ChunkInfo.COST_REQ_DOWNLOAD_CPU,req['size'])
        req['time'] += self.net_rate(req['size'])
        # register timer in here when finish to report client,like RPC
        # self.timer.add_timer(int(time() + self.net_rate(size)))
        return True

    def remove(self,nodeid):
        """ remove file"""
        v = self.node.get(nodeid) or self.data_dict.get(nodeid)
        if v is None:
            logging.error("fail to get key:%s",nodeid)
            return False
        
        #req = RequestInfo(RequestInfo.CHUNK_REQ_REMOVE)
        #req['dataid'] = nodeid        
        #req['size'] = self.data_dict.get(nodeid) if \
        #    self.data_dict.get(nodeid).size is None else 0

        self.inc_overload(ChunkInfo.COST_REQ_REMOVE_CPU,0)
        self.node.remove(nodeid,v.size)
        self.data_dict.pop(nodeid)
        return True

    def update(self,nodeid,size,obj,buf=None):
        
        v = self.data_dict.get(nodeid)
        if v is None:
            logging.error("cannot get key:%s in node:%s",nodeid,self.nodeid)
            return False

        req = RequestInfo(RequestInfo.CHUNK_REQ_UPDATE)
        req['dataid'] = nodeid
        req['size'] = size
        req['buf'] = buf
        req['obj'] = obj
        req['time'] += self.net_rate(size)
        self.inc_overload(ChunkInfo.COST_REQ_UPDATE_CPU,size)
        self.req_list.append(req)
        #v.size = size
        #self.data_dict[nodeid] = v
        #self.node[nodeid] = v
        # register timer in here
        return True

    def set_positive(self):
        if not self.status in (NodeInfo.NODE_REPAIR_POS,
                               NodeInfo.NODE_REPAIR_NEG):
            self.status = NodeInfo.NODE_REPAIR_POS
            self.node.status = self.status
            self.migarate_positive()

    def set_negative(self):
        if not self.status in (NodeInfo.NODE_REPAIR_POS,
                                  NodeInfo.NODE_REPAIR_NEG):
            self.status = NodeInfo.NODE_REPAIR_POS
            self.node.status = self.status
            self.migarate_negative()
    
    def down(self):
        """ set chunk node is down"""
        self.status = NodeInfo.NODE_DOWN
        self.node.status = self.status
        self.master.report_down(self)

            
    def __migarate(self):
        """ migarate node carefully"""
        for v in self.migarate_list:
            dst = self.master.migarate_begin(v.nodeid)
            if dst is None:
                continue
            dst.migarate_node(v.nodeid,v.size,self)
            self.counter += 1
            v.status = CDataInfo.DATA_STATUS_MIGARATE

        for k,v in self.data_dict.iteritems():
            dst = self.master.migarate_begin(k)
            if dst is None:
                logging.warn("miagarate node src:%s,dst is None",self.nodeid)
                continue
            # should read some data in here for migarting
            dst.migarate_node(v.nodeid,v.size,self)
            self.counter += 1
            v.status = CDataInfo.DATA_STATUS_MIGARATE
            # self.migarate_list.append(v)
    
    def need_migarate(self):
        return (self.status == NodeInfo.NODE_REPAIR_POS or 
                self.status == NodeInfo.NODE_REPAIR_NEG or
                not self.migarate_list)

    def migarate_end(self,nodeid,dstid):
        """ migarate node callback"""
        self.counter -= 1
        if self.data_dict.get(nodeid) is None:
            logging.error('fail to migarage node nodeid:%s,src:%s,dst:%s',nodeid,self.nodeid,dstid)
            return

        v = self.data_dict[nodeid]
        v.status = CDataInfo.DATA_STATUS_NORMAL
        print 'migarate src:',self.nodeid,',dst id:',dstid
        self.master.migarate_end(nodeid,None,dstid)
        if self.counter <= 0 and (self.status == NodeInfo.NODE_REPAIR_POS or 
                                  self.status == NodeInfo.NODE_REPAIR_NEG):
            self.status = NodeInfo.NODE_NORMAL
            self.node.status = self.status
            self.counter = 0

        try:
            self.migarate_list.remove(v)
        except Exception:
            pass

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
