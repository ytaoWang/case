#!/usr/bin/env python
#
#
# Management Node implementation
#
#

import random
# from ChNode import ChunkInfo
import logging

class DataInfo(object):
    """A class for Data that save data info.
    data guid,chunk node ID List
   
    >>> h = DataInfo({"abcdef":"192.168.1.23,192.168.1.24"})
    >>> h.keys()
    'abcdef'
    >>> h['abcdef']
    ['192.168.1.23', '192.168.1.24']
    
    >>> h.add("abcdef","192.168.1.23")
    False
    >>> h.is_dirty()
    False
    >>> h.get("Abcdef")
    ['192.168.1.23', '192.168.1.24']
    >>> h.get_count("aBCdef")
    2
    >>> h.exist("abcdef",'192.168.1.23')
    True
    
    >>> h.change("abcdef",'192.168.1.23','192.168.1.24')
    False
    >>> h.is_dirty()
    False
    >>> t = DataInfo("abcdef")
    >>> t.keys()
    'abcdef'
    >>> t['abcdef'] = '192.168.1.23'
    >>> t['abcdef']
    ['192.168.1.23']
    >>> t.is_dirty()
    True
    >>> t['abcdef'] = '192.168.1.24'
    >>> t['abcdef']
    ['192.168.1.24']
    >>> t.is_dirty()
    True
    """

    def __init__(self,args):
        self.value = []
        self.update(args)
        self.dirty = False

    def update(self,args):
        if isinstance(args,dict):
            for k,v in args.iteritems():
                self.key = DataInfo._normalize_name(k)
                self.value = v.split(",")
                self.dirty = True
        else:
            self.key = args
    
    def __setitem__(self,key,value):
        if self.key == DataInfo._normalize_name(key):
            for v in self.value:
                self.value.remove(v)
        return self.add(key,value)        
    
    def __getitem__(self,key):
        if self.key != DataInfo._normalize_name(key):
            return None
        return self.value
    
    def __len__(self):
        return len(self.value)
    
    def remove(self,key,nodeid):
        if self.exist(key,nodeid):
            self.value.remove(nodeid)

    # new public function
    def get_count(self,key):
        if self.key != DataInfo._normalize_name(key):
            return None
        return len(self.value)
    
    #
    def exist(self,key,value):
        if self.key != DataInfo._normalize_name(key):
            return False
        return value in self.value

    def change(self,key,old,new):
        """ update old location with new node location"""
        name = DataInfo._normalize_name(key)
        if self.exist(name,new):
            return False
        if not self.exist(name,old):
            self.value.append(new)
        else:
            self.value.remove(old)
            self.value.append(new)
        
        self.dirty = True
        return True
    
    def keys(self):
        return self.key
    
    def get(self,key):
        return self.__getitem__(key)

    def is_dirty(self):
        return self.dirty

    def add(self,key,value):
        if self.key != DataInfo._normalize_name(key):
            return False
        if value.find(",") < 0:
            if value not in self.value:
                self.value.append(value)
                self.dirty = True
                return True
            else:
                return False

        for v in value.split(','):
            if v not in self.value:
                self.value.append(v)
        
        self.dirty = True
        return True

    def migarate(self,srcip,dstip):
        if srcip is not None:
            self.value.remove(srcip)
        if dstip is not None:
            self.value.append(dstip)        
        print 'successful migarate key:',self.value,',src:',srcip,',dst:',dstip
            
    @staticmethod
    def _normalize_name(name):
        """Convert a name to all lowercase
        
        >>> DataInfo._normalize_name("aAAbc12")
        'aAAbc12'
        """
        if name:
            return name

class ResourceInfo:
    """Node's resource info:disk,network,cpu,visit number etc

    >>> h = ResourceInfo()
    >>> str(h)
    'disk=30720,network=100,cpu=0.1,visit=1'
    >>> h['disk'] = 1234
    >>> h['disk']
    1234
    >>> h.update({"disk":12,"network":120,"cpu":10,"visit":3})
    >>> str(h)
    'disk=12,network=120,cpu=10,visit=3'
    >>> h.update('disk=120,network=12,cpu=100,visit=13')
    >>> str(h)
    'disk=120,network=12,cpu=100,visit=13'

    """
    # Constants from the ResourceInfo
    RES_DEFAULT_DISK = 30*1024 #30*1024 M
    RES_DEFAULT_NETWORK = 100 # 100M
    RES_DEFAULT_CPU = 0.1
    RES_DEFAULT_VISIT = 1
    # priority share
    RES_PRIORITY_NEG = 10
    RES_PRIORITY_UP = 45
    RES_PRIORITY_NORMAL = 50

    RES_PRIORITY_CHECK = 100
    RES_PRIORITY_DISK = 0.35
    RES_PRIORITY_NETWORK = -0.2
    RES_PRIORITY_CPU = -0.1
    RES_PRIORITY_VISIT = 0.35
    
    def __init__(self):
        self.res_dict = {}
        self.res_dict['disk'] = self.RES_DEFAULT_DISK
        self.res_dict['network'] = self.RES_DEFAULT_NETWORK
        self.res_dict['cpu']  = self.RES_DEFAULT_CPU
        self.res_dict['visit'] = self.RES_DEFAULT_VISIT
    
    def __getitem__(self,key):
        return self.res_dict[DataInfo._normalize_name(key)]
    
    def __setitem__(self,key,value):
        self.res_dict[DataInfo._normalize_name(key)] = value

    def inc(self):
        self.res_dict['visit'] = self.res_dict['visit'] + 1

    def get(self):
        return self.res_dict        
    
    def get_priority(self):
        """
        calculate priority with share
        """
        return (ResourceInfo.RES_PRIORITY_CHECK 
                + self.res_dict['disk'] * ResourceInfo.RES_PRIORITY_DISK 
                + self.res_dict['network'] * ResourceInfo.RES_PRIORITY_NETWORK
                + self.res_dict['cpu'] * ResourceInfo.RES_PRIORITY_CPU
                + self.res_dict['visit'] * ResourceInfo.RES_PRIORITY_VISIT)
    
    def __str__(self):
        return 'disk=%s,network=%s,cpu=%s,visit=%s' % (self.res_dict['disk'],self.res_dict['network'],self.res_dict['cpu'],self.res_dict['visit'])
        
    def update(self,args):
        """update two style,string and dict
        
        """
        if isinstance(args,dict):
            for k,v in args.iteritems():
                self.__setitem__(k,v)
        else:
            _vlist = [ w for w in args.split(",")]
            for v1 in iter(_vlist):
                (k,v) = v1.split("=")
                self.__setitem__(k,v)
    
    def clear(self):
        self.res_dict.clear()
        

class NodeInfo:
    """A class recording all node's info,like
       Node ID(key),status,<dict:Data guid,Data size>,Resource Info,etc
       Maybe calculate node's priority(TODO next)

       >>> h = NodeInfo("192.168.1.23",NodeInfo.NODE_UP)
       >>> h.status
       8
       >>> h.is_dirty()
       False
       >>> h['abcdef'] = 10
       >>> h['abcdef']
       10
       >>> h.is_dirty()
       True
       >>> h.remove('abcdef',0)
       >>> h.dirty
       True
       >>> h['abcdef'] = 11
       >>> h['abcdef']
       11
       >>> h.update_item('network',10)
       >>> h.get_res()
       {'disk': 30720, 'visit': 1, 'network': 10, 'cpu': 0.10000000000000001}
    """
    
    NODE_REPAIR_POS = (1<<0) # 1
    NODE_REPAIR_NEG = (1<<1) # 2
    NODE_DOWN = (1<<2) # 4
    NODE_UP = (1<<3) # 8
    NODE_NORMAL = (1<<4) # 16

    def __init__(self,nodeid,node,status = NODE_UP):
        """ should keep the Chunk Agent in here? data_dict key is DataInfo"""
        self.nodeid = nodeid
        self._status = status
        self.data_dict = {}
        self.node = node
        self.res_info = ResourceInfo()
        self.dirty = False

    @property
    def node(self):
        return self.node
    @property
    def status(self):
        return self._status 
    @status.setter
    def status(self,value):
        self._status = value
    @property
    def dirty(self):
        return self.dirty
    @property
    def nodeid(self):
        return self.nodeid

    def migarate(self,okey,src,dst):
        self.data_dict[okey].migarate(src,dst)

    def get(self,key,default=None):
        return self.data_dict.get(key,default)

    def __getitem__(self,key):
        name = DataInfo._normalize_name(key)
        return self.data_dict.get(name)
    def __setitem__(self,key,value):
        name = DataInfo._normalize_name(key)
        self.data_dict[name] = value
        self.dirty = True

    def __len__(self):
        return len(self.data_dict)

    def remove(self,key,size=0):
        name = DataInfo._normalize_name(key)
        try:
            self.data_dict.pop(name)
            self.res_info['disk'] += size
            self.dirty = True
        except KeyError:
            pass

        
    def update_data(self,newid,newsize,oldid,osize):
        nname = DataInfo._normalize_name(newid)
        oname = DataInfo._normalize_name(oldid)
        self.remove(oname,osize)
        self.data_dict[nname] = newsize
        self.dirty = True
    
    def update_res(self,args):
        """update resource info """
        self.res_info.update(args)
    
    def update_item(self,key,value):
        """update item of resource info"""
        self.res_info[key] = value

    def get_priority(self):
        
        v = 0
        if self.status == NodeInfo.NODE_UP:
            v = ResourceInfo.RES_PRIORITY_UP
        elif self.status == NodeInfo.NODE_NORMAL:
            v = ResourceInfo.RES_PRIORITY_NORMAL
        elif self.status == NodeInfo.NODE_NEG:
            v = ResourceInfo.RES_PRIORITY_NEG
        else:
            v = 0
        return v + self.res_info.get_priority()

    def get_res(self):
        return self.res_info.get()

    def inc_visit(self):
        return self.res_info.inc()

    def is_dirty(self):
        return self.dirty

class MNode:
    """Management Node implementation,Manage all online nodes,choose the most 
    priority node,migrate node,repair node to other nodes,field:
    <dict>(nodeid,ChunkInfo),<dict>(dataid,DataInfo)
    
    >>> from ChNode import ChunkInfo
    >>> h = MNode()
    >>> d = DataInfo({"abcdef":"192.168.1.23,192.168.1.24"})
    >>> n1 = ChunkInfo("192.168.1.23",NodeInfo.NODE_NORMAL)
    >>> n2 = ChunkInfo("192.168.1.24",NodeInfo.NODE_NORMAL)
    >>> n3 = ChunkInfo("192.168.1.25",NodeInfo.NODE_NORMAL)
    >>> n1.update_item('disk',0)
    >>> n2.update_item('disk',100000)
    >>> h.add(d)
    >>> h.add(n1)
    >>> h.add(n2)
    >>> h.add(n3)
    >>> n4 = []
    >>> n4 = h.download_begin("abcdef")
    >>> n4
    ['192.168.1.23', '192.168.1.24']
    >>> h.download_end("192.168.1.23")
    >>> print len(h.need_migarate_positive())
    1
    >>> for w in h.need_migarate_positive():
    ...     print w.get_priority()
    125.69
    >>> print(h.need_migarate_negative())
    []
    """
    DEFAULT_PRIORITY = 10
    GAP_POSITIVE = 100
    GAP_NEGATIVE = 50

    def __init__(self):
        self.node_dict = {}
        self.data_dict = {}

    def add(self,obj):
        from ChNode import ChunkInfo
        if isinstance(obj,DataInfo):
            # print 'successful to add key:',obj.keys()
            self.data_dict[obj.keys()] = obj
        elif isinstance(obj,ChunkInfo):
            self.node_dict[obj.nodeid] = obj
        else:
            pass

    def remove(self,obj):
        try:
            if isinstance(obj,DataInfo):
                self.data_dict.pop(obj.keys())
            elif isinstance(obj,NodeInfo):
                self.node_dict.pop(obj.nodeid)
            else:
                pass
        except KeyError:
            pass

    def get_node(self,key):
        return self.node_dict.get(key)
    
    def update_node(self,key,args):
        self.node_dict[key].update_res(args)

    def update_node_status(self,nodeid,status):
        self.node_dict[nodeid].status = status

    def update_node_item(self,key,value):
        self.node_dict[key].update_item(key,value)

    def update_data_item(self,key,value):
        self.data_dict[key][key] = value
    
    def get_data(self,key):
        if self.data_dict.get(key) is None:
            import logging
            logging.error("fail to get key:%s in MNode",key)
            return None
        return self.data_dict.get(key).get(key)

    def upload_begin(self,priority = DEFAULT_PRIORITY):
        """ choose the highest priority for client"""
        _nlist = []
        for x in range(3):
            k = self.get_available_node(_nlist,priority)
            if k is not None:
                _nlist.append(k) 
        return _nlist

    def upload_end(self,dataid,_nlist):
        """ commit dataInfo and save it"""
        # convert _nlist to string
        strlist = ','.join(_nlist)
        print 'upload key successful,key:',dataid
        data_info = DataInfo({dataid:strlist})
        self.add(data_info)
        for w in _nlist:
            self.node_dict[w].inc_visit()

    def get_chunk(self,okey):
        return self.node_dict[okey]

    def download_begin(self,dataid):
        return self.get_data(dataid)

    def download_end(self,key):
        try:
            self.node_dict[key].inc_visit()
            print 'successful download key:',key
        except KeyError:
            pass
    
    def update_begin(self,dataid):
        return self.download_begin(dataid)
    
    def update_end(self,okey,osize,nkey,nsize,nlist):
        for w in iter(nlist):
            self.node_dict[w].update_data(nkey,nsize,okey,osize)
            print 'successful update key:',okey

    def remove_begin(self,key):
        obj = self.data_dict.get(key)
        if obj is None:
            logging.error("fail to remove key:%s",key)
            return None
        return obj.get(key)
            
    def remove_end(self,key,nodeid):
        """ Remove given file key"""
        d = self.data_dict[key]
        if len(d[key]) == 1:
            self.data_dict.pop(key)
            #print 'successful remove key:',key
        else:
            self.data_dict[key].remove(key,nodeid)
            #print 'successful remove key:',key,',len:',len(d[key])
                

    def report_down(self,node):
        """ chunk node is down,migarate positive now"""
        src = node.nodeid
        
        for k,v in node.data_dict:
            dst = self.migarate_begin(src)
            dst.migarate_node(k,v.size)

    def migarate_begin(self,nodeid):
        
        _nlist = []
        _nlist.append(nodeid)
        while True:
            k = self.get_available_node(_nlist,0)
            if k is not None:
                return self.node_dict[k]
            
    def migarate_end(self,okey,nodeid,srcid,dstid):
         """ update Management node's id"""
         self.node_dict[nodeid].migarate(okey,srcid,dstid)
         

    def get_available_node(self,exclude,priority = DEFAULT_PRIORITY):
        """get the highest priority node then choose it 
           randomly exclude the exclude list
        """
        a_list = []
        start = 0
        # get all normal status node
        for k,v in self.node_dict.iteritems():
            if v.status in (NodeInfo.NODE_NORMAL,NodeInfo.NODE_UP,
                            NodeInfo.NODE_REPAIR_NEG):
                a_list.append(v)
        
        # sort by priority (ununseless)
        #a_list.sort(lambda x,y:x.status - y.status if x.status != y.status else
        #            x.get_priority() - y.get_priority())

        for v in a_list:
            start = start + priority + v.get_priority()
        
        # now it's choose the highest priority random
        while True:

            if len(a_list) == len(exclude):
                return None

            n = random.uniform(priority,start)
            start = 0
            for k in a_list:
                if(n >= start and n <= (start + k.get_priority() + priority) and 
                   k.nodeid not in exclude):
                    return k.nodeid
                start = start + k.get_priority() + priority

    def average(self):
        """ average priority"""
        
        a_list = []
        averagep = 0.0

        for k,v in self.node_dict.iteritems():
            if v.status == NodeInfo.NODE_NORMAL or v.status == NodeInfo.NODE_UP:
                a_list.append(v)
            
        #a_list = filter(lambda v:v.status == NodeInfo.NODE_NORMAL,self.node_dict)

        # sort by priority increment
        a_list.sort(lambda x,y:int(x.get_priority() - y.get_priority()))
        
        #map((lambda x:print x.get_priority()),a_list)
        
        
        s,t = 0,0
        for v in a_list:
            s += v.get_priority()
            t += 1

        if t == 0:
            return t

        averagep = s/t
        
        print 'average:',averagep,',min:',a_list[0].get_priority(),'max:',a_list[len(a_list)-1].get_priority()
        
        return averagep

    # priority select and migrate positive and negative
    def need_migarate_positive(self):
        """ check whether this system needs migarate positively
            if priority  > 100 * average priority
            should migarate positive
        """
        averagep = self.average()

        a_list = []
        pos_list = []
        
        for k,v in self.node_dict.iteritems():
            if v.status == NodeInfo.NODE_NORMAL or v.status == NodeInfo.NODE_UP:
                a_list.append(v)
        
        # print 'average:',averagep
        
        if averagep == 0:
            return pos_list

        #print "average:",averagep,",total",MNode.GAP_POSITIVE * averagep

        #plist = filter(lambda x:100 * averagep <= x.get_priority(),a_list)
        #print "plist:",plist

        pos_list = filter(lambda x:(x.get_priority() * MNode.GAP_POSITIVE
                                    < averagep),a_list)

        # check copy number if less than 3,should migarate positive
        
        #print "pos_list:",pos_list

        return pos_list
        

    def need_migarate_negative(self):
        """check whether this system needs migarate positively
            if priority  > 100 * average priority
            should migarate positive
        """
        averagep = self.average()
        pos_list = []
        if averagep == 0:
            return pos_list

        a_list = []

        for k,v in self.node_dict.iteritems():
            if v.status == NodeInfo.NODE_NORMAL:
                a_list.append(v)
        
        pos_list = filter(lambda x:(averagep 
                                    <= x.get_priority() * MNode.GAP_NEGATIVE 
                                    and x.get_priority() * MNode.GAP_POSITIVE 
                                    < averagep),a_list)
        
        return pos_list

    def check_migarate(self):
        
        total = 0
        for k,v in self.data_dict.iteritems():
            print 'key:',k,',nodeid:',v[k]
            total += len(v[k])
        
        
        length = 0

        for k,v in self.node_dict.iteritems():
            length += len(v)
            str(v)
            
        print 'master key:',len(self.data_dict),',total num:',total,',all data sum in chunk node:',length

        self.check_migarate_positive()
        self.check_migarate_negative()

    def check_migarate_positive(self):
        """ If need migarate positive,then start migarate directly"""
        plist = []
        plist = self.need_migarate_positive()
        
        if plist is not None:
            # should migarate positive
            # report the chunk node migarate positive

            for v in plist:
                print 'migarate positive v.nodeid:',v.nodeid
                v.set_positive()
            

    def check_migarate_negative(self):
        """ If need migarate negative,then start migarate directly"""
        plist = []
        plist = self.need_migarate_negative()
        
        if plist is not None:
            # should migarate negative
            # report the chunk node migarate negative
            for v in plist:
                print 'migarate negative v.nodeid:',v.nodeid
                v.set_negative()
    

def doctests():
        import doctest
        return doctest.DocTestSuite()    

if __name__ == "__main__":
        import doctest
        doctest.testmod()
