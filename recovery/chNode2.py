#!/usr/bin/env python
#
# 
#
#
#
#
#
#

class DataInfo(dict):
    """ A dictionary that maintains data metadata for all keys
    
    data guid[key] <---> chunk node ID,data status 
                                      chunk node ID <---> data status

    Support multiple values per key via a pair for new methods,
    add() and get_list(). The regular dictionary interface retuns a single value
    per key,with multiple values joined by a comma
    
    
    >>> h = DataInfo({"abcdef":"192.168.1.23=0,192.168.1.24=1,192.168.1.44=0"})
    >>> h.keys()
    ['abcdef']
    >>> h["abcdef"]
    '192.168.1.23=0,192.168.1.24=1,192.168.1.44=0'
    
    >>> h.add("abcde1","192.168.1.23",0)
    >>> h.add("abcde1","192.168.1.23",1)
    >>> h["Abcde1"]
    '192.168.1.23=0,192.168.1.23=1'
    >>> h.get_list("abcde1")
    ['192.168.1.23=0', '192.168.1.23=1']
    >>> h.get_value_count("abcde1")
    2
    >>> h.is_exist_value("abcde1",'192.168.1.23')
    True
    """
    def __init__(self,*args,**kwargs):
        
        dict.__init__(self)
        self._as_dict = {}
        self.update(*args,**kwargs)

    # new public function
    
    def add(self,name,value,status):
        """Adds a new value for the given key."""
        norm_name = DataInfo._normalize_name(name)
        if norm_name in self:
            # bypass our override of __setitem__ since it modifies _as_dict
            dict.__setitem__(self,norm_name,self[norm_name] + ',' + value)
            self._as_dict[norm_name][value] = status
        else:
            self[norm_name][value] = status
        
    def get_list(self,name):
        """Returns all values for the given header as a list."""
        norm_name = DataInfo._normalize_name(name)
        _data_list = []
        for k,v in self._as_dict.get(norm_name):
            _data_list.append("%s,%s" % (k,v))
        return _data_list

    def get_value_count(self,name):
        """Returns value number for the given name."""
        norm_name = DataInfo._normalize_name(name)
        return len(self._as_dict.get(norm_name,{}))
    
    def is_exist_value(self,name,value):
        """Returns the value given name is in corresponding value"""
        norm_name = DataInfo._normalize_name(name)
        _vdict = self._as_dict.get(norm_name,{})
        return _vdict.get(value) == None
    
    def get_all(self):
        """Returns an iterable of all (name,value) pairs.

        If a header has multiple values, multiple pairs will be 
        returned with the same value
        """
        for name,list in self._as_dict.iteritems():
            for value in list:
                yield (name,get_list(name))

        
    # dict implementations overrides
        
    def __setitem__(self,name,value):
        norm_name = DataInfo._normalize_name(name)
        dict.__setitem__(self,norm_name,value)
        self._as_dict[norm_name] = dict(value)

    def __getitem__(self,name):
        return dict.__getitem__(self,DataInfo._normalize_name(name))
    
    def __delitem__(self,name):
        norm_name = DataInfo._normalize_name(name)
        dict.__delitem__(self,norm_name)
        del self._as_dict[norm_name]

    def get(self,name,default=None):
        return dict.get(self,DataInfo._normalize_name(name),default)
        
    def update(self,*args,**kwargs):
        #dict.update bypasses our __setitem__
        for k,v in dict(*args,**kwargs).iteritems():
            if not DataInfo._is_valid(v):
                raise 
            # print("key:%s,v:%s" % (k,v))
            self[k] = v

    @staticmethod
    def _is_valid(value):
        """test value whether is valid or not
        
        >>> DataInfo._is_valid("19")
        False
        
        >>> DataInfo._is_valid("12=23,12=3")
        False

        """
        
        _data_list = [ w for w in value.split(",")]
        _data_dict = {}
        for it in _data_list:
            if len(it.split("=")) != 2:
                return False
            (k,v) = it.split('=')

            if k is None or v is None or _data_dict.get(k) is not None:
                return False
            _data_dict[k] = v
            
        return True

    @staticmethod
    def _normalize_name(name):
        """Converts a name to all lowcase name
        
        >>> DataInfo._normalize_name("aAbC")
        'aabc'
        """
        if name:
            return name.lower()

def doctests():
    import doctest
    return doctest.DocTestSuite()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
