#!/usr/bin/env python
#  
#  a multiply-timer keeper
#
#

import heapq

class CTimer(object):
    """
    A timer keeper,such as data-structure like min-heap,or other

    >>> h = CTimer()
    >>> h.add_timer(1,110)
    >>> h.add_timer(2,20)
    >>> h.add_timer(3,3)
    >>> h.add_timer(1,100)
    >>> h.remove_timer(2)
    >>> h.print_timer()
    [1, 110]
    [3, 3]
    [1, 100]
    >>> h.next_timeout()
    [[1, 110]]
    >>> h.remove_timer(1)
    >>> h.remove_timer(1)
    >>> h.remove_timer(3)
    >>> h.print_timer() is None
    True
    """

    def __init__(self):
        self.timer_list = []
    
    def add_timer(self,second,obj):
        # from time import time
        # item = second + int(time())
        # heapq.heappush(self.timer_list,(item,second))
        self.timer_list.append([second,obj])
    
    def remove_timer(self,second):
        self.timer_list = filter(lambda x:(x[0] != second),self.timer_list)

    def print_timer(self):
        for w in self.timer_list: print w
        
    def next_timeout(self):
        return heapq.nsmallest(1,self.timer_list,key=lambda x:x[0])
    
def doctests():
    import doctest
    return doctest.DocTestSuite()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
