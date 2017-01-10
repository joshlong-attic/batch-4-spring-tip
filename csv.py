#!/usr/bin/env python
import os,sys,time,re, random

if __name__ == '__main__':

    names = 'josh,jane,bob,patrick,jennifer,tammy,lois,george,jimmy'.split(",")
    ub = [a for a in sys.argv if a.find('-ub=') != -1 ]
    ub = ub[0] if len(ub) > 0 else '-ub=100'
    ub = int (ub.split( '=') [1])
    
    for i in range(ub):
        indx = int( min(  round(random.random() * len(names)), len(names) -1) * 1 )
        n = names[indx]
        print ','.join( (n, str (int(  random.random() * 100)),  '%s@email.com' % n ) )
