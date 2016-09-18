#!/usr/bin/python

import sys
import string
from datetime import datetime


name =''
b = datetime.strptime('1970-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
e = datetime.strptime('1970-01-01 00:00:00','%Y-%m-%d %H:%M:%S')


while True:
    line = sys.stdin.readline()
    if not line:
        break

    row = string.strip(line)
    data = string.split(row)
    day,room_id, domain, beginday, begintime, endday, endtime, gameid, gamename, title,\
    is_live = data

    begintime = datetime.strptime(beginday + ' '+begintime,'%Y-%m-%d %H:%M:%S')
    endtime = datetime.strptime(endday + ' '+endtime,'%Y-%m-%d %H:%M:%S')

    if name != title:
        if name !='':
            print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' %(x,y,z,name,b,e,o,p,q)
        name = title
        b = begintime
        e = endtime
        x = day
        y = room_id
        z = domain
        o = gameid
        p = is_live
        q = gamename

    else:
        if abs(begintime-e).seconds<=300:
            e = endtime
        else:
            print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' %(x,y,z,name,b,e,o,p,q)
            name = title
            b = begintime
            e = endtime
print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' %(x,y,z,name,b,e,o,p,q)


