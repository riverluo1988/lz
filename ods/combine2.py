#!/usr/bin/python

import sys
import string
from datetime import datetime


name ='\tplu'
b = datetime.strptime('1970-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
e = datetime.strptime('1970-01-01 00:00:00','%Y-%m-%d %H:%M:%S')



while True:
    line = sys.stdin.readline()
    if not line:
        break

    row = string.strip(line)
    data = string.split(row)
    if len(data) != 20:
        day = data[0]
        room_id = data[1]
        domain = data[2]
        beginday = data[3]
        begintime =  data[4]
        endday = data[5]
        endtime = data[6]
        max_viewer = int(data[7])
        accumulative_viewer_total = int(data[8])
        execute_times = int(data[9])
        try:
            begin_flower_count = int(data[10])
        except:
            begin_flower_count = 0
        try:
            end_flower_count = int(data[11])
        except:
            end_flower_count = 0
        from_qq = data[12]
        live_permission = data[13]
        live_source_type = data[14]
        live_stream_type = data[15]
        gameid = data[16]
        gamename = data[17]
        title = ' '.join(data[18:-1])
        is_live = data[-1]
    else:
        day,room_id, domain, beginday, begintime, endday, endtime, max_viewer,\
        accumulative_viewer_total, execute_times, begin_flower_count, end_flower_count,\
        from_qq,live_permission,live_source_type, live_stream_type,\
        gameid,gamename, title, is_live = data

        max_viewer = int(max_viewer)
        accumulative_viewer_total = int(accumulative_viewer_total)
        execute_times = int(execute_times)
        try:
            begin_flower_count = int(begin_flower_count)
        except:
            begin_flower_count = 0
        try:
            end_flower_count = int(end_flower_count)
        except:
            end_flower_count = 0


    begintime = datetime.strptime(beginday + ' '+begintime,'%Y-%m-%d %H:%M:%S')
    endtime = datetime.strptime(endday + ' '+endtime,'%Y-%m-%d %H:%M:%S')


    if name != title:
        if name !='\tplu':
            print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s'\
            %(x,y,z,b,e,r,s,t,u,v,h,i,j,k,o,p,name,q)
        name = title
        b = begintime
        e = endtime
        x = day
        y = room_id
        z = domain
        r = max_viewer
        s = accumulative_viewer_total
        t = execute_times
        u = begin_flower_count
        v = end_flower_count
        h = from_qq
        i = live_permission
        j = live_source_type
        k = live_stream_type
        o = gameid
        p = gamename
        q = is_live

    else:
        if room_id == y:

            if abs(begintime-e).seconds<=300:
                e = endtime
                s += accumulative_viewer_total
                t += execute_times

                if max_viewer>r:
                    r = max_viewer
                if end_flower_count > v:
                    v = end_flower_count
                if begin_flower_count < u:
                    u = begin_flower_count




            else:
                print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s'\
                %(x,y,z,b,e,r,s,t,u,v,h,i,j,k,o,p,name,q)
                name = title
                b = begintime
                e = endtime
                s = accumulative_viewer_total
                t = execute_times
                r = max_viewer
                u = begin_flower_count
                v = end_flower_count
        else:
            print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s'\
            %(day,room_id,domain,begintime,endtime,max_viewer,accumulative_viewer_total,\
            execute_times,begin_flower_count,end_flower_count,\
            from_qq,live_permission,live_source_type, live_stream_type,
            gameid,gamename,title,is_live)






print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
%(x,y,z,b,e,r,s,t,u,v,h,i,j,k,o,p,name,q)


