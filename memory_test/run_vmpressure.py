#!/bin/python
import re
import matplotlib.pyplot as pl
                                   #h     m     s                        ms   . ss    
re_match=re.compile(r'(\S+) (\S+) (\d+):(\d+):(\d+) (\S+) (\S+) \[(\s+)(\d+)\.(\d+)\] vm pressure memcg (\d+) scanned (\d+) reclaimed (\d+) level (\d+)')

class cg_pressure:

    def __init__(self,cg_id,time,scanned,reclaimed,level):
        self.cg_id    =cg_id
        self.time     =time
        self.scanned  =scanned
        self.reclaimed=reclaimed
        self.level    =level

    def pressure_score(self):
        return (self.scanned - self.reclaimed)*1.0/self.scanned




if __name__=="__main__":
    start=0
    fpressure=open("syslog","r")
    cg_maps={}
    for line in fpressure.readlines():
        match=re_match.match(line)
        if match:
            groups=match.groups()
            time     =int(groups[8])+(int(groups[9])*1.0)/1000000
            cg_id    =int(groups[10])
            scanned   =int(groups[11])
            reclaimed=int(groups[12])
            level    =int(groups[13])
            if start == 0:
                start = time
            else:
                time  = time - start
            cg = cg_pressure(cg_id,time,scanned,reclaimed,level)
            if cg_maps.get(cg_id) is None:
                cg_maps[cg_id]=[]
            cg_maps[cg_id].append(cg)

    print len(cg_maps)
    for key in cg_maps.keys():
        ##filter some cg maps
        if len(cg_maps[key]) < 100:
            continue
        print key
        X=[]
        Y=[]
        for pressure in cg_maps[key]:
            X.append(pressure.time)
            Y.append(pressure.pressure_score())
            pass
        if key==94:
            pl.plot(X,Y,label=key)
    pl.show()
        


        
