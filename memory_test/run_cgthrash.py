#!/bin/python
import re
import matplotlib.pyplot as pl
                                   #h     m     s                        ms    .ss    
re_match=re.compile(r'(\S+) (\S+) (\d+):(\d+):(\d+) (\S+) (\S+) \[(\s+)(\d+)\.(\d+)\] cg_thrash (\d+) add: (\d+)  (\d+)')

class cg_thrash:

    def __init__(self,cg_id,time,pgmj,pgev):
        self.cg_id  =cg_id
        self.time   =time
        self.pgmj   =pgmj
        self.pgev   =pgev
    
    def evict():
        pass
        

def average_pgmj(cg_thrashs):
    average=0.0
    for cg_thrash in cg_thrashs:
        average = average + cg_thrash.pgmj
    return average*1.0/len(cg_thrashs)
    



if __name__=="__main__":
    start=0
    fthrash=open("syslog","r")
    cg_maps={}
    for line in fthrash.readlines():
        match=re_match.match(line)
        if match:
            groups=match.groups()
            time   =int(groups[8])+(int(groups[9])*1.0)/1000000
            cg_id  =int(groups[10])
            pgmj   =int(groups[11])
            pgev   =int(groups[12])
            if start == 0:
                start = time
            else:
                time  = time - start
            cg = cg_thrash(cg_id,time,pgmj,pgev)
            if cg_maps.get(cg_id) is None:
                cg_maps[cg_id]=[]
            cg_maps[cg_id].append(cg)

    print len(cg_maps)
    for key in cg_maps.keys():
        if average_pgmj(cg_maps[key]) < 10000:
            continue
        X=[]
        Y=[]
        Z=[]
        count=50
        for thrash in cg_maps[key]:
            X.append(thrash.time)
            Y.append(thrash.pgmj)
            Z.append(thrash.pgev)
            count = count -1
            if count <= 0:
                break
            
        pl.figure()
        pl.scatter(X,Y,label=key)
        pl.scatter(X,Z,label=key)
    pl.show()
        


        
