#script to monit cgroup memory usage


#!/bin/python
import re
import os
import matplotlib.pyplot as pl
import time
from datetime import datetime
re_match=re.compile(r'\/sys\/fs\/cgroup\/memory\/docker\/(\w+)')


CGROUP_PATH="/sys/fs/cgroup/memory/docker"

clock=1

cg_maps={}

class cg_lru:
    
    def __init__(self,cg_id,time,sdatetime,acan,inan,acfi,infi,pgfa,pgmj,pgin,pgou,pgev,swap):
        self.cg_id=cg_id
        self.inan =inan
        self.acan =acan
        self.acfi =acfi
        self.infi =infi
        self.pgfa =pgfa
        self.pgmj =pgmj
        self.pgin =pgin
        self.pgou =pgou
        self.pgev =pgev
        self.swap =swap
        self.time =time
        self.sdatetime=sdatetime

    def sum_in(self):
        return self.inan+self.infi

    def sum_ac(self):
        return self.acan+self.acfi

    def sum_total(self):
        return self.sum_in()+self.sum_ac()

##return if the container is alive
def monitor_memcg():
    found=False
    for dirn,subdirs,files in os.walk(CGROUP_PATH):
        match=re_match.match(dirn)
        if match:
            found=True
            dirn=dirn+"/memory.stat"
            fmemcg=open(dirn,"r")
            inan=-1
            acan=-1
            infi=-1
            acfi=-1
            pgfa=-1
            pgmj=-1
            pgin=-1
            pgou=-1
            swap=-1
            for line in fmemcg.readlines():
                if "inactive_anon" in line:
                    inan=int(line.split()[1])/1000000.0
                elif "active_anon" in line:
                    acan=int(line.split()[1])/1000000.0
                elif "inactive_file" in line:
                    infi=int(line.split()[1])/1000000.0
                elif "active_file" in line:
                    acfi=int(line.split()[1])/1000000.0
                elif "pgfault" in line:
                    pgfa=int(line.split()[1])
                elif "pgmajfault" in line:
                    pgmj=int(line.split()[1])
                elif "pgpgin" in line:
                    pgin=int(line.split()[1])
                elif "pgpgout" in line:
                    pgou=int(line.split()[1])
                elif "page_eviction" in line:
                    pgev=int(line.split()[1])
                elif "swap" in line:
                    swap=int(line.split()[1])/1000000.0
                else:
                    pass
            sdatetime=str(datetime.now())
            cg = cg_lru(dirn,clock,sdatetime,acan,inan,acfi,infi,pgfa,pgmj,pgin,pgou,pgev,swap)
            if cg_maps.get(dirn) is None:
                cg_maps[dirn]=[]
            cg_maps[dirn].append(cg)
    return found
    



if __name__=="__main__":
    ##loop for the end of the monitor
    while monitor_memcg()==True:
        clock=clock+1
        time.sleep(1) 

    emf=open("lru.data","w")
    if len(cg_maps)<=0:
        exit()
    for key in cg_maps.keys():
        A=[]
        B=[]
        C=[]
        D=[]
        E=[]
        F=[]
        X=[]
        Y=[]
        Z=[]
        U=[]
        W=[]
        V=[]
        count=0
        emf.write("%s\n"%key)
        for lru in cg_maps[key]:
            A.append(lru.pgfa)
            B.append(lru.pgmj)
            C.append(lru.pgin)
            D.append(lru.pgou)
            E.append(lru.pgev)
            V.append(lru.swap) 
            X.append(lru.time)
            Y.append(lru.inan)
            Z.append(lru.acan)
            U.append(lru.infi)
            W.append(lru.acfi)
            emf.write(lru.sdatetime+" pgmj "+str(lru.pgmj)+"\n")
            emf.write(lru.sdatetime+" pgev "+str(lru.pgev)+"\n")
            emf.write(lru.sdatetime+" swap "+str(lru.swap)+"\n")
        print "figure"
        pl.figure(1)
        pl.title(key)
        pl.subplot(211)
        #pl.plot(X,A,label="pgfa")
        pl.plot(X,B,label="pgmj")
        pl.plot(X,E,label="pgev")
        #pl.plot(X,C,label="pgin")
        #pl.plot(X,D,label="pgou")
        pl.legend()
        pl.subplot(212)
        pl.plot(X,V,label="swap")
        pl.plot(X,Y,label="inan")
        pl.plot(X,Z,label="acan")
        pl.plot(X,U,label="infi")
        pl.plot(X,W,label="acfi")
        pl.legend()
        pl.show()
    emf.close()
