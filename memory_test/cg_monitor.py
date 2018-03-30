#!/bin/python
import os
import time
import pickle

import matplotlib.pyplot as pl

metrics={}

def read_metrics(path):
    if os.path.exists(path) is False:
        return
    if ("stat" in path) or ("proc" in path):
        fopen=open(path,"r")
        #print path
        for line in fopen.readlines():
            #print line
            #print line.split()
            
            key  = line.split()[0]
            value= line.split()[1]
            if key not in metrics[path]:
                continue;
            subpath=path+"/"+key
            if metrics.get(subpath) is None:
                metrics[subpath]=[]
            metrics[subpath].append((int)(value))
        fopen.close() 
    else:
        fopen=open(path,"r")
        value=fopen.readline()
        metrics[path].append((int)(value))
        fopen.close()
        

def draw_metrics():
    ##dump the results to disk
    pickle.dump(metrics,open("metrics","wb"))
    for key in metrics.keys():
        if len(metrics[key]) > 0 and not isinstance(metrics[key][0],str):
            fig=pl.figure(figsize=(10,3))
            datas=metrics[key]
            pl.plot(datas)
            pl.title(key)
        else:
            pass
    pl.show()      
    

if __name__=="__main__":

    #metrics["/sys/fs/cgroup/memory/memory.failcnt"]             =[]
    #metrics["/sys/fs/cgroup/memory/memory.kmem.failcnt"]        =[]
    #metrics["/sys/fs/cgroup/memory/memory.kmem.limit_in_bytes"] =[]
    #metrics["/sys/fs/cgroup/memory/memory.kmem.usage_in_bytes"] =[]
    #metrics["/sys/fs/cgroup/memory/memory.limit_in_bytes"]      =[]
    #metrics["/sys/fs/cgroup/memory/memory.max_usage_in_bytes"]  =[]
    #metrics["/sys/fs/cgroup/memory/memory.memsw.usage_in_bytes"]=[]
    #metrics["/sys/fs/cgroup/memory/memory.usage_in_bytes"]      =[]
    #metrics["/sys/fs/cgroup/memory/memory.stat"]                =[]
    metrics["/proc/meminfo"]=["MemFree:","MemAvailable:","Buffers:","Cached:",
                              "SwapCached:","Active:","Inactive:","SwapTotal:",
                              "SwapFree:","Dirty:","Writeback:","AnonPages:",
                              "Mapped:","Shmem:","Slab:","SReclaimable:","SUnreclaim:",
                              "PageTables:","Active(anon):","Inactive(anon):",
                              "Active(file):","Inactive(file):","Unevictable:"
                             ]

    ttime=90
    while ttime>0:
        ##read metrics from files
        for key in metrics.keys():
            read_metrics(key)
        ttime=ttime-0.05
        time.sleep(0.05)

    draw_metrics() 
        
    
