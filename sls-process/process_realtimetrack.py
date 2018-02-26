import json
import matplotlib.pyplot as pl
data=json.load(open("realtimetrack.json"))

##each metrics is a time to value key-value

allo_mems=[]
used_mems=[]

START=-1

for track in data:
    if START==-1:
        START = track["time"]
    time = track["time"] - START; 
    allo_mems.append((time,track["cluster.allocated.memory"]))
    used_mems.append((time,track["cluster.used.pmem"]))
    

pl.plot(map(lambda x:x[0],allo_mems),map(lambda x:x[1],allo_mems))
pl.plot(map(lambda x:x[0],used_mems),map(lambda x:x[1],used_mems))

pl.show()
