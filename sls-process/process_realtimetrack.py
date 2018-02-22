import json

data=json.load(open("realtimetracj.json"))

##each metrics is a time to value key-value

allo_mems={}
used_mems={}

for track in data:
    allo_mems[track["time"]]=track["cluster.alocated.memory"]
    used_mems[track["time"]]=track["cluster.used.memory"]
    
