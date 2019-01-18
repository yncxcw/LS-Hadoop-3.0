# Charon

In this project, we exploit to utilize the opportunitisc memory for opportunistic jobs (often used for best-effort jobs).
Charon is composed of on two components:

(1) Kernel augmentation. We implement UA killer in kernel space to select oom candidates to kill in a timely and informative manner.

(2) Charon implementation in NodeManager of Mercury, a daemon thread to monitor node memory and launch containers.

For more information, please refer our paper (Currently under submission).

## Install and compile
To build this project, plesse refer `BUILDING.txt` for detail. My codebase is built on `Hadoop-3.0.0`. 

## UA killer install
Our kernel implementation of UA killer is in this: https://github.com/yncxcw/UAkiller repo. Please downlaod and install 
the modified linux kernel as:
```
make  -j(number of CPU cores)
make install
make modules_install
```
After installing, the cgth\_time can be set by:
```
echo 3 > /pro/sys/vm/cgth_time
```
## Simulation
We are still sorting the scripts we use to parse the Google trace. The setup of simulator will be available soon.

