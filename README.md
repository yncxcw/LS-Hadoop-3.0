# dCS

In this project, we exploit the to utilize the opportunitisc memory for opportunistic jobs (often used for production jobs).
dCS relies on two component:

(1) Kernel augmentation. We implement UA killer in kernel space to timely and informatively select oom candidates to kill
(2) dCS implementation in NodeManager, a daemon thread to monitor node memory and launch containers.

For more information, please refer our paper (Currently under submission).

## Install and compile
For building this project, plesse refer `BUILDING.txt` for detail. Since my codebase is built on `Hadoop-3.0.0`. 

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
echo 3 > /pro/
```
## Simulation
We are still sorting the scripts we use to parse the Google trace. The setup of simulator will be available soon.

