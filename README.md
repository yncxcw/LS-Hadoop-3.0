# dCS

In this project, we exploit the to utilize the opportunitisc memory for opportunistic jobs (often used for production jobs).
dCS relies on two component:

(1) Kernel augmentation. We implement UA killer in kernel space to timely and informatively select oom candidates to kill
(2) dCS implementation in NodeManager, a daemon thread to monitor node memory and launch containers.

For more information, please refer our paper (Currently under submission).

## Install and compile
For building this project, plesse refer `BUILDING.txt` for detail. Since my codebase is built on `Hadoop-3.0.0`. 
