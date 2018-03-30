#include<iostream>
#include<thread>
#include<vector>
#include<ctime>
#include<sys/time.h>
#include<unistd.h>
#include<sys/syscall.h>
#include<sys/types.h>
#include<stdio.h>
//default is 5*1024
//size of memory in terms of GB
#define S 5*1024
//size of 1MB
#define M 1024*1024
//default is 8
//number of threads
#define N 10


using namespace std;


void allocate(int tid,char** p){

cout<<tid<<" start allocate"<<endl;

for(int i=0;i<S;i++){

  p[i]=new char[M];
  for(int j=0;j<M;j++){
   p[i][j]='a';
  }

}


}

void do_access(int tid,char** p){

cout<<tid<<" start access"<<endl;
int loop=0;

//default is 2
while(loop<2){
  //default is S
  for(int i=0;i<S;i++){
    for(int n=0;n<M;n++){
      p[i][n]='b';      
    }
  }
 loop++;
}


}
void _memory_(int index){

//sieze of 1K pointer
char *p[S];
allocate(index,p);
do_access(index,p);

return;
}

int main(){
int pid=getpid();
int memcg_id=syscall(333,pid);
std::cout<<"pid: "<<pid<<"memcg_id: "<<memcg_id<<endl;

//allocation
timespec ts,te;
clock_gettime(CLOCK_REALTIME, &ts);
std::cout<<"start"<<std::endl;
std::vector<thread> threads;
for(int i=1;i<=N;i++){
 threads.push_back(thread(&_memory_,i));
}

for(auto& th : threads){
 th.join();
}
clock_gettime(CLOCK_REALTIME, &te);
std::cout<<"finish "<<double(te.tv_sec-ts.tv_sec)<<std::endl;
return 0;

}
