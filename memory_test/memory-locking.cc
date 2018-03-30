#include<iostream>
#include<thread>
#include<vector>
#include<ctime>
#include<sys/time.h>
#include<mutex>
using namespace std;

//size of memory in terms of GB
#define S 5*1024
//size of 1MB
#define M 1024*1024
//number of threads


//shared data array
char *p[S];

typedef struct _thread_data{

  int index;

}thread_data;

//global locks
mutex lock_x;



void init_data(){

for(int i=0;i<S;i++){
  p[i]=new char[M];
  for(int j=0;j<M;j++){
   p[i][j]='a';
  }

}

}

void access(void *arg){


thread_data *data = (thread_data *)arg;
int index = data->index;

//sieze of 1K pointer
int loop=0;

lock_x.lock();
//access 1M data for each time
for(int i=0;i<M;i++){
  p[index][i]='0'+index%10;
}
lock_x.unlock();
return;
}


int main(){

init_data();

//allocation
timespec ts,te;
clock_gettime(CLOCK_REALTIME, &ts);
std::cout<<"start"<<endl;
vector<thread> threads;
//the # of threads = the number of 1M block size
for(int i=0;i<S;i++){
 thread_data *thread_data_t = new thread_data();
 thread_data_t->index = i;
 threads.push_back(thread(&access,(void *)thread_data_t));
}

for(auto& th : threads){
 th.join();
}
clock_gettime(CLOCK_REALTIME, &te);
cout<<"finish "<<double(te.tv_sec-ts.tv_sec)<<endl;
return 0;

}
