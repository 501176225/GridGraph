//shmDatadef.h

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/shm.h>
#include <iostream>
#include <vector>
using namespace std;

#define SHARE_MEMORY_BUFFER_LEN 1024

typedef struct jobid{
    int id;
    jobid *next;
}jobid;

struct stuShareMemory{
	int iSignal;
    jobid* global_table[10];
    //vector<jobid*> global_table = vector<jobid*>(10);
	char chBuffer[SHARE_MEMORY_BUFFER_LEN];
	
	stuShareMemory(){
		iSignal = 0;
        for (int i = 0; i < 10; i++) {
            jobid * jobhead = new jobid;
            jobhead->id = 0;
            jobhead->next = NULL;
            global_table[i] = jobhead;
        }
		memset(chBuffer,0,SHARE_MEMORY_BUFFER_LEN);
	}
};



