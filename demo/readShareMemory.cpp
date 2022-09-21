//readShareMemory.cpp

#include "shmDatadef.h"

int main(int argc, char* argv[])
{
	void *shm = NULL;
	struct stuShareMemory *stu;
	int shmid = shmget((key_t)1234, sizeof(struct stuShareMemory), 0666|IPC_CREAT);
	if(shmid == -1)
	{
		printf("shmget err.\n");
		return 0;
	}

	shm = shmat(shmid, (void*)0, 0);
	if(shm == (void*)-1)
	{
		printf("shmat err.\n");
		return 0;
	}

	stu = (struct stuShareMemory*)shm;

	stu->iSignal = 1;

	//while(true)  //如果需要多次写入，可以启用while
	{
		if(stu->iSignal != 0)
		{
			printf("current txt : %s %d", stu->chBuffer, stu->global_table[0]->next->id);
			stu->iSignal = 0;
		}
		else
		{
			sleep(10);
		}
	}
	
	shmdt(shm);
	shmctl(shmid, IPC_RMID, 0);

	std::cout << "end progress." << endl;
	return 0;
}