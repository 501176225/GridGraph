#include <iostream>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <vector>
#include <stdint.h>
#include <string.h>
using namespace std;
#define SHM_KEY_PATH "/home/sunym"

const char* path = "/usr/IPC";

typedef struct head {
	int firstid;
	int tailid;
	int nodenum;
} Head;

typedef struct node {
	int nextid;
	int data;
} Node;

void add_a_node(Head *head, int dat) {
	//char* subpath = "/123/";
	//const char* result = string(string(path)+string(subpath)).c_str();
	//cout << result << endl;
	if (head->nodenum >= 0) {
		// key_t key = ftok(result, head->nodenum);
		// if (key < 0) {
		// 	cout << "ftok failed" << endl;
		// 	return;
		// }
		int key = 1234;
		int shmId = shmget((key_t)key, sizeof(Node), IPC_CREAT | 0666);
		if (shmId < 0) {
			cout << "shmget failed" << endl;
			return;
		}
		cout << "shmId=" << shmId << endl;
		Node * node = (Node*)shmat(shmId, 0, 0);
		if (head->nodenum == 0) {
			head->firstid = shmId;
			head->tailid = shmId;
		}
		else {
			if (head->tailid < 0) {
				head->tailid = shmId;
				shmdt((void*)node);
				return;
			}
			Node * tail = (Node*)shmat(head->tailid, 0, 0);
			if (tail != NULL) {
				tail->nextid = shmId;
				head->tailid - shmId;
				shmdt((void*)tail);
			}
			shmdt((void*)node);
		}
		head->nodenum++;
	}
}

int main()
{
	/*
	int data[5] = {1,2,3,4,5};
	int i = 0;
	key_t key;
	int shmId;
	
	key = ftok(path, 1); //为共享内存生成键值
	if(key == -1)
	{
		cout << "ftok failed" << endl;
	}
	
	shmId = shmget(key, 100, IPC_CREAT|IPC_EXCL | 0600); //获取共享内存标志符
	cout <<"shmId=" << shmId << endl;
	if(shmId == -1)
	{
		cout << "shmget failed" << endl;
	}
	
	int* shmaddr = (int*)shmat(shmId, NULL, 0); //获取共享内存地址
	for(i = 0; i < 5; i++)
	{
	//往共享内存写数据
		*shmaddr = data[i];
		shmaddr++;
	}
	cout << "write end" << endl;
	*/
	key_t key = ftok(SHM_KEY_PATH, 1);
	if (key < 0) {
		cout << "ftok failed" << endl;
		//return;
	}

	// 如果已经存在，删除
	// int m_nShmId = shmget(key, 12, 0);
    // if (m_nShmId != -1)
    // {
    //     shmctl(m_nShmId, IPC_RMID, 0);
    // }


	int shmId = shmget(key, 10*sizeof(Head), IPC_CREAT | 0666);
	if (shmId < 0) {
		cout << "shmget failed" << endl;
		//return;
	}
	cout << "shmId=" << shmId << endl;
	vector<Head* > global_table(10);
	
	Head *shm = NULL;
	shm = (Head*)shmat(shmId, (void*)0, 0);
	if(shm == (Head*)-1)
	{
		cout << "shmat err." << endl;
		return 0;
	}
	for (int i = 0; i < 10; i++) {
		global_table[i] = shm+i*sizeof(Head);
		global_table[i]->firstid = -1*i;
	}

	// Head * head = (Head*)shm;
	// head->firstid = -1;
    // head->tailid = -1;
    // head->nodenum = 0;
	// vector<Head> global_table(10);
	// global_table[0].firstid = -1;
	// global_table[0].tailid = -1;
	// global_table[0].nodenum = 0;
	for (int i = 0; i < 10; i++) {
		cout << global_table[i]->firstid << endl;
		add_a_node(global_table[i], i);
	}
	// shmdt(head);
	return 0;
}

