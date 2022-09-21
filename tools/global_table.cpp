#include <iostream>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>
#include <vector>
using namespace std;
#define SHM_KEY_PATH "/home/sym"

long PAGESIZE = 4096;

typedef struct head {
	int firstid;
	int tailid;
	int nodenum;
} Head;

typedef struct node {
	int data;
	int nextid;
} Node;

void add_a_node(Head *head, int dat) {
	// if (head->nodenum >= 0) {
	// 	if (head->nodenum % (PAGESIZE/sizeof(int)-1) == 0) {
	// 		int key = numofp*100+(head->nodenum/(PAGESIZE/sizeof(int)-1))+1;
	// 		cout << key << endl;
	// 		int shmId = shmget((key_t)key, PAGESIZE, IPC_CREAT | 0666);
	// 		if (shmId < 0) {
	// 			cout << "1: shmget failed" << endl;
	// 			return;
	// 		}
	// 		if (head->nodenum == 0) {
	// 			head->firstid = shmId;
	// 			head->tailid = shmId;
	// 		}
	// 		else {
	// 			if (head->tailid < 0) {
	// 				head->tailid = shmId;
	// 				return;
	// 			}
	// 			Node * tail = (Node *)shmat(head->tailid, 0, 0);
	// 			if (tail != NULL) {
	// 				tail->nextid = shmId;
	// 				head->tailid = shmId;
	// 				shmdt((void *)tail);
	// 			}
	// 		}
	// 	}
	// 	int key = numofp*100+(head->nodenum/(PAGESIZE/sizeof(int)-1))+1;
	// 	cout << key << endl;
	// 	int shmId = shmget((key_t)key, 0, 0);
	// 	if (shmId < 0) {
	// 		cout << "2: shmget failed" << endl;
	// 		return;
	// 	}
	// 	Node * node = (Node*)shmat(shmId, 0, 0);
	// 	node->data[head->nodenum % (PAGESIZE/sizeof(int)-1)] = dat;
	// 	node->nextid = -1;
	// 	head->nodenum++;
	// }
	if(head->nodenum >= 0) {
		int shmid = shmget(0, sizeof(Node), IPC_CREAT | 0666);
		if(shmid < 0)
			return;
		Node *node = (Node *)shmat(shmid, 0, 0);
		if(node == NULL)
			return;
		node->nextid = -1;
		node->data = dat;
		if(head->nodenum == 0) {
			head->firstid = shmid;
			head->tailid = shmid;
		}
		else {
			if(head->tailid < 0) {
				head->tailid = shmid;
				shmdt((void *)node);
				return;
			}
			Node *tail = (Node *)shmat(head->tailid, 0, 0);
			if(tail != NULL) {
				tail->nextid = shmid;
				head->tailid = shmid;
				shmdt((void *)tail);
			}
			shmdt((void *)node);
		}
		head->nodenum++;
	}
}

void delete_a_node(Head *head, int dat) {
	if (head->nodenum > 0) {
		int shmId = head->firstid;
		Node * node = (Node*)shmat(shmId, NULL, 0);
		if (node->data == dat) {
			head->firstid = node->nextid;
			head->nodenum--;
			shmdt((void *)node);
			return;
		}
		Node * thisnode = node;
		shmId = node->nextid;
		while (shmId != -1) {
			Node * node = (Node*)shmat(shmId, NULL, 0);
			if (node->data == dat) {
				thisnode->nextid = node->nextid;
				head->nodenum--;
				shmdt((void *)node);
				shmctl(shmId, IPC_RMID, NULL);
				break;
			}
			thisnode = node;
			shmId = node->nextid;
		}
	}
}

void get_all_active_pid(Head* head, int i, int time) {
	int shmId = head->firstid;
	cout << "time:" << time << "\t" << i << ": ";
	while (shmId != -1) {
		Node * node = (Node*)shmat(shmId, NULL, 0);
		cout << node->data << " ";
		shmId = node->nextid;
	}
	cout << endl;
}

void del_shm_list(Head *head)      //删除共享内存链表函数
{
	int shmId = head->firstid;
	int nextId;
	while(shmId != -1) {
		Node * node = (Node*)shmat(shmId, NULL, 0);
		nextId = node->nextid;
		shmdt((void *)node);
		shmctl(shmId, IPC_RMID, NULL);
		shmId = nextId;
		head->nodenum--;
	}
}


int read_pid() {
	return 0;
}

int main(int argc, char ** argv)
{
	if (argc<2) {
		fprintf(stderr, "usage: bfs [path] [start vertex id] [memory budget in GB]\n");
		exit(-1);
	} 
	int partitions = atoi(argv[1]);
	int deleted = atoi(argv[2]);

	key_t key = ftok(SHM_KEY_PATH, 1);
	if (key < 0) {
		cout << "ftok failed" << endl;
	}
	int shmId = shmget(key, partitions*partitions*sizeof(Head), IPC_CREAT | 0666);
	if (shmId < 0) {
		cout << "shmget failed" << endl;
	}
	Head *shm = NULL;
	shm = (Head*)shmat(shmId, (void*)0, 0);
	if(shm == (Head*)-1)
	{
		cout << "shmat err." << endl;
		return 0;
	}

	vector<Head*> global_table(partitions*partitions);

	for (int i = 0; i < partitions*partitions; i++) {
		global_table[i] = shm+i*sizeof(Head);
		global_table[i]->firstid = -1;
		global_table[i]->tailid = -1;
		global_table[i]->nodenum = 0;
		// shmdt(global_table[i]);s
		//add_a_node(global_table[i], 2);
	}

	if (deleted == 1) {
		for (int i = 0; i < partitions*partitions; i++) {
			del_shm_list(global_table[i]);
		}
		shmctl(shmId, IPC_RMID, NULL);
	}

	int time = 0;
	while (true) {
		key = ftok(SHM_KEY_PATH, 1);
		if (key < 0) {
			cout << "ftok failed" << endl;
		}
		shmId = shmget(key, 0, 0);
		if (shmId < 0) 
			cout << "shmget failed" << endl;
		shm = (Head*)shmat(shmId, (void*)0, 0);
		if(shm == (Head*)-1)
		{
			cout << "shmat err." << endl;
			return 0;
		}
		for (int i = 0; i < partitions*partitions; i++) {
			global_table[i] = shm+i*sizeof(Head);
			if (global_table[i]->nodenum == 0) continue;
			
			get_all_active_pid(global_table[i], i, time);
		}
		time++;
		sleep(2);
	
	}
	
	
	return 0;
}