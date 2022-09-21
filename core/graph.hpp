/*
Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef GRAPH_H
#define GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>

#include <thread>
#include <vector>

#include "core/constants.hpp"
#include "core/type.hpp"
#include "core/bitmap.hpp"
#include "core/atomic.hpp"
#include "core/queue.hpp"
#include "core/partition.hpp"
#include "core/bigvector.hpp"
#include "core/time.hpp"
#include "core/graphm.hpp"
#include <sys/ipc.h>
#include <sys/shm.h>
#define SHM_KEY_PATH "/home/sym"

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
	if(head->nodenum >= 0) {
		
		int shmid = shmget(0, sizeof(Node), IPC_CREAT | 0666);
		if(shmid < 0) {
			perror("shmget error:");
			printf("shmget error!!!");
			return;
		}
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
		printf("dfsdf %d\n", head->nodenum);
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
				break;
			}
			thisnode = node;
			shmId = node->nextid;
		}
	}
}

void get_all_active_pid(Head* head, std::vector<int>& jobs) {
	int shmId = head->firstid;
	while (shmId != -1) {
		Node * node = (Node*)shmat(shmId, NULL, 0);
		jobs.push_back(node->data);
		printf("%d\n", node->data);
		shmId = node->nextid;
	}

}


bool f_true(VertexId v) {
	return true;
}

void f_none_1(std::pair<VertexId,VertexId> vid_range) {

}

void f_none_2(std::pair<VertexId,VertexId> source_vid_range, std::pair<VertexId,VertexId> target_vid_range) {

}

class Graph {
	int parallelism;
	int edge_unit;
	bool * should_access_shard;
	long ** fsize;
	char ** buffer_pool;
	long * column_offset;
	long * row_offset;
	long memory_bytes;
	int partition_batch;
	long vertex_data_bytes;
	long PAGESIZE;
public:
	std::string path;

	int edge_type;
	VertexId vertices;
	EdgeId edges;
	int partitions;

	Graph (std::string path) {
		PAGESIZE = 4096;
		parallelism = std::thread::hardware_concurrency();
		buffer_pool = new char * [parallelism*1];
		for (int i=0;i<parallelism*1;i++) {
			buffer_pool[i] = (char *)memalign(PAGESIZE, IOSIZE);
			assert(buffer_pool[i]!=NULL);
			memset(buffer_pool[i], 0, IOSIZE);
		}
		init(path);
	}

	void set_memory_bytes(long memory_bytes) {
		this->memory_bytes = memory_bytes;
	}

	void set_vertex_data_bytes(long vertex_data_bytes) {
		this->vertex_data_bytes = vertex_data_bytes;
	}

	void init(std::string path) {
		this->path = path;
		FILE * fin_meta = fopen((path+"/meta").c_str(), "r");
		fscanf(fin_meta, "%d %d %ld %d", &edge_type, &vertices, &edges, &partitions);
		fclose(fin_meta);

		if (edge_type==0) {
			PAGESIZE = 4096;
		} else {
			PAGESIZE = 12288;
		}

		should_access_shard = new bool[partitions];

		if (edge_type==0) {
			edge_unit = sizeof(VertexId) * 2;
		} else {
			edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		}

		memory_bytes = 1024l*1024l*1024l*1024l; // assume RAM capacity is very large
		partition_batch = partitions;
		vertex_data_bytes = 0;

		char filename[1024];
		fsize = new long * [partitions];
		for (int i=0;i<partitions;i++) {
			fsize[i] = new long [partitions];
			for (int j=0;j<partitions;j++) {
				sprintf(filename, "%s/block-%d-%d", path.c_str(), i, j);
				fsize[i][j] = file_size(filename);
			}
		}

		long bytes;

		GraphM graphm(path, partitions, edge_unit, 16l*1024l*1024l);

		// 获得job的进程id，用于构建global table
		pid_t process_id = getpid();
		printf("process id: %d\n", process_id);

		column_offset = new long [partitions*partitions+1];
		int fin_column_offset = open((path+"/column_offset").c_str(), O_RDONLY);
		bytes = read(fin_column_offset, column_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_column_offset);

		row_offset = new long [partitions*partitions+1];
		int fin_row_offset = open((path+"/row_offset").c_str(), O_RDONLY);
		bytes = read(fin_row_offset, row_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_row_offset);
	}

	Bitmap * alloc_bitmap() {
		return new Bitmap(vertices);
	}

	template <typename T>
	T stream_vertices(std::function<T(VertexId)> process, Bitmap * bitmap = nullptr, T zero = 0,
		std::function<void(std::pair<VertexId,VertexId>)> pre = f_none_1,
		std::function<void(std::pair<VertexId,VertexId>)> post = f_none_1) {
		T value = zero;
		if (bitmap==nullptr && vertex_data_bytes > (0.8 * memory_bytes)) {
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre(std::make_pair(begin_vid, end_vid));
				#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
				for (int partition_id=cur_partition;partition_id<cur_partition+partition_batch;partition_id++) {
					if (partition_id < partitions) {
						T local_value = zero;
						VertexId begin_vid, end_vid;
						std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
						for (VertexId i=begin_vid;i<end_vid;i++) {
							local_value += process(i);
						}
						write_add(&value, local_value);
					}
				}
				#pragma omp barrier
				post(std::make_pair(begin_vid, end_vid));
			}
		} else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				T local_value = zero;
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				if (bitmap==nullptr) {
					for (VertexId i=begin_vid;i<end_vid;i++) {
						local_value += process(i);
					}
				} else {
					VertexId i = begin_vid;
					while (i<end_vid) {
						unsigned long word = bitmap->data[WORD_OFFSET(i)];
						if (word==0) {
							i = (WORD_OFFSET(i) + 1) << 6;
							continue;
						}
						size_t j = BIT_OFFSET(i);
						word = word >> j;
						while (word!=0) {
							if (word & 1) {
								local_value += process(i);
							}
							i++;
							j++;
							word = word >> 1;
							if (i==end_vid) break;
						}
						i += (64 - j);
					}
				}
				write_add(&value, local_value);
			}
			#pragma omp barrier
		}
		return value;
	}

	void set_partition_batch(long bytes) {
		int x = (int)ceil(bytes / (0.8 * memory_bytes));
		partition_batch = partitions / x;
	}

	template <typename... Args>
	void hint(Args... args);

	template <typename A>
	void hint(BigVector<A> & a) {
		long bytes = sizeof(A) * a.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B>
	void hint(BigVector<A> & a, BigVector<B> & b) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B, typename C>
	void hint(BigVector<A> & a, BigVector<B> & b, BigVector<C> & c) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length + sizeof(C) * c.length;
		set_partition_batch(bytes);
	}

	template <typename T>
	T stream_edges(std::function<T(Edge&)> process, Bitmap * bitmap = nullptr, T zero = 0, int update_mode = 1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1) {
		if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;
			}
		} else {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = false;
			}
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0) {
						should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}

		T value = zero;
		Queue<std::tuple<int, long, long> > tasks(65536);
		std::vector<std::thread> threads;
		long read_bytes = 0;

		long total_bytes = 0;
		for (int i=0;i<partitions;i++) {
			if (!should_access_shard[i]) continue;
			for (int j=0;j<partitions;j++) {
				total_bytes += fsize[i][j];
			}
		}
		int read_mode;
		if (memory_bytes < total_bytes) {
			read_mode = O_RDONLY | O_DIRECT;
			// printf("use direct I/O\n");
		} else {
			read_mode = O_RDONLY;
			// printf("use buffered I/O\n");
		}

		int fin;
		long offset = 0;

		std::vector<Head*> global_table(partitions*partitions);

		key_t key = ftok(SHM_KEY_PATH, 1);
		if (key < 0) {
			printf("ftok failed\n");
		}
		int shmId = shmget(key, 0, 0);
		if (shmId < 0) {
			printf("shmget failed\n");
		}
		//vector<int> a ();
		//vector<Head*> global_table;

		Head *shm = NULL;
			
		switch(update_mode) {
		case 0: // source oriented update
			threads.clear();
			for (int ti=0;ti<parallelism;ti++) {
				threads.emplace_back([&](int thread_id){
					T local_value = zero;
					long local_read_bytes = 0;
					while (true) {
						int fin;
						long offset, length;
						std::tie(fin, offset, length) = tasks.pop();
						if (fin==-1) break;
						char * buffer = buffer_pool[thread_id];
						long bytes = pread(fin, buffer, length, offset);
						assert(bytes>0);
						local_read_bytes += bytes;
						// CHECK: start position should be offset % edge_unit
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge & e = *(Edge*)(buffer+pos);
							if (bitmap==nullptr || bitmap->get_bit(e.source)) {
								local_value += process(e);
							}
						}
					}
					write_add(&value, local_value);
					write_add(&read_bytes, local_read_bytes);
				}, ti);
			}
			fin = open((path+"/row").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);
			for (int i=0;i<partitions;i++) {
				// 通过源顶点判断
				if (!should_access_shard[i]) continue;
				for (int j=0;j<partitions;j++) {
					long begin_offset = row_offset[i*partitions+j];
					if (begin_offset - offset >= PAGESIZE) {
						offset = begin_offset / PAGESIZE * PAGESIZE;
					}
					long end_offset = row_offset[i*partitions+j+1];
					if (end_offset <= offset) continue;
					while (end_offset - offset >= IOSIZE) {
						tasks.push(std::make_tuple(fin, offset, IOSIZE));
						offset += IOSIZE;
					}
					if (end_offset > offset) {
						tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE));
						offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					}
				}
			}
			for (int i=0;i<parallelism;i++) {
				tasks.push(std::make_tuple(-1, 0, 0));
			}
			// 执行多线程操作，处理edge
			for (int i=0;i<parallelism;i++) {
				threads[i].join();
			}
			break;
		// 按列访问, 默认的访问方式, 可以更好的利用局部性
		case 1: // target oriented update
			// 设置文件描述符fin和预加载方式

			fin = open((path+"/column").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);

			for (int j = 0; j < partitions; j++) {
				for (int i = 0; i < partitions; i++) {
					if (!should_access_shard[i]) continue;
					// printf("activate partition: %d \n", i*partitions+j);
				}
			}

			
			for (int i = 0; i < partitions*partitions; i++) {
				global_table[i] = shm+i*sizeof(Head);
				//printf("%d\n", global_table[i]->firstid);
			}


			// 更新全局表中的活跃分区
			for (int i=0; i<partitions; i++) {
				for (int j=0; j<partitions; j++) {
					if (!should_access_shard[i]) continue;
					shm = (Head*)shmat(shmId, (void*)0, 0);
					if(shm == (Head*)-1)
					{
						printf("shmat err.\n");
					}
					Head * head = shm+(i*partitions+j)*sizeof(Head);
					add_a_node(head, getpid());
					printf("%d\n", head->firstid);
					shmdt(head);
				}
			}

			for (int i=0; i<partitions; i++) {
				for (int j=0; j<partitions; j++) {
					if (!should_access_shard[i]) continue;
					shmId = shmget(key, 0, 0);
					if (shmId < 0) {
						printf("shmget failed\n");
					}
					shm = (Head*)shmat(shmId, (void*)0, 0);
					if(shm == (Head*)-1)
					{
						printf("shmat err.\n");
					}
					// get jobs from globaltable
					int partitionid = i*partitions+j;
					Head * head = shm+partitionid*sizeof(Head);
					// printf("%d\n", head->nodenum);
				}
			}

			// for (int i=0; i<partitions; i++) {
			// 	for (int j=0; j<partitions; j++) {
			// 		if (!should_access_shard[i]) continue;
			// 		shmId = shmget(key, 0, 0);
			// 		if (shmId < 0) {
			// 			printf("shmget failed\n");
			// 		}
			// 		shm = (Head*)shmat(shmId, (void*)0, 0);
			// 		if(shm == (Head*)-1)
			// 		{
			// 			printf("shmat err.\n");
			// 		}
			// 		// get jobs from globaltable
			// 		int partitionid = i*partitions+j;
			// 		global_table[partitionid] = shm+partitionid*sizeof(Head);
			// 		std::vector<int> activate_jobs;
			// 		get_all_active_pid(global_table[partitionid], activate_jobs);
			// 		printf("%d\n", activate_jobs.size());
			// 	}
			// }

			// 每次迭代访问一列数据
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				// 获取partition的起始和终止id
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre_source_window(std::make_pair(begin_vid, end_vid));
				// printf("pre %d %d\n", begin_vid, end_vid);
				threads.clear();
				// 每个线程执行process
				// GraphM需要修改的部分
				for (int ti=0;ti<parallelism;ti++) {
					threads.emplace_back([&](int thread_id){
						T local_value = zero;
						long local_read_bytes = 0;
						while (true) {
							// for each activate partition
							int fin;
							long offset, length;
							std::tie(fin, offset, length) = tasks.pop();
							if (fin==-1) break;
							//printf("%d: out offset %d\n", cur_partition, offset);
							char * buffer = buffer_pool[thread_id];
							// partition <-- load()
							long bytes = pread(fin, buffer, length, offset);
							assert(bytes>0);
							local_read_bytes += bytes;
							// CHECK: start position should be offset % edge_unit
							// for each edge in partition
							for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
								Edge & e = *(Edge*)(buffer+pos);
								if (e.source < begin_vid || e.source >= end_vid) {
									continue;
								}
								// process the streamed edges
								if (bitmap==nullptr || bitmap->get_bit(e.source)) {
									local_value += process(e);
								}
							}
						}
						write_add(&value, local_value);
						write_add(&read_bytes, local_read_bytes);
					}, ti);
				}
				offset = 0;
				// 按照partition batch访问（需要找当前访问partition编号）
				for (int j=0;j<partitions;j++) {
					for (int i=cur_partition;i<cur_partition+partition_batch;i++) {
						if (i>=partitions) break;
						// 通过源顶点判断边分区是否活跃
						if (!should_access_shard[i]) continue;
						// printf("activate partition: %d \n", i*partitions+j);
						// 找到所有的活跃分区
						//add_a_node(global_table[i*partitions+j], getpid());
						long begin_offset = column_offset[j*partitions+i];
						if (begin_offset - offset >= PAGESIZE) {
							offset = begin_offset / PAGESIZE * PAGESIZE;
						}
						long end_offset = column_offset[j*partitions+i+1];
						if (end_offset <= offset) continue;
						//printf("%d: in offset %d\n", i, offset);
						while (end_offset - offset >= IOSIZE) {
							tasks.push(std::make_tuple(fin, offset, IOSIZE));
							offset += IOSIZE;
						}
						if (end_offset > offset) {
							tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE));
							offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
						}
					}
				}
				for (int i=0;i<parallelism;i++) {
					tasks.push(std::make_tuple(-1, 0, 0));
				}
				for (int i=0;i<parallelism;i++) {
					threads[i].join();
				}
				post_source_window(std::make_pair(begin_vid, end_vid));
				// printf("post %d %d\n", begin_vid, end_vid);
			}

			break;
		default:
			assert(false);
		}

		close(fin);
		// printf("streamed %ld bytes of edges\n", read_bytes);
		return value;
	}
};

#endif
