#ifndef GRAPHM_H
#define GRAPHM_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>

#include <thread>
#include <vector>
#include <map>

#include "core/constants.hpp"
#include "core/type.hpp"
#include "core/bitmap.hpp"
#include "core/atomic.hpp"
#include "core/queue.hpp"
#include "core/partition.hpp"
#include "core/bigvector.hpp"
#include "core/filesystem.hpp"
#include "core/time.hpp"

class GraphM {
    long PAGESIZE;
    long chunk_size;
    int edge_unit;
public:
    std::string path;

    int edge_type;
    VertexId vertices;
    EdgeId edges;
    int partitions;

    GraphM(std::string path, int partitions, int edge_unit, long chunk_size) {
        // init(path, partitions, edge_unit, chunk_size);
    }
    
    void init(std::string path, int partitions, int edge_unit, long chunk_size) {
        printf("this is graphm's init\n");
        int parallelism = std::thread::hardware_concurrency();

        this->partitions = partitions;
        this->edge_unit = edge_unit;
        this->chunk_size = chunk_size;
        char filename[1024];
        // int edge = 0;
        for (int i=0;i<partitions;i++) {
            for (int j=0;j<partitions;j++) {
                sprintf(filename, "%s/block-%d-%d", path.c_str(), i, j);
                printf("%s\n", filename);
                int fin = open(filename, O_RDONLY);
                long filesize = file_size(filename);
                char * buffers = (char *)memalign(PAGESIZE, filesize);
                std::vector<std::map<VertexId, long>> chunk_set;
                // std::vector<std::thread> threads;
                long bytes = read(fin, buffers, filesize);
                long chunk_num = 0;
                int edge_num = 0;
                std::map<VertexId, long> c_table;
                // 对每个分区执行划分chunk操作
                for (long pos=0; pos+edge_unit<=bytes; pos+=edge_unit) {
                    Edge & e = *(Edge*)(buffers+pos);
                    if (c_table.find(e.source) != c_table.end())
                        c_table[e.source] += 1;
                    else
                        c_table[e.source] = 1;
                    edge_num++;
                    if (edge_num*edge_unit >= chunk_size || pos+edge_unit*2>bytes) {
                        chunk_num++;
                        chunk_set.push_back(c_table);
                        // printf("init a chunk%d %d %d %d\n", bytes, edge_num*edge_unit, pos, pos+edge_unit);
                        c_table.erase(c_table.begin(), c_table.end());
                        edge_num = 0;
                    }
                    // edge++;
                    // printf("%d %d %f\n", e.source, e.target, e.weight);
                }
                // partition的chunk表存在chunk_set中, 需要将chunk_set保存在硬盘中
                // chunk_set中的key-value对在硬盘中连续存储
                // 建立offset文件 第一项存储partition的chunk数 后面每一项表示不同chunk的起始位置
                char chunkfilename[4096];
                char chunkfileoffset[4096];
                long offset = 0;
                sprintf(chunkfilename, "%s/block-%d-%d-chunk", path.c_str(), i, j);
                sprintf(chunkfileoffset, "%s/block-%d-%d-offset", path.c_str(), i, j);
                int fout_chunk = open(chunkfilename, O_WRONLY|O_APPEND|O_CREAT, 0644);
	            int fout_chunk_offset = open(chunkfileoffset, O_WRONLY|O_APPEND|O_CREAT, 0644);
                write(fout_chunk_offset, &chunk_num, sizeof(chunk_num));
                for (auto c:chunk_set) {
                    write(fout_chunk_offset, &offset, sizeof(offset));
                    for (auto table:c) {
                        // printf("%d %d\n", table.first, table.second);
                        write(fout_chunk, &table.first, sizeof(table.first));
                        write(fout_chunk, &table.second, sizeof(table.second));
                        offset += sizeof(table.first)+sizeof(table.second);
                    }
                }

                /*
                for (int ti=0;ti<parallelism;ti++) {
                    threads.emplace_back([&]() {

                        //while (true) {
                            
                            long bytes = pread(fin, buffers, edge_unit, offset);
                            offset += edge_unit;
                            assert(bytes!=-1);
                            if (bytes==0) break;
                            Edge & e = *(Edge*)(buffers);
                            edge++;
                            
                            // printf("%d %d %f\n", e.source, e.target, e.weight);
                        //}
                    });
                }
                for (int ti=0;ti<parallelism;ti++) {
		            threads[ti].join();
	            }
                */
                
            }
        }
        // printf("%d\n", edge);
    }

};

#endif