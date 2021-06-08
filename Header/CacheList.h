//
// Created by 19856 on 2021/4/22.
//

#ifndef LSMTREE_CACHELIST_H
#define LSMTREE_CACHELIST_H

#include <stdint.h>

#include "BloomFilter.h"
#include "Pair.h"
#include "SSTable.h"
#include "CompactionNode.h"

struct Cache {
    /* file information stored in cache except data */

    /* file path and file name*/
    std::string path;

    /* meta data */
    uint64_t timestamp;
    uint64_t count;
    uint64_t minKey;
    uint64_t maxKey;
    BloomFilter bf;
    Pair *pairs;

    /* pointer to next cache */
    Cache *next;

    Cache();

    Cache(const std::string &path, Cache *_next);

    Cache(const std::string &path, const SSTable &ssTable, Cache *_next);

    ~Cache();

    /* update return value and timestamp if key exists and timestamp is newer */
//    void get(uint64_t key, uint64_t &ts, std::string &ret) const;

    bool get(uint64_t key, uint64_t &ts, std::string &ret) const;

    bool get_1(uint64_t key, uint64_t &ts, std::string &ret) const;

    bool get_2(uint64_t key, uint64_t &ts, std::string &ret) const;

    uint64_t getTimeStamp() const;

    Pair* binarySearch(Pair* begin, uint64_t key, uint64_t size) const;

    std::string getPath() const;
};

// judge whether a cache should be compacted
struct Range{
    uint64_t maxKey;
    uint64_t minKey;

    Range(uint64_t max, uint64_t min){
        maxKey = max;
        minKey = min;
    }

    Range(){
        maxKey = 0;
        minKey = 0xffffffffffffffff;
    }

    bool valid(){
        return !(maxKey==0 && minKey==0xffffffffffffffff);
    }
};

Range getRange(const std::vector<Range> &range_vec);

class CacheList {
private:
    Cache *head;
    uint64_t nextFileIndex;
public:
    // initialize an empty CacheList
    CacheList();

    // initialize CacheList from existing directory and update max timestamp
    CacheList(const std::string &subpath, uint64_t &max);

    ~CacheList();

    void AddCache(const std::string &path, const SSTable &ssTable);

    // add cache and return its timestamp
    uint64_t AddCache(const std::string &path);

//    void get(uint64_t key, uint64_t &ts, std::string &ret) const;

    bool get(uint64_t key, uint64_t &ts, std::string &ret) const;

    bool get_1(uint64_t key, uint64_t &ts, std::string &ret) const;

    bool get_2(uint64_t key, uint64_t &ts, std::string &ret) const;

    void delAllFiles();

    uint32_t size() const;

    uint64_t getNextFileIndex() const;

    // for level-0 CacheList, return Range and nodes to be compacted
//    std::vector<Range> getNodes_0(std::vector<std::vector<CompactionNode*>> &nodes);

    Range getNodes_0(std::vector<std::vector<CompactionNode*>> &nodes);

    Range getNodes_k(std::vector<std::vector<CompactionNode*>> &nodes,
                                  const Range &range, uint32_t level);

    // for level-k CacheList, input higher level's compacting cache's range and level index
    // return Range(empty if unnecessary) and nodes to be compacted(empty if unnecessary)
//    std::vector<Range> getNodes_k(std::vector<std::vector<CompactionNode*>> &nodes,
//                                  const std::vector<Range> &range, uint32_t level);

    bool inRange(const std::vector<Range> &range, uint64_t max, uint64_t min);

    void clear();

    // sort [start, end]
    std::vector<CompactionNode*> MergeSort(std::vector<std::vector<CompactionNode*>> &A,
                                           uint64_t start, uint64_t end);

    std::vector<CompactionNode*> MergeTwo(std::vector<CompactionNode*> &A, std::vector<CompactionNode*> &B);

    // split compaction nodes, write to files and add caches to cachelist
    void splitNodes(std::vector<CompactionNode*> &sorted, uint32_t level);

    std::vector<SSTable*> getSSTables(std::vector<CompactionNode*> &sorted);
};

#endif //LSMTREE_CACHELIST_H
