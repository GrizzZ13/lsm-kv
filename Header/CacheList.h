//
// Created by 19856 on 2021/4/22.
//

#ifndef LSMTREE_CACHELIST_H
#define LSMTREE_CACHELIST_H

#include <stdint.h>

#include "BloomFilter.h"
#include "Pair.h"
#include "SSTable.h"

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
    void get(uint64_t key, uint64_t &ts, std::string &ret) const;

    uint64_t getTimeStamp() const;

    Pair* binarySearch(Pair* begin, uint64_t key, uint64_t size) const;
};

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

    void get(uint64_t key, uint64_t &ts, std::string &ret) const;

    void delFiles();

    uint32_t size() const;

    uint64_t getNextFileIndex() const;
};

#endif //LSMTREE_CACHELIST_H
