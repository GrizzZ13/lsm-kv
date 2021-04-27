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

    Cache(const std::string &path);

    Cache(const std::string &path, const SSTable &ssTable);

    ~Cache();

    /* update return value and timestamp if key exists and timestamp is newer */
    void get(uint64_t key, uint64_t &ts, std::string &ret) const;

    Pair* binarySearch(Pair* begin, uint64_t key, uint64_t size) const;
};

class CacheList {
private:
    Cache *head;
public:
    CacheList();

    CacheList(const std::string &subpath, uint64_t &max);

    ~CacheList();

    void AddCache(const std::string &path, const SSTable &ssTable);

    void AddCache(const std::string &path);

    void get(uint64_t key, uint64_t &ts, std::string &ret) const;

    void delFiles();

    uint32_t size() const;
};

#endif //LSMTREE_CACHELIST_H
