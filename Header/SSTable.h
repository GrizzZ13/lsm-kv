//
// Created by 19856 on 2021/4/20.
//

#ifndef LSMTREE_SSTABLE_H
#define LSMTREE_SSTABLE_H

#include <stdint.h>
#include <string.h>
#include <string>

#include "BloomFilter.h"
#include "Pair.h"
#include "SkipList.h"

class SSTable {
private:
    /* header 32 bytes */
    uint64_t timestamp;
    uint64_t count;  /* number of key-value pair in this SSTable */
    uint64_t minKey; /* minimum key in this SSTable */
    uint64_t maxKey; /* maximum key in this SSTable */

    /* bloom filter: 10Kb / 10240 bytes */
    BloomFilter bf;

    /* key and offset field */
    Pair *pairs;

    /* data field */
    std::string *data;

public:
    SSTable(uint64_t ts, const SkipList &skipList);
    ~SSTable();
};

#endif //LSMTREE_SSTABLE_H
