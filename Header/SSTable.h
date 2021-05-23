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
#include "CompactionNode.h"

class SSTable {
private:
    /* header 32 bytes */
    uint64_t timestamp; /* timestamp */
    uint64_t count;  /* number of key-value pair in this SSTable */
    uint64_t minKey; /* minimum key in this SSTable */
    uint64_t maxKey; /* maximum key in this SSTable */

    /* bloom filter: 10 KB / 10240 bytes */
    BloomFilter bf;

    /* key and offset field */
    Pair *pairs;

    /* data field */
//    std::string *data;
    std::string data;
public:
    SSTable(uint64_t ts, const SkipList &skipList);

    // construct a sstable from existing compaction nodes [start, end)
    // timestamp hasn't been set yet, CompactionNode* will be deleted
    SSTable(std::vector<CompactionNode*> &sorted, uint64_t start, uint64_t end);

    void setTimestamp(uint64_t _timestamp);

    ~SSTable();

    void writeToFile(const char *path) const;

    void copyBF(BloomFilter *out) const;

    void copyPairs(Pair *out) const;

    uint64_t getTimestamp() const;

    uint64_t getCount() const;

    uint64_t getMinKey() const;

    uint64_t getMaxKey() const;
};

#endif //LSMTREE_SSTABLE_H
