//
// Created by 19856 on 2021/4/20.
//

#include <fstream>

#include "../Header/SSTable.h"
#include "../Header/MurmurHash3.h"

SSTable::SSTable(uint64_t ts, const SkipList &skipList) {
    timestamp = ts;
    count = skipList.getCount();
    minKey = skipList.getMinKey();
    maxKey = skipList.getMaxKey();

    pairs = new Pair[count];

    skipList.toSSTable(&bf, pairs, data);
}

SSTable::~SSTable() {
    delete [] pairs;
}

void SSTable::writeToFile(const char *path) const{
    std::ofstream ofs;
    ofs.open(path, std::ios::out | std::ios::binary);  /* open in write mode */

    /* header */
    ofs.write((const char*)&timestamp,8);
    ofs.write((const char*)&count,8);
    ofs.write((const char*)&minKey,8);
    ofs.write((const char*)&maxKey,8);

    /* bloom filter */
    bf.writeToFile(ofs);

    /* key and offset */
    for (int i = 0; i < count; ++i) {
        pairs[i].writeToFile(ofs);
    }

    /* data */
    ofs.write(data.c_str(), data.length());

    ofs.close();
}

void SSTable::copyBF(BloomFilter *out) const {
    bf.copyBF(out);
}

void SSTable::copyPairs(Pair *out) const {
    for (int i = 0; i < count; ++i) {
        out[i].offset = pairs[i].offset;
        out[i].key = pairs[i].key;
    }
}

uint64_t SSTable::getTimestamp() const {
    return timestamp;
}

uint64_t SSTable::getCount() const {
    return count;
}

uint64_t SSTable::getMinKey() const {
    return minKey;
}

uint64_t SSTable::getMaxKey() const {
    return maxKey;
}

SSTable::SSTable(std::vector<CompactionNode*> &sorted, uint64_t start, uint64_t end) {
    // timestamp hasn't been set yet
    count = end-start;
    pairs = new Pair[count];
    minKey = 0xffffffffffffffff;
    maxKey = 0;
    uint32_t accumulation = 0;
    uint32_t overhead = 10272 + 12 * count;
    for(uint64_t i = start; i < end; ++i) {
        CompactionNode *tmp = sorted[i];
        minKey = (minKey < tmp->key) ? minKey : tmp->key;
        maxKey = (maxKey > tmp->key) ? maxKey : tmp->key;
        bf.setKey(tmp->key);
        pairs[i-start].key = tmp->key;
        pairs[i-start].offset = overhead + accumulation;
        accumulation += tmp->value.length();
        data += tmp->value;
        delete sorted[i];
        sorted[i] = nullptr;
    }
}

void SSTable::setTimestamp(uint64_t _timestamp) {
    timestamp = _timestamp;
}

