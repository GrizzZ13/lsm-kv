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
    data = new std::string[count];

    skipList.toSSTable(&bf, pairs, data);
}

SSTable::~SSTable() {
    delete [] pairs;
    delete [] data;
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
    for (int i = 0; i < count; ++i) {
        ofs.write(data[i].c_str(), data[i].length());
    }

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

