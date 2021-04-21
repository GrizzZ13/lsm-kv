//
// Created by 19856 on 2021/4/20.
//

#ifndef LSMTREE_SSTABLE_H
#define LSMTREE_SSTABLE_H

#include <stdint.h>
#include <string.h>

#include "MurmurHash3.h"

struct BloomFilter{
    uint32_t bitset[2560]; /* bitset with length of 81920 bits */

    BloomFilter();

    bool match128(const uint64_t* hashVal) const; /* hashVal is an array with two uint64_t element */
    void set128(const uint64_t* hashVal);

    bool match32(uint32_t val) const;
    void set32(uint32_t val);
};

BloomFilter::BloomFilter() {
    memset(bitset, 0, sizeof bitset);
}

bool BloomFilter::match128(const uint64_t* hashVal_128) const {
    uint32_t *arr = (uint32_t*)hashVal_128;
    bool flag = true;
    for (int i = 0; i < 4; ++i) {
        flag = flag && match32(arr[i]);
    }
    return flag;
}

void BloomFilter::set128(const uint64_t* hashVal_128) {
    uint32_t *arr = (uint32_t*)hashVal_128;
    for (int i = 0; i < 4; ++i) {
        set32(arr[i]);
    }
}

bool BloomFilter::match32(uint32_t val) const {
    uint32_t index_bit = val % 81920;
    uint32_t index_32 = index_bit / 32;
    uint32_t offset = index_bit % 32;
    uint32_t tmp = bitset[index_32];
    uint32_t mask = ((uint32_t)0x1) << offset;

    return (mask == (tmp & mask));
}

void BloomFilter::set32(uint32_t val) {
    uint32_t index_bit = val % 81920;
    uint32_t index_32 = index_bit / 32;
    uint32_t offset = index_bit % 32;
    uint32_t tmp = bitset[index_32];
    tmp = (((uint32_t)0x1) << offset) | tmp;
    bitset[index_32] = tmp;
}

class SSTable {
private:
    /* header 32 bytes */
    uint64_t timestamp;
    uint64_t count;  /* number of key-value pair in this SSTable */
    uint64_t minKey; /* minimum key in this SSTable */
    uint64_t maxKey; /* maximum key in this SSTable */

    /* bloom filter: 1oKb / 10240 bytes */
    BloomFilter bf;
};


#endif //LSMTREE_SSTABLE_H
