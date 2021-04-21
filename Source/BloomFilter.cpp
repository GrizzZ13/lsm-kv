//
// Created by 19856 on 2021/4/21.
//

#include "../Header/BloomFilter.h"


BloomFilter::BloomFilter() {
    memset(bitset, 0, sizeof bitset);
}

bool BloomFilter::match128(const uint32_t* hashVal_128) const {
    bool flag = true;
    for (int i = 0; i < 4; ++i) {
        flag = flag && match32(hashVal_128[i]);
    }
    return flag;
}

void BloomFilter::set128(const uint32_t* hashVal_128) {
    for (int i = 0; i < 4; ++i) {
        set32(hashVal_128[i]);
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