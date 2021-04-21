//
// Created by 19856 on 2021/4/21.
//

#ifndef LSMTREE_BLOOMFILTER_H
#define LSMTREE_BLOOMFILTER_H

#include <stdint.h>
#include <string.h>

struct BloomFilter{
    uint32_t bitset[2560]; /* bitset with length of 81920 bits */

    BloomFilter();

    bool match128(const uint32_t* hashVal) const; /* hashVal is an array with two uint64_t element */
    void set128(const uint32_t* hashVal);

    bool match32(uint32_t val) const;
    void set32(uint32_t val);
};

#endif //LSMTREE_BLOOMFILTER_H
