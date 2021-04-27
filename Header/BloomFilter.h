//
// Created by 19856 on 2021/4/21.
//

#ifndef LSMTREE_BLOOMFILTER_H
#define LSMTREE_BLOOMFILTER_H

#include <stdint.h>
#include <string.h>
#include <fstream>

struct BloomFilter{
    uint32_t bitset[2560]; /* bitset with length of 81920 bits */

    BloomFilter();

    bool match32(uint32_t val) const;
    void set32(uint32_t val);

    bool matchKey(uint64_t key) const;
    void setKey(uint64_t key);

    void writeToFile(std::ofstream &ofs) const;
    void readFromFile(std::ifstream &ifs);
    void copyBF(BloomFilter *out) const;
};

#endif //LSMTREE_BLOOMFILTER_H
