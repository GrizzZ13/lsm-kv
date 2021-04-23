//
// Created by 19856 on 2021/4/21.
//

#include "../Header/BloomFilter.h"
#include "../Header/MurmurHash3.h"

BloomFilter::BloomFilter() {
    memset(bitset, 0, sizeof bitset);
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

void BloomFilter::writeToFile(std::ofstream &ofs) const {
    ofs.write((const char*)bitset, (size_t)10240);
}

void BloomFilter::copyBF(BloomFilter *out) const {
    for (int i = 0; i < 2560; ++i) {
        out->bitset[i] = this->bitset[i];
    }
}

bool BloomFilter::matchKey(uint64_t key) const {
    uint64_t tmp = key;
    uint32_t hashVal[4] = {0};
    bool flag = true;
    MurmurHash3_x64_128(&tmp, sizeof(tmp), 1, hashVal);
    for (int i = 0; i < 4; ++i) {
        flag = flag && match32(hashVal[i]);
    }
    return flag;
}

void BloomFilter::setKey(uint64_t key) {
    uint64_t tmp = key;
    uint32_t hashVal[4] = {0};
    MurmurHash3_x64_128(&tmp, sizeof(tmp), 1, hashVal);
    for (int i = 0; i < 4; ++i) {
        set32(hashVal[i]);
    }
}
