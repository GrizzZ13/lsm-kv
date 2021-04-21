//
// Created by 19856 on 2021/4/21.
//

#ifndef LSMTREE_PAIR_H
#define LSMTREE_PAIR_H

#include <stdint.h>

struct Pair {
    uint64_t key;
    uint32_t offset;

    Pair(uint64_t key, uint32_t offset) : key(key), offset(offset) {}
    Pair(){};

};

#endif //LSMTREE_PAIR_H
