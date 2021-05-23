//
// Created by 19856 on 2021/5/22.
//

#ifndef LSMTREE_COMPACTIONNODE_H
#define LSMTREE_COMPACTIONNODE_H

#include <stdint.h>
#include <string>

struct CompactionNode{
    uint64_t timestamp;
    uint64_t key;
    std::string value;

    CompactionNode(uint64_t _timestamp, uint64_t _key, const std::string &_value){
        timestamp = _timestamp;
        key = _key;
        value = _value;
    }
};

#endif //LSMTREE_COMPACTIONNODE_H
