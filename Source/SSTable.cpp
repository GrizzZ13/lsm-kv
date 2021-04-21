//
// Created by 19856 on 2021/4/20.
//

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
