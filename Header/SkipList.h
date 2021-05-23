//
// Created by 19856 on 2021/4/16.
//

#ifndef LSMTREE_SKIPLIST_H
#define LSMTREE_SKIPLIST_H

#include <stdint.h>
#include <string>
#include <vector>

#include "BloomFilter.h"
#include "Pair.h"

struct Node {
    Node *right, *down;
    uint64_t key;
    std::string val;

    Node(Node *right, Node *down, uint64_t key, std::string val):
    right(right),down(down), key(key), val(val) {};

    Node(): right(nullptr), down(nullptr) {};
};

class SkipList {
private:
    Node *head;

    uint32_t size; /* size of corresponding SSTable, including header, bloom filter and data */
    uint64_t count; /* number of elements */
    uint64_t minKey;
    uint64_t maxKey;

    Node*  getNode(uint64_t key) const;
public:
    SkipList();

    ~SkipList();

    bool put(uint64_t key, const std::string &s);

    bool del(uint64_t key); /* return false if storage overflow (greater than 2MB) */

    void reset();

    std::string returnVal(const Node* ptr) const;

    std::string get(uint64_t key) const;

    void toSSTable(BloomFilter *bf, Pair *pairs, std::string &data) const;

    uint32_t getSize() const;

    uint64_t getCount() const;

    uint64_t getMinKey() const;

    uint64_t getMaxKey() const;
};

#endif //LSMTREE_SKIPLIST_H
