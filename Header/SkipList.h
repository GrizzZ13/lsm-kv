//
// Created by 19856 on 2021/4/16.
//

#ifndef LSMTREE_SKIPLIST_H
#define LSMTREE_SKIPLIST_H

#include <stdint.h>
#include <string>
#include <vector>

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

public:
    SkipList();

    ~SkipList();

    void put(uint64_t key, const std::string &s);

    Node*  getNode(uint64_t key) const;

    bool del(uint64_t key);

    void reset();

    std::string returnVal(const Node* ptr) const;

    std::string get(uint64_t key) const;
};


#endif //LSMTREE_SKIPLIST_H
