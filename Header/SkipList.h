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

public:
    SkipList();

    ~SkipList();

    void put(uint64_t key, const std::string &s);

    Node*  getNode(uint64_t key);

    bool del(uint64_t key);

    void reset();

    std::string returnVal(const Node* ptr) const;

    std::string get(uint64_t key);
};


#endif //LSMTREE_SKIPLIST_H
