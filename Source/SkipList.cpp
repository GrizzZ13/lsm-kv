//
// Created by 19856 on 2021/4/16.
//

#include "../Header/SkipList.h"

SkipList::SkipList() {
    head = new Node();

    size = 10272; /* header: 32 bytes, bloom filter: 10240 bytes */
    count = 0;
    minKey = 0xFFFFFFFFFFFFFFFF;
    maxKey = 0;
}

SkipList::~SkipList() {
    Node *ptrDown, *ptrRight;
    while(head){
        ptrDown = head->down;
        while(head){
            ptrRight = head->right;
            delete head;
            head = ptrRight;
        }
        head = ptrDown;
    }
}

void SkipList::put(uint64_t key, const std::string &s) {
    // already exist
    uint32_t oldlen;
    uint32_t newlen = s.length();
    Node *tmp = getNode(key);
    if(tmp){
        oldlen = tmp->val.length();
        while(tmp){
            tmp->val = s;
            tmp = tmp->down;
        }

        size = size + newlen - oldlen;
        /* count  stays the same */
        /* minKey stays the same */
        /* maxKey stays the same */
        return;
    }

    // does not exist
    std::vector<Node*> pathList;
    Node *ptr = head;
    while(ptr){
        while(ptr->right && ptr->right->key < key)
            ptr = ptr->right;
        pathList.push_back(ptr);
        ptr = ptr->down;
    }

    bool grow = true;
    Node *downNode = nullptr;
    while(grow && !pathList.empty()) {
        Node *insert = pathList.back();
        pathList.pop_back();
        insert->right = new Node(insert->right, downNode, key, s);
        downNode = insert->right;
        grow = (rand() & 1);
    }
    size = size + newlen + 12; /* data: newlen / key and offset: 12 */
    count++;
    minKey = (key < minKey) ? key : minKey;
    maxKey = (key > maxKey) ? key : minKey;
}

Node* SkipList::getNode(uint64_t key) const {
    Node *ptr = head;
    while(ptr){
        while(ptr->right && ptr->right->key < key) ptr = ptr->right;
        if(ptr->right && ptr->right->key == key) return ptr->right;
        ptr = ptr->down;
    }
    return nullptr;
}

bool SkipList::del(uint64_t key) {
    Node *ptr = getNode(key);
    if(ptr == nullptr) return false; // does not exist
    if(ptr->val == "~DELETED~") return false; // already deleted

    uint32_t oldlen = ptr->val.length();
    while(ptr){
        ptr->val = "~DELETED~";
        ptr = ptr->down;
    }
    size = size - oldlen + 9; /* length of "~DELETED~" is 9 */
    /* count  stays the same */
    /* minKey stays the same */
    /* maxKey stays the same */

    return true;
}

void SkipList::reset() {
    size = 10272;
    count = 0;
    minKey = 0xFFFFFFFFFFFFFFFF;
    maxKey = 0;

    Node *ptrDown, *ptrRight;
    while(head){
        ptrDown = head->down;
        while(head){
            ptrRight = head->right;
            delete head;
            head = ptrRight;
        }
        head = ptrDown;
    }

    Node *newHead = new Node();
    head = newHead;
}

std::string SkipList::returnVal(const Node *ptr) const {
    return (ptr && ptr->val != "~DELETED~") ? ptr->val : "";
}

std::string SkipList::get(uint64_t key) const {
    return returnVal(getNode(key));
}
