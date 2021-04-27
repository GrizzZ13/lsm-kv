//
// Created by 19856 on 2021/4/16.
//

#include "../Header/SkipList.h"
#include "../Header/MurmurHash3.h"

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

bool SkipList::put(uint64_t key, const std::string &s) {
    // key exists
    uint32_t oldlen;
    uint32_t newlen = s.length();
    uint32_t tmpVal;
    Node *tmp = getNode(key);
    if(tmp){
        oldlen = tmp->val.length();
        tmpVal = size + newlen - oldlen;
        if(tmpVal > 2097000){
            while(tmp){
                tmp->val = "~DELETED~";
                tmp = tmp->down;
            }
            size = size - oldlen + 9;
            return false;
        }
        else{
            while(tmp){
                tmp->val = s;
                tmp = tmp->down;
            }
            size = tmpVal;
            return true;
        }
        /* count  stays the same */
        /* minKey stays the same */
        /* maxKey stays the same */
    }

    // key does not exist
    tmpVal = size + newlen + 12;
    if(tmpVal > 2097000) { /* should be 2,097,000 */
        return false;
    }

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
    if(grow){
        Node *oldHead = head;
        head = new Node();
        head->right = new Node(NULL, downNode, key, s);
        head->down = oldHead;
    }
    size = size + newlen + 12; /* data: newlen / key and offset: 12 */
    count++;
    minKey = (key < minKey) ? key : minKey;
    maxKey = (key > maxKey) ? key : maxKey;
    return true;
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
    if(ptr == nullptr) {
        if(put(key, "~DELETED~"))
            return true;
        else
            return false;
    }
    if(ptr->val == "~DELETED~") return true; // already deleted

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
    return (ptr==nullptr) ? "" : ptr->val;
}

std::string SkipList::get(uint64_t key) const {
    return returnVal(getNode(key));
}

uint32_t SkipList::getSize() const {
    return size;
}

uint64_t SkipList::getCount() const {
    return count;
}

uint64_t SkipList::getMinKey() const {
    return minKey;
}

uint64_t SkipList::getMaxKey() const {
    return maxKey;
}

void SkipList::toSSTable(BloomFilter *bf, Pair *pairs, std::string *data) const {
    uint32_t index = 0;
    uint32_t offset = 10272 + 12*count;
    uint64_t key;
    Node *tmp = head;
    while(tmp->down) tmp = tmp->down;
    tmp = tmp->right;

    while(tmp){
        key = tmp->key;
        pairs[index].key = key;
        pairs[index].offset = offset;
        data[index] = tmp->val;
        bf->setKey(key);

        offset += tmp->val.length();
        tmp = tmp->right;
        index++;
    }
}
