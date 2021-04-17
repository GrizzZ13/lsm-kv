//
// Created by 19856 on 2021/4/16.
//

#include "../Header/SkipList.h"

SkipList::SkipList() {
    head = new Node();
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
    Node *tmp = getNode(key);
    if(tmp){
        while(tmp){
            tmp->val = s;
            tmp = tmp->down;
        }
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
}

Node* SkipList::getNode(uint64_t key) {
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

    while(ptr){
        ptr->val = "~DELETED~";
        ptr = ptr->down;
    }
    return true;
}

void SkipList::reset() {
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

std::string SkipList::get(uint64_t key) {
    return returnVal(getNode(key));
}
