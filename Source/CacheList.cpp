//
// Created by 19856 on 2021/4/22.
//

#include "../Header/CacheList.h"
#include "../Header/utils.h"
#include <fstream>
#include <iostream>

Cache::Cache() {
    pairs = nullptr;
    next = nullptr;
}

Cache::Cache(const std::string &str) {
    path = str;
    next = nullptr;

    std::ifstream ifs;
    ifs.open(path, std::ios::in | std::ios::binary);
    ifs.read((char*)&timestamp, 8);
    ifs.read((char*)&count, 8);
    ifs.read((char*)&minKey, 8);
    ifs.read((char*)&maxKey, 8);

    /* bloom filter */
    bf.readFromFile(ifs);

    /* key and offset */
    pairs = new Pair[count];
    for (int i = 0; i < count; ++i) {
        pairs[i].readFromFile(ifs);
    }

    ifs.close();
}

Cache::Cache(const std::string &str, const SSTable &ssTable) {
    path = str;
    timestamp = ssTable.getTimestamp();
    count = ssTable.getCount();
    minKey = ssTable.getMinKey();
    maxKey = ssTable.getMaxKey();
    ssTable.copyBF(&bf); /* set bloom filter */
    pairs = new Pair[count];
    ssTable.copyPairs(pairs); /* set pairs */
    next = nullptr;
}

Cache::~Cache() {
    if(pairs != nullptr) delete [] pairs;
}

void Cache::get(uint64_t key, uint64_t &ts, std::string &ret) const {
    /* this.timestamp is older */
    if(timestamp < ts) return;

    /* key does not exist */
    if(key > maxKey || key < minKey) return;
    if(!bf.matchKey(key)) return;
    Pair* target = binarySearch(pairs, key, count);
    if(target == nullptr) return;

    /* key exists */
    uint32_t size, beg, end;

    std::ifstream ifs(path, std::ios::in|std::ios::binary);
    if(target == pairs+count-1){ /* the last element */
        ifs.seekg(0, std::ios::end);
        end = ifs.tellg();
    }
    else{
        ifs.seekg((target+1)->offset, std::ios::beg);
        end = ifs.tellg();
    }
    ifs.seekg(target->offset, std::ios::beg);
    beg = ifs.tellg();
    size = end - beg;
    char *str = new char[size+1];
    memset(str, '\0', size+1);
    ifs.read(str, size);
    ifs.close(); /* close file */

    /* update ret */
    ret = str;
    /* update timestamp */
    ts = timestamp;

    delete [] str;
}

Pair *Cache::binarySearch(Pair *pair, uint64_t key, uint64_t size) const {
    int64_t low = 0, high = size, mid;
    /* if uint64_t high, segmentation fault */
    while(low <= high){
        mid = low + (high - low) / 2;
        if(pair[mid].key == key) {
            return &(pair[mid]);
        }
        else if(pair[mid].key > key) {
            high = mid - 1;
        }
        else {
            low = mid + 1;
        }
    }
    return nullptr;
}

CacheList::CacheList() {
    head = new Cache();
}

CacheList::CacheList(const std::string &subpath, uint64_t &max) {
    head = new Cache();
    std::vector<std::string> file_name;
    std::string file_path;
    utils::scanDir(subpath, file_name);
    for(auto itr=file_name.begin();itr!=file_name.end();++itr){
        file_path.clear();
        file_path = subpath + "/" + *itr;
        uint64_t tmp;
        sscanf((*itr).c_str(), "%Lu", &tmp);
        max = (max > tmp) ? max : tmp;
        AddCache(file_path);
    }
}

CacheList::~CacheList() {
    Cache *tmp;
    while(head){
        tmp = head;
        head = head->next;
        delete tmp;
    }
}

void CacheList::AddCache(const std::string &str, const SSTable &ssTable) {
    Cache *tmp = head;
    while(tmp->next) tmp= tmp->next;
    tmp->next = new Cache(str, ssTable);
}

void CacheList::AddCache(const std::string &str) {
    Cache *tmp = head;
    while(tmp->next) tmp= tmp->next;
    tmp->next = new Cache(str);
}

void CacheList::get(uint64_t key, uint64_t &ts, std::string &ret) const {
    Cache *ptr = head->next; /* traversal pointer */
    while(ptr){
        ptr->get(key, ts, ret);
        ptr = ptr->next;
    }
}

void CacheList::delFiles() {
    Cache *tmp = head->next;
    while(tmp){
        utils::rmfile(tmp->path.c_str());
        tmp = tmp->next;
    }
}

uint32_t CacheList::size() const {
    Cache *tmp = head->next;
    uint32_t size = 0;
    while(tmp){
        tmp = tmp->next;
        size++;
    }
    return size;
}

//void CacheList::reset() {
//    Cache *tmp, *del_ptr = head->next;
//    while(del_ptr){
//        tmp = del_ptr;
//        del_ptr = del_ptr->next;
//        delete tmp;
//    }
//    head->next = nullptr;
//}
