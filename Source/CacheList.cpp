//
// Created by 19856 on 2021/4/22.
//

#include "../Header/CacheList.h"
#include "../Header/utils.h"
#include <fstream>

Cache::Cache() {
    pairs = nullptr;
    next = nullptr;
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

void CacheList::get(uint64_t key, uint64_t &ts, std::string &ret) const {
    Cache *ptr = head->next; /* traversal pointer */
    uint64_t ts_tmp = 0;
    while(ptr){
        ptr->get(key, ts, ret);
        ptr = ptr->next;
    }
}

void CacheList::delFiles() {
    Cache *tmp = head->next;
    while(tmp){
        utils::rmfile(tmp->path.c_str());
    }
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
