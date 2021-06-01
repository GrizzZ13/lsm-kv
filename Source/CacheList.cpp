//
// Created by 19856 on 2021/4/22.
//

#include "../Header/CacheList.h"
#include "../Header/utils.h"
#include "../Header/SSTable.h"
#include <fstream>
#include <iostream>

Cache::Cache() {
    pairs = nullptr;
    next = nullptr;
}

Cache::Cache(const std::string &str, Cache *_next) {
    path = str;
    next = _next;

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

Cache::Cache(const std::string &str, const SSTable &ssTable, Cache *_next) {
    path = str;
    timestamp = ssTable.getTimestamp();
    count = ssTable.getCount();
    minKey = ssTable.getMinKey();
    maxKey = ssTable.getMaxKey();
    ssTable.copyBF(&bf); /* set bloom filter */
    pairs = new Pair[count];
    ssTable.copyPairs(pairs); /* set pairs */
    next = _next;
}

Cache::~Cache() {
    if(pairs != nullptr) delete [] pairs;
}

void Cache::get(uint64_t key, uint64_t &ts, std::string &ret) const {
    /* this.timestamp is older */
    if(timestamp <= ts) return;

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

bool Cache::get_2(uint64_t key, uint64_t &ts, std::string &ret) const {
    /* this.timestamp is older */
    if(timestamp <= ts) return false;

    /* key does not exist */
    if(key > maxKey || key < minKey) return false;
    if(!bf.matchKey(key)) return false;
    Pair* target = binarySearch(pairs, key, count);
    if(target == nullptr) return false;

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
    return true;
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

uint64_t Cache::getTimeStamp() const {
    return timestamp;
}

std::string Cache::getPath() const {
    return path;
}

CacheList::CacheList() {
    head = new Cache();
    nextFileIndex = 1;
}

CacheList::CacheList(const std::string &subpath, uint64_t &max) {
    head = new Cache();
    nextFileIndex = 0;
    uint64_t maxTimeStamp = 0;
    std::vector<std::string> file_name;
    std::string file_path;
    utils::scanDir(subpath, file_name);
    for(auto itr=file_name.begin();itr!=file_name.end();++itr){
        file_path.clear();
        file_path = subpath + "/" + *itr;
        uint64_t tmp;
        sscanf((*itr).c_str(), "%Lu", &tmp);
        nextFileIndex = (tmp + 1 >= nextFileIndex) ? tmp + 1 : nextFileIndex;
        tmp = AddCache(file_path);
        maxTimeStamp = (maxTimeStamp >= tmp) ? maxTimeStamp : tmp;
    }
    // update maxTimestamp
    max = maxTimeStamp;
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
    Cache *head_next = head->next;
    Cache *tmp = new Cache(str, ssTable, head_next);
    head->next = tmp;
    nextFileIndex++;
}

uint64_t CacheList::AddCache(const std::string &str) {
    // initialize from existing directory, don't need to update next file name here
    Cache *head_next = head->next;
    Cache *tmp = new Cache(str, head_next);
    head->next = tmp;
    return tmp->getTimeStamp();
}

void CacheList::get(uint64_t key, uint64_t &ts, std::string &ret) const {
    Cache *ptr = head->next; /* traversal pointer */
    while(ptr){
        ptr->get(key, ts, ret);
        ptr = ptr->next;
    }
}

bool CacheList::get_2(uint64_t key, uint64_t &ts, std::string &ret) const {
    bool ret_bool = false;
    Cache *ptr = head->next; /* traversal pointer */
    while(ptr){
        bool tmp = ptr->get_2(key, ts, ret);
        ret_bool = tmp || ret_bool;
        ptr = ptr->next;
    }
    return ret_bool;
}

void CacheList::delAllFiles() {
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

uint64_t CacheList::getNextFileIndex() const {
    return nextFileIndex;
}

void CacheList::clear() {
    Cache *tmp1 = head->next;
    Cache *tmp2;
    while(tmp1){
        tmp2 = tmp1->next;
        delete tmp1;
        tmp1 = tmp2;
    }
    head->next = nullptr;
}

std::vector<Range> CacheList::getNodes_0(std::vector<std::vector<CompactionNode*>> &nodes) {
    std::vector<Range> range;
    if(size()<3) return range;
    Cache *tmp = head->next;
    std::ifstream ifs;
    uint32_t size, beg, end;
    while(tmp){
        ifs.open(tmp->path);
        // file pointer which points to the end of file
        ifs.seekg(0, std::ios::end);
        end = ifs.tellg();
        // file pointer which points to the beginning of data
        ifs.seekg(tmp->pairs[0].offset, std::ios::beg);
        beg = ifs.tellg();
        // data block's size
        size = end - beg;
        char *str = new char[size+1];
        memset(str, '\0', size+1);
        ifs.read(str, size);
        ifs.close();
        std::string valuePool = str;
        // free memory
        delete [] str;
        // update range
        range.emplace_back(tmp->maxKey, tmp->minKey);
        // delete the compacting file
        utils::rmfile(tmp->path.c_str());

        // update &nodes
        uint32_t offset0 = tmp->pairs[0].offset;
        uint64_t count = tmp->count;
        uint64_t timestamp = tmp->timestamp;
        Pair *pairs = tmp->pairs;
        std::vector<CompactionNode*> a;
        int i;
        // 0 ~ count-2 value
        CompactionNode *node_ptr = nullptr;
        for (i = 0; i < count-1; ++i) {
            node_ptr = new CompactionNode(timestamp, pairs[i].key,
                                          valuePool.substr(pairs[i].offset-offset0,pairs[i+1].offset-pairs[i].offset), UP);
            a.push_back(node_ptr);
            node_ptr = nullptr;
        }
        // count-1 value
        node_ptr = new CompactionNode(timestamp, pairs[i].key,
                                      valuePool.substr(pairs[i].offset-offset0), UP);
        a.push_back(node_ptr);
        // update nodes
        nodes.push_back(a);

        // version 2 //


        // update tmp pointer
        tmp = tmp->next;
    }
    clear();
    return range;
}

std::vector<Range> CacheList::getNodes_k(std::vector<std::vector<CompactionNode*>> &nodes,
                                         const std::vector<Range> &range, uint32_t level) {
    bool lastLevel;
    std::string dir = "./data/level-" + std::to_string(level);
    std::string last_level_dir = "./data/level-" + std::to_string(level+1);
    if(!utils::dirExists(dir)){
        utils::mkdir(dir.c_str());
    }
    if(utils::dirExists(last_level_dir)){
        lastLevel = false;
    }
    else{
        lastLevel = true;
    }
    // ret val
    std::vector<Range> range2;
    uint32_t levelMaxSize = 1<<(level+1);
    Cache *tmp_1 = head;
    Cache *tmp_2 = head->next;
    std::ifstream ifs;
    uint32_t size, beg, end;
    while(tmp_2){
        if(inRange(range, tmp_2->maxKey, tmp_2->minKey)){
            ifs.open(tmp_2->path);
            // end of file
            ifs.seekg(0, std::ios::end);
            end = ifs.tellg();
            // beginning of data
            ifs.seekg(tmp_2->pairs[0].offset, std::ios::beg);
            beg = ifs.tellg();
            // data block's size
            size = end - beg;
            char *str = new char[size+1];
            memset(str, '\0', size+1);
            ifs.read(str, size);
            ifs.close();
            std::string valuePool = str;
            // free memory
            delete [] str;
            // remove the compacting file
            utils::rmfile(tmp_2->path.c_str());
            // insert new nodes
            uint32_t offset0 = tmp_2->pairs[0].offset;
            uint64_t count = tmp_2->count;
            uint64_t timestamp = tmp_2->timestamp;
            Pair *pairs = tmp_2->pairs;
            std::vector<CompactionNode*> a;
            int i;
            // 0 ~ count-2 value
            CompactionNode *node_ptr = nullptr;
            for (i = 0; i < count-1; ++i) {
                node_ptr = new CompactionNode(timestamp, pairs[i].key,
                                              valuePool.substr(pairs[i].offset-offset0,pairs[i+1].offset-pairs[i].offset), DOWN);
                if(node_ptr->key==4840){
                    int p=0;
                }
                a.push_back(node_ptr);
                node_ptr = nullptr;
            }
            // count-1 value
            node_ptr = new CompactionNode(timestamp, pairs[i].key,
                                          valuePool.substr(pairs[i].offset-offset0), DOWN);
            a.push_back(node_ptr);
            // update nodes
            nodes.push_back(a);
            // remove compaction cache
            tmp_1->next = tmp_2->next;
            delete tmp_2;
            tmp_2 = tmp_1->next;
        }
        else{
            tmp_1 = tmp_1->next;
            tmp_2 = tmp_2->next;
        }
    }
    std::vector<CompactionNode*> sorted = MergeSort(nodes, 0, nodes.size()-1);

    // a fatal bug once happened here
    if(lastLevel){
        uint64_t timestamp = 0;
        std::vector<CompactionNode*> vec_tmp;
        for(uint32_t i=0;i<sorted.size();++i){
            timestamp = (timestamp > sorted[i]->timestamp) ? timestamp : sorted[i]->timestamp;
            if(sorted[i]->value!="~DELETED~")
                vec_tmp.push_back(sorted[i]);
            else
                delete sorted[i];
        }
        sorted = vec_tmp;
        for(auto & i : sorted){
            i->timestamp = timestamp;
        }
    }
    else{
        uint64_t timestamp = 0;
        for(auto & i : sorted){
            timestamp = (timestamp > i->timestamp) ? timestamp : i->timestamp;
        }
        for(auto & i : sorted){
            i->timestamp = timestamp;
        }
    }
    splitNodes(sorted, level);

    // select new CompactionNodes
    // the old node array elements have been deleted, only nullptr remains
    nodes.clear();
    // cache index
    if(this->size() <= levelMaxSize) return range2;
    // first valid cache
    tmp_1 = head;
    std::vector<CompactionNode*> compactionNodes;
    // 注释掉之后会选取所有结点的进行compaction
//    for (uint32_t i=0;i<levelMaxSize;++i) tmp_1 = tmp_1->next;

    ////////////////////////////////////////////////////////////////////////////////
    while(tmp_1->next){
        tmp_2 = tmp_1->next;
        range2.emplace_back(tmp_2->maxKey, tmp_2->minKey);
        ifs.open(tmp_2->path);
        // end of file
        ifs.seekg(0, std::ios::end);
        end = ifs.tellg();
        // beginning of data
        ifs.seekg(tmp_2->pairs[0].offset, std::ios::beg);
        beg = ifs.tellg();
        // data block's size
        size = end - beg;
        char *str = new char[size+1];
        memset(str, '\0', size+1);
        ifs.read(str, size);
        ifs.close();
        std::string valuePool = str;
        // free memory
        delete [] str;
        // remove the compacting file
        utils::rmfile(tmp_2->path.c_str());
        // insert new nodes
        uint32_t offset0 = tmp_2->pairs[0].offset;
        uint64_t count = tmp_2->count;
        uint64_t timestamp = tmp_2->timestamp;
        Pair *pairs = tmp_2->pairs;
        std::vector<CompactionNode*> a;
        int i;
        // 0 ~ count-2 value
        CompactionNode *node_ptr = nullptr;
        for (i = 0; i < count-1; ++i) {
            node_ptr = new CompactionNode(timestamp, pairs[i].key,
                                          valuePool.substr(pairs[i].offset-offset0,pairs[i+1].offset-pairs[i].offset), UP);
            a.push_back(node_ptr);
            node_ptr = nullptr;
        }
        // count-1 value
        node_ptr = new CompactionNode(timestamp, pairs[i].key,
                                      valuePool.substr(pairs[i].offset-offset0), UP);
        a.push_back(node_ptr);
        // update nodes
        nodes.push_back(a);
        // remove compaction cache
        tmp_1->next = tmp_2->next;
        delete tmp_2;
    }
    return range2;
}

bool CacheList::inRange(const std::vector<Range> &range, uint64_t max, uint64_t min) {
    for(auto &itr : range){
        if((max>=itr.minKey && max<=itr.maxKey)||(min>=itr.minKey && min<=itr.maxKey))
            return true;
    }
    return false;
}

std::vector<CompactionNode*> CacheList::MergeSort(std::vector<std::vector<CompactionNode*>> &A,
                                                  uint64_t start, uint64_t end) {
    if(start>=end)
        return A[start];
    uint64_t mid = start + (end-start)/2;
    std::vector<CompactionNode*> left = MergeSort(A, start, mid);
    std::vector<CompactionNode*> right = MergeSort(A, mid+1, end);
    return MergeTwo(left, right);
}

std::vector<CompactionNode*> CacheList::MergeTwo(std::vector<CompactionNode*> &A, std::vector<CompactionNode*> &B) {
    std::vector<CompactionNode*> tmp;
    uint64_t i=0, j=0;
    while(i<A.size() && j<B.size()){
        CompactionNode *tmp1, *tmp2;
        if(A[i]->key < B[j]->key)
        {
            if(A[i]->key==10784){
                tmp1 = A[i];
            }
            tmp.push_back(A[i++]);
        }
        else if(A[i]->key > B[j]->key)
        {
            if(B[j]->key==10784){
                tmp2 = B[j];
            }
            tmp.push_back(B[j++]);
        }
        else{
            if(A[i]->timestamp > B[j]->timestamp){
                if(A[i]->key==10784){
                    tmp1 = A[i];
                }
                tmp.push_back(A[i++]);
                delete B[j++];
            }
            else if(A[i]->timestamp < B[j]->timestamp){
                if(B[j]->key==10784){
                    tmp2 = B[j];
                }
                tmp.push_back(B[j++]);
                delete A[i++];
            }
            else{
                if(A[i]->pos==UP && B[j]->pos==DOWN){
                    if(A[i]->key==10784){
                        tmp1 = A[i];
                    }
                    tmp.push_back(A[i++]);
                    delete B[j++];
                }
                else if(A[i]->pos==DOWN && B[j]->pos==UP){
                    if(B[j]->key==10784){
                        tmp2 = B[j];
                    }
                    tmp.push_back(B[j++]);
                    delete A[i++];
                }
                else{
                    int err = 10086;
                }
            }
        }
    }
    while(i<A.size()) {
        CompactionNode *tmp1;
        if(A[i]->key==10784){
            tmp1 = A[i];
        }
        tmp.push_back(A[i++]);
    }
    while(j<B.size()) {
        CompactionNode *tmp2;
        if(B[j]->key==10784){
            tmp2 = B[j];
        }
        tmp.push_back(B[j++]);
    }
    return tmp;
}

void CacheList::splitNodes(std::vector<CompactionNode *> &sorted, uint32_t level) {
    std::vector<SSTable*> SSTables = getSSTables(sorted);
    std::string dir = "./data/level-" + std::to_string(level) + "/";
    std::string filename, path;
//    for (auto & SSTable : SSTables) {
//        // add cache
//        filename = std::to_string(nextFileIndex) + ".sst";
//        path = dir + filename;
//        AddCache(path, *SSTable);
//        // write to files
//        SSTable->writeToFile(path.c_str());
//        // delete SSTables in memory
//        delete SSTable;
//    }
    for (int i = (int)(SSTables.size()-1); i >=0; --i) {
        // add cache
        filename = std::to_string(nextFileIndex) + ".sst";
        path = dir + filename;
        AddCache(path, *SSTables[i]);
        // write to files
        SSTables[i]->writeToFile(path.c_str());
        // delete SSTables in memory
        delete SSTables[i];
    }
}

std::vector<SSTable*> CacheList::getSSTables(std::vector<CompactionNode*> &sorted) {
    std::vector<SSTable*> ret;
    SSTable *sst_ptr = nullptr;
    uint64_t start, end, timestamp=0;
    start = 0;
    uint64_t accumulation = 10272;
    for (uint64_t i=0;i<sorted.size();++i){
        accumulation = accumulation + 12 + sorted[i]->value.length();
        timestamp = (timestamp > sorted[i]->timestamp) ? timestamp : sorted[i]->timestamp;
        if(accumulation > 2097000){
            end = i;
            sst_ptr = new SSTable(sorted, start, end);
            ret.push_back(sst_ptr);
            sst_ptr = nullptr;
            start = i;
            // 10 KB + 32 Bytes
            accumulation = 10272;
        }
    }
    sst_ptr = new SSTable(sorted, start, sorted.size());
    ret.push_back(sst_ptr);
    for (auto &itr : ret) {
        itr->setTimestamp(timestamp);
    }
    return ret;
}

