#include <string>
#include <iostream>

#include "../Header/kvstore.h"
#include "../Header/utils.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    ssTable = nullptr;
    maxTimeStamp = 0;
    // update max filename
    uint64_t maxts = 0;

    std::string subpath;
    std::vector<std::string> dir_name;
    utils::scanDir(dir, dir_name);
    uint32_t size = dir_name.size();
    for (uint32_t i = 0; i < size; ++i) {
        subpath = dir + "/level-" + std::to_string(i);
        CacheList* cacheList_tmp = new CacheList(subpath, maxts);
        storage.push_back(cacheList_tmp);
    }
    maxTimeStamp = maxts;
}

KVStore::~KVStore(){
    // the data that remains in memory need to be wrote to disk
//    writeToDisk();

    // destructor body
    uint32_t size = storage.size();
    for (int i = 0; i < size; ++i) {
        if(storage[i]!= nullptr)
            delete storage[i];
    }
    storage.clear();
}

void KVStore::writeToDisk() {
    maxTimeStamp++;
    ssTable = new SSTable(maxTimeStamp, memTable);

    // avoid null pointer
    CacheList *cacheList_ptr;
    if(storage.empty()) {
        cacheList_ptr = new CacheList();
        storage.push_back(cacheList_ptr);
    }
    cacheList_ptr = storage[0];
    // calculate next fine index and transfer it to filename string
    uint64_t nextFileIndex = cacheList_ptr->getNextFileIndex();
    std::string filename = std::to_string(nextFileIndex) + ".sst";
    std::string dir = "./data/level-0/";
    if (!utils::dirExists(dir)) {
        utils::mkdir(dir.c_str());
    }
    std::string path = dir + filename;
    ssTable->writeToFile(path.c_str());
    cacheList_ptr->AddCache(path, *ssTable);
    delete ssTable;
    ssTable = nullptr;
    memTable.reset();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    static int count=1;
    if(memTable.put(key, s))
        return;
    else{
        /* more than 2MB */

        writeToDisk();
        if(count ==285){
            int a = 10;
        }
        std::string tmp1=this->get(10784);
        compaction();
        count++;
        std::string tmp2=this->get(10784);
//        if(tmp1 != tmp2){
//            int a = 10;
//        }
        memTable.put(key, s);
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string retVal = memTable.get(key);
    if(retVal=="~DELETED~") return "";

    uint64_t timestamp = 0;
    /* not found in memTable */
    if(retVal.empty()){
        for(auto & itr : storage){
            bool tmp = itr->get_2(key, timestamp, retVal);
            if(tmp) break;
        }
    }
    if(retVal=="~DELETED~") retVal="";
    return retVal;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string retVal = memTable.get(key);

    uint64_t timestamp = 0;
    /* not found in memTable */
    if(retVal.empty()){
        for(auto & itr : storage){
            itr->get(key, timestamp, retVal);
        }
    }
    if(retVal=="~DELETED~") retVal="";
    if(retVal.empty()) return false;

//    if(!memTable.del(key)){
//        put(key, "~DELETED~");
//    }
    put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
//    std::cout << "reset begin " << std::endl;
    memTable.reset();
    delete ssTable;
    ssTable = nullptr;
    maxTimeStamp = 0;

    /* delete storage */
    uint32_t size = storage.size();
    for (int i = 0; i < size; ++i) {
        delete storage[i];
    }
    storage.clear();

    /* delete files */
    std::string path = "./data";
    std::vector<std::string> name_dir;
    utils::scanDir(path, name_dir);
    for(auto itr1=name_dir.begin();itr1!=name_dir.end();++itr1){
        std::string sub_path = path + "/" + *itr1;
        std::vector<std::string> name_file;
        utils::scanDir(sub_path, name_file);
        for(auto itr2=name_file.begin();itr2!=name_file.end();++itr2){
            std::string file_path = sub_path + "/" + *itr2;
            utils::rmfile(file_path.c_str());
        }
        utils::rmdir(sub_path.c_str());
    }
}

void KVStore::compaction() {
    // level-0
    if(storage[0]->size() < 3)
        return;
    else{// storage[0]->size == 3
        std::vector<Range> range, range2;
        std::vector<std::vector<CompactionNode*>> nodes;
        range = storage[0]->getNodes_0(nodes);
        uint32_t level = 1;
        do{
            if(storage.size()==level){
                CacheList *tmp = new CacheList();
                storage.push_back(tmp);
            }
            range2 = storage[level]->getNodes_k(nodes, range, level);
            range = range2;
            level++;
        }while(!range.empty());
    }
}
