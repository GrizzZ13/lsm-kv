#include <string>
#include <iostream>

#include "../Header/kvstore.h"
#include "../Header/utils.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    ssTable = nullptr;
    countInDisk = 0;
}

KVStore::~KVStore(){
//    std::cout << "destructor" << std::endl;
    uint32_t size = storage.size();
    for (int i = 0; i < size; ++i) {
        if(storage[i]!= nullptr)
            delete storage[i];
    }
//    std::cout << "destructor finished" << std::endl;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    bool success = memTable.put(key, s);
    if(success) return;

    /* more than 2MB */
    countInDisk++;
    ssTable = new SSTable(countInDisk, memTable);
    std::string filename = std::to_string(countInDisk);
    std::string dir = "../data/level-0/";
    if (!utils::dirExists(dir)) {
        utils::mkdir(dir.c_str());
    }
    std::string path = dir + filename + ".sst";
    ssTable->writeToFile(path.c_str());
    CacheList *cacheList_ptr;
    if(storage.empty()) {
        cacheList_ptr = new CacheList();
        storage.push_back(cacheList_ptr);
    }
    else{
        cacheList_ptr = storage[0];
    }
    cacheList_ptr->AddCache(path, *ssTable);

    delete ssTable;
    ssTable = nullptr;
    memTable.reset();

    memTable.put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
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

    memTable.del(key);
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
    countInDisk = 0;

//    std::cout << "reset storage " << std::endl;

    /* delete storage */
    uint32_t size = storage.size();
    for (int i = 0; i < size; ++i) {
        delete storage[i];
    }
    storage.clear();

    /* delete files */
    std::string path = "../data";
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
//    std::cout << "reset finished" << std::endl;
}
