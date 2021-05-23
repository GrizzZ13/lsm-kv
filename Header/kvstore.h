#pragma once

#include <vector>

#include "kvstore_api.h"
#include "SkipList.h"
#include "SSTable.h"
#include "CacheList.h"

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    SkipList memTable;
    SSTable *ssTable;
    uint64_t maxTimeStamp;
    std::vector<CacheList*> storage;
public:
    KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	// write from memory to disk and clear sstable and memory table
	void writeToDisk();

	void compaction();
};
