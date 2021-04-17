#pragma once

#include "kvstore_api.h"
#include "SkipList.h"

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    SkipList memTable;
public:
    __attribute__((unused)) KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

};