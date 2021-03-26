#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include <list>
#include <fstream>
#include<filesystem>
#include "sstable.h"
using namespace std;
using namespace std::filesystem;





class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	SkipList<uint64_t,string> *MemTable;
	SStable *sstables;
	uint64_t cur_mem_data;//记录当前memtable中的数据量
	string root_dir;
	void Compaction();
public:
	KVStore(const string &dir);

	~KVStore();

	void put(uint64_t key, const string &s) override;

	string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

};
