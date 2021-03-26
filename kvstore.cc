#include "kvstore.h"
#include <string>

KVStore::KVStore(const string &dir): KVStoreAPI(dir)
{
	MemTable=new SkipList<uint64_t,string>;
	cur_mem_data=0;
	root_dir=dir;
	if (!exists(root_dir))
	{
		if(!create_directory(root_dir))
		cout<<"create directory failed!";
	}
	sstables=new SStable(root_dir);
//	cur_maxlevel=0;
}

KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	string str=s;
	Node<uint64_t,string> *t=MemTable->search_element(key);
	if(t!=NULL){
		cur_mem_data-=8+t->get_value().length();
	}
	MemTable->insert_element(key,str);
	cur_mem_data+=8+s.length();
	if(cur_mem_data<=MAX_MEMDATA)//存储数据量不超过2MB
		return;
	//存储数据量超过2MB了
	sstables->W_to_disk(MemTable);
	delete MemTable;
	MemTable=new SkipList<uint64_t,string>;
	cur_mem_data=0;
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	Node<uint64_t, string> *tmp=MemTable->search_element(key);
	if(tmp) 
		return tmp->get_value();
	else{
		string t=sstables->search_element(key);
		return t;
	}
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	if(get(key)!=""){
		put(key,"");
		return true;
	}
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	delete MemTable;
	MemTable=new SkipList<uint64_t,string>;
	cur_mem_data=0;
	sstables->reset();
}

