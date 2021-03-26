#ifndef SSTABLE_H
#define SSTABLE_H

#include <cstdint>
#include <string>
#include "skiplist.h"
#include <vector>
#include <map>
#include <list>
#include <filesystem>
#include <fstream>
using namespace std;
using namespace std::filesystem;
#define MAX_MEMDATA 2097152 //2MB
#define MIN_SSTABLE 1677730	//1.6MB
#define S_UINT64 8
#define S_INT 4
#define S_CHAR 1


typedef pair<uint64_t,uint64_t> KV_index ;
typedef pair<uint64_t,string> KV_data ;
typedef pair<int,int> RW_file ;

class SStable{
private:
	int max_level;
	int cur_files_0;
	int cur_files_last;
	int cur_files_cache;// -1 层文件数
	string find_value="";
	string root_dir;
	map<uint64_t,uint64_t> indexes;
	map<uint64_t,string> datas;
	uint64_t min_key;
	uint64_t max_key;
	vector<RW_file> RW_files;
	uint64_t cur_level_max_key;
	uint64_t cur_data_size;
public:
	SStable(const string f_dir);//创建sstable，首先输入根目录

	void W_to_disk(SkipList<uint64_t,string> *mt);//写入文件

	void RW_to_disk(int level, int seq,uint64_t maxkey=UINT64_MAX); //若seq=-1;表示将内存中所有剩余数据全部读入

	int Read_file(int level,int seq,bool flag=false);//先读index，判断是否区间有重叠，有则返回true，且归并排序,加flag表示强制读

	void f_doublication(int level);

	void B_f_d(int level, int low,int high);

	void Compaction(int level);//递归遍历Compaction

	void Merge(KV_index *t_indexes,char *t_datas,int num,int lastchar);

	string search_element(uint64_t key);

	int search_file(uint64_t key,int level,int seq);

	void reset();

	void handle_empty(int start); //出现空缺

	void handle_overflow(int end ); //出现overflow

	RW_file calculate_old(int level,int seq,int movestep); //往前移动计算
	RW_file calculate_new(int level,int seq,int movestep); //往后移动计算
};

#endif