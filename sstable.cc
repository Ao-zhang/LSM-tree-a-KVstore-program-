#include "sstable.h"
#include <cmath>
#include <stdio.h>
#include <stdlib.h>

SStable::SStable(const string f_dir){
		min_key=max_key=0;//缓存去最小最大键值初始为0
		max_level=-1;//初始化当前最大的level为0
		cur_files_0=cur_files_last=cur_files_cache=0;//level0和最大的level的文件数都为0;
		root_dir=f_dir;
		if(!exists(f_dir)){  //如果没有根目录，就创建
			if(!create_directory(f_dir))
				cout<<"create directory failed!";
		}
		while(true){
			string t="level"+to_string(max_level);
			path tpath=f_dir;//防止更改f_dir
			tpath/=t;
			if(max_level<=0 && !exists(tpath)){
			 	if(!create_directory(tpath))
				cout<<"create directory failed!";
			}	
			else if(!exists(tpath)) break;
			max_level++;
		}
		max_level--;
		while(true){  //找最后一层的最大文件数
			string file_name=root_dir+"/level"+to_string(max_level)+"/s-"+to_string(cur_files_last);
			if(! exists(file_name))break;
			cur_files_last++;
		}
		while(true){
			string file_name=root_dir+"/level0/s-"+to_string(cur_files_0);
			if(!exists(file_name))break;
			cur_files_0++;
		}
		
		/*string c="level"+to_string(-1);   //创建缓冲层	
		tpath=f_dir;//防止更改f_dir
		tpath/=c;
		if(!exists(tpath)){
			if(!create_directory(tpath))
			cout<<"create directory failed!";
		}	*/
}

void SStable::Merge(KV_index *t_indexes,char *t_datas,int num,int lastchar){
	vector<KV_index> C_index(indexes.begin(),indexes.end());
	int cur_size=indexes.size();
	int i=0,j=0;//i指向文件读取出的t_indexes的cursor
	//这里先不修改offset，写入文件的时候再写
		while(j<cur_size){
			if(i==num) break;//读取数据处理完毕或者原来的数据找到了末尾了
			if(C_index[j].first<t_indexes[i].first)	j++;//继续后找	
			else if(C_index[j].first==t_indexes[i].first)i++; //此键值对不用插入
			else{
				KV_data t;
				t.first=t_indexes[i].first;
				uint64_t start=t_indexes[i].second+8;
				uint64_t end=(i+1<num)?t_indexes[i+1].second:lastchar;//都不减1，这样 方便计算长度
				i++;
				int str_size=end-start;
				string t_s(t_datas+start,str_size);
				t.second=t_s;
				cur_data_size+=8+str_size;//插入使得内存中数据量增大
				datas.insert(pair(t.first,t.second));//插入新数据
				indexes.insert(pair(t.first,0));//对于新插入的键值对，先初始offset为0；因为之后写入文件的时候需要重新计算
				
			}
		} 
		C_index.clear();
	while(i<num){
				KV_data t;
				t.first=t_indexes[i].first;
				uint64_t start=t_indexes[i].second+8;
				uint64_t end=(i+1<num)?t_indexes[i+1].second:lastchar;//都不减1，这样 方便计算长度
				i++;
				int str_size=end-start;
				string t_s(t_datas+start,str_size);
				t.second=t_s;
				cur_data_size+=8+str_size;//插入使得内存中数据量增大
				datas.insert(pair(t.first,t.second));//插入新数据
				indexes.insert(pair(t.first,0));//对于新插入的键值对，先初始offset为0；因为之后写入文件的时候需要重新计算
	}
}

void SStable::Compaction(int level){
	if(level==0){
			cur_files_0=0;
			cur_level_max_key=0;
			for(int i=0;i<2;i++){//从较新的文件开始读取level0中的所有sstable文件，并且合并
				Read_file(0,i);//read之后会merge，调整最小、最大键值
			}
			if(max_level==0){ 
				string t="level"+to_string(1);
				path tpath=root_dir;//防止更改f_dir
				tpath/=t;
				if(!exists(tpath)){
					if(!create_directory(tpath)){
						cout<<"create directory failed!";
					}
					
				}	
				max_level=1;cur_files_last=0;
				RW_to_disk(1,-1);//将剩下的内容写入level1/s-0;-1表示全部写入
				return;
			}
			else {
				f_doublication(1);
				if(!indexes.empty())
				Compaction(1);
				return;
			}
	}
	//if(indexes.empty()) return;
	if(level==max_level ){//**********************************************************************************
		RW_to_disk(max_level,-1);//直接写
		return;
	}
	f_doublication(level+1);
	if(!indexes.empty())
	Compaction(level+1);
}

int SStable::Read_file(int level,int seq,bool flag){  //左移就先不读，只有要向右移动才读
	if(level==0 ||flag){//直接读,flag来源4->，2->
		string nf_name=root_dir+"/level"+to_string(level)+"/s-"+to_string(seq);
		FILE *f=fopen(nf_name.c_str(),"rb");
		if(!f) return -2;/******************************************************/
		uint64_t offset;
		int num;//num记录有多少组数据
		fread(&offset,S_UINT64,1,f);//先读取索引区的位置
		fread(&num,S_INT,1,f); //读取数据组个数
		offset-=12;//此时表示键值对的大小了
		char *buffer=new char[offset];
		fread(buffer,S_CHAR,offset,f);//读入所有数据
		KV_index *t_index=new KV_index[num];
		fread(t_index,16,num,f); //读索引
		//fflush(f);
		fclose(f);
		int flag=2;
		if(t_index[0].first<min_key)min_key=t_index[0].first;
		if(t_index[num-1].first>max_key){
			max_key=t_index[num-1].first;
			flag=0;  //停止后找
		}
		Merge(t_index,buffer,num,offset);//offset不用减一，用来算字符长度时，可以直接减去start
		if(!remove(nf_name)){
			cout<<"delete "<<nf_name<<" failed !"<<endl;
		}  
		if(level!=0){
			RW_files.push_back(pair(level,seq));
		}
		if(level!=0){
			cur_level_max_key=(cur_level_max_key>t_index[num-1].first)?cur_level_max_key:t_index[num-1].first;
		}
		return flag;
	}
	int maxseq=(level!=max_level)?pow(2,level+1):cur_files_last;
	if(seq>=maxseq) return 0;
	int result=0;
	string nf_name=root_dir+"/level"+to_string(level)+"/s-"+to_string(seq);
	FILE *f=fopen(nf_name.c_str(),"rb");
	uint64_t offset;
	int num;//num记录有多少组数据
	fread(&offset,S_UINT64,1,f);//先读取索引区的位置
	fread(&num,S_INT,1,f); //读取数据组个数			
	fseek(f,offset,SEEK_SET);
	KV_index *t_index=new KV_index[num];
	fread(t_index,16,num,f); //先读索引
	fseek(f,0,SEEK_END);
	uint64_t t_size=ftell(f);
	//fflush(f);
	//fclose(f);
	

	if(t_size>=MIN_SSTABLE)  //对于大文件
		if(min_key>t_index[num-1].first)  //没有覆盖，不用重新写
		{	
			fclose(f);
			return 8;//表示此文件区间完全小于，右移二分 +
		}
			
	if(min_key>t_index[num-1].first)  
		{	
			result=2;
			min_key=t_index[0].first;
			//针对最末尾的小文件，纳入归并范围，并继续右移
		}
			
	
		if(max_key<t_index[0].first) {
			fclose(f);
			return -8;//表示此文件区间完全大于，左移二分 -
		}
	//对于小文件********************************
		
		if(min_key<t_index[0].first && max_key<=t_index[num-1].first){
			fclose(f);
			return -2;//还需左移查找 --先不读
			//max_key=t_index[num-1].first;
		} 
		if(min_key<=t_index[0].first && max_key>=t_index[num-1].first){
			fclose(f);
			result=4;//需要左右都查找！！！！！！！！！！！！！
			return result;
		} 
		if(min_key>=t_index[0].first && max_key>t_index[num-1].first){
			result=2;//还需右移查找 ++
			min_key=t_index[0].first;
		} 
		if(min_key>=t_index[0].first && max_key<=t_index[num-1].first){
			result=0;//被包含了，不需要查找了
		} 
	offset-=12;//offset变成kvdata的长度
	char *buffer=new char[offset];
	//f=fopen(nf_name.c_str(),"rb");
	fseek(f,12,SEEK_SET);
	fread(buffer,S_CHAR,offset,f);//读入所有数据		 
	fflush(f);fclose(f);
	if(!remove(nf_name)){
			cout<<"delete "<<nf_name<<" falied !"<<endl;
		}  //删除level0的文件
	RW_files.push_back(RW_file(level,seq));//表明此file需要重新写
	Merge(t_index,buffer,num,offset);

	if(result==2)	//取目前读到的最大的key
	cur_level_max_key=(cur_level_max_key>t_index[num-1].first)?cur_level_max_key:t_index[num-1].first;
	
	return result; //
}
//assertion：调用此函数说明必有数据需要写入且默认key是递增的，不会出现陡增以及跨度很大的情况；
void SStable::f_doublication(int level){//找到有重叠区间的文件
	int high=(max_level!=level)?pow(2,(level+1)): cur_files_last;
	int flag1=Read_file(level,high-1);
	if( flag1==2 ||flag1==8 ||flag1==0){	 //需要右移，
		return;
	}
		B_f_d(level,0,high-1);
	if( (flag1 <0 )||(level==max_level) ){ //此层应该涵盖所有区间了,或者归并到了最后一层
		if(!RW_files.empty() ){ //有归并
		int fsize=RW_files.size();
		int a;
		for( a=0;a<fsize && !indexes.empty();a++){//归并后重新写入原文件
			RW_file t_file=RW_files[a];
			RW_to_disk(t_file.first,t_file.second);
		}
		if(a<fsize) {
			handle_empty(a);//将后面的所有文件迁移+改名
		}
		if(indexes.empty()){
			 RW_files.clear(); //刚好写完
			 return;
		}
		while(!indexes.empty()){
				RW_to_disk(-1,cur_files_cache++);//先把文件写到缓存区域
		}
		handle_overflow(a-1);
		 RW_files.clear(); //刚好写完
		}
	} 


	

}

void SStable::B_f_d(int level,int low,int high){
	if(low>=high){ Read_file(level,low,true);return;}

	int  middle=(low+high)/2;
	int flag=Read_file(level,middle);
	switch (flag)
	{
	case -8://左边继续二分
		B_f_d(level,low,middle-1);
		break;
	case 8://右边继续二分
		B_f_d(level,middle+1,high);
		break;
	case -2:{//临近向左查找
		B_f_d(level,low,middle-1);
		Read_file(level,middle,true); //强制读取
		break;
	}
	case 4:{ //左右都查找
		B_f_d(level,low,middle-1);//先从左边找到头
		Read_file(level,middle,true);
	}
	case 2:{//临近向右查找
		while(++middle<=high && Read_file(level,middle,true)>0){  //大于0就是可以向右找，不可能大于8
		int fsize=RW_files.size();   //为防止内存文件过多，先写一部分
		if(fsize>5){
			int i=0;
			for(i;i<fsize && cur_data_size>MAX_MEMDATA &&indexes.begin()->first<cur_level_max_key;i++){
				auto a_file=RW_files[i];
				RW_to_disk(a_file.first,a_file.second,cur_level_max_key);
			}
			auto iter=RW_files.begin();
			RW_files.erase(iter,iter+i);
			}
		}
		break;
	}
	
	default :return;
	}
}

void SStable::RW_to_disk(int level, int seq,uint64_t maxkey){
	if(seq<0){		//写完 全部数据
		if(indexes.empty())return;
		if(cur_files_last==pow(2,(max_level+1))){//新增一层继续写
			max_level++;cur_files_last=0;
				string t="level"+to_string(max_level);
				path tpath=root_dir;//防止更改f_dir
				tpath/=t;
			if(!exists(tpath)){
			if(!create_directory(tpath))
				cout<<"create directory failed!";
			}
			RW_to_disk(max_level,-1);//在新一层写
			return;
		}
		RW_to_disk(level,cur_files_last);//此层还能写；就先写
		if(!indexes.empty()) 
			RW_to_disk(level,-1);
		return;
	}
	if(level==max_level && seq==cur_files_last) //新建了文件写入
		cur_files_last++;
	
	uint64_t offset=0;
	int num=0;
	vector<KV_index> t_indexes;
	vector<KV_data> t_datas;
	for(auto diter=datas.begin();datas.size()>0 && offset<MAX_MEMDATA && diter->first<maxkey;){
		
		string value=diter->second; 
		uint64_t key=diter->first;
		t_indexes.push_back(pair(key,offset));
		t_datas.push_back(pair(key,value));
		offset+=8+value.length();
		indexes.erase(key);
		datas.erase(diter++);
		num++;
	}
	cur_data_size-=offset;//写数据入文件的时候datasize变小了;
	string nf_name=root_dir+"/level"+to_string(level)+"/s-"+to_string(seq);
	FILE *f=fopen(nf_name.c_str(),"wb+");
	if(f==NULL){
		cout<<"error when writing binary file in RW"<<endl;
		return;
	}
	uint64_t t=offset+12;//直接写入offset会出现后四个byte数据乱码的情况！！！！！！！！！
		fwrite(&t,S_UINT64,1,f);//文件头8byte记录索引区数据位置
		fwrite(&num,S_INT,1,f);//记录总的数据组数
		//fflush(f);//调试用，立刻写入文件;
	for(int i=0;i<num;i++){
		KV_data td=t_datas[i];
		fwrite(&td.first,S_UINT64,1,f);
		fwrite(td.second.c_str(),td.second.length(),1,f);
	}
		t_datas.clear();
		//fflush(f);
		fwrite(&t_indexes[0],16,num,f);//从vector首地址开始写
		t_indexes.clear();
		fflush(f);fclose(f);
	
}

void SStable::W_to_disk(SkipList<uint64_t,string> *mt){
		Node<uint64_t,string> *tmp=mt->kv_list();
		min_key=max_key=tmp->get_key();
		int offset=0;
		indexes.clear();
		datas.clear();
		cur_data_size=0;
		int num=0;//数据组个数;
		while(tmp!=NULL){
			uint64_t key=tmp->get_key();
			string value=tmp->get_value(); 
			indexes.insert(pair(key,offset));
			datas.insert(pair(key,value));
			offset+=8+value.length();
			tmp=tmp->forward[0];
			max_key=key;
			num++; //数据组个数+1
		}
		cur_data_size=offset;//更新目前内存中数据量大小*******************
		if(cur_files_0<2){
			if(max_level==0) cur_files_last++;
			string nf_name=root_dir+"/level0/s-"+to_string(cur_files_0++);
			vector<KV_index> t_indexes(indexes.begin(),indexes.end());
			uint64_t t=offset+12;//直接写入offset会出现后四个byte数据乱码的情况！！！！！！！！！
			vector<KV_data> t_datas(datas.begin(),datas.end());
			FILE *f=fopen(nf_name.c_str(),"wb+");
			if(f==NULL){
				cout<<"error when writing binary file IN W"<<endl;
				return;
			}
			fwrite(&t,S_UINT64,1,f);//文件头8byte记录索引区数据位置
			fwrite(&num,S_INT,1,f);//记录总的数据组数
			fflush(f);
			for(int i=0;i<num;i++){
				KV_data td=t_datas[i];
				fwrite(&td.first,S_UINT64,1,f);
				fwrite(td.second.c_str(),td.second.length(),1,f);
			}
			fwrite(&t_indexes[0],16,num,f);//从vector首地址开始写
			fflush(f);
			fclose(f);	
			t_indexes.clear();
			t_datas.clear();
		}
		else Compaction(0);//从level0开始compaction
}

string SStable::search_element(uint64_t key){
	string result;
	for(int t_level=0;t_level<=max_level;t_level++){
		int max=(t_level==max_level)? cur_files_last:pow(2,(t_level+1));
		if(t_level==0){  //在level0文件数不稳定，可能为0
			if(cur_files_0==0) continue;//直接后找
			if(cur_files_0==2){  
				if(search_file(key,0,1)==1)break;
			}
			if(search_file(key,0,0)==1)break;
			continue;
		}
		int flag=search_file(key,t_level,max-1);
		if(flag==1 || flag==-1) break;//找到了
		if(flag==2) continue; //找下一层(++)
		else{
			int flag2= search_file(key,t_level,0);
			if(flag2 < 2) break;//1：找到或者 0:找不到(--)
			else{
				int low=1,high=max-2;//第一个和最后一个都已经找过了
				int flag3;
				while (low<=high)
				{
					int middle=(low+high)/2;
					flag3=search_file(key,t_level,middle);
					if(flag3==1 || flag3==-1) break;
					if(flag3==0) high=middle-1;
					if(flag3==2)low=middle+1;
				}
				if(flag3==1 ||flag3==-1)break;
			}
		}
	}
	if(find_value!="") {
		string t=find_value;
		find_value="";
		return t;
	}
	return "";
}

int SStable::search_file(uint64_t key,int level,int seq){
	int result=0;
	string nf_name=root_dir+"/level"+to_string(level)+"/s-"+to_string(seq);
	
	uint64_t offset;
	int num;//num记录有多少组数据
	FILE *f=fopen(nf_name.c_str(),"rb");
	if(!f)return -1;
	fread(&offset,S_UINT64,1,f);//先读取索引区的位置
	fread(&num,S_INT,1,f); //读取数据组个数			
	fseek(f,offset,SEEK_SET);
	KV_index *t_index=new KV_index[num];
	fread(t_index,16,num,f); //先读索引
	//fflush(f);
	//fclose(f);
		if(key>t_index[num-1].first)  //
			{	
				fclose(f);
				return 2;//表示此文件区间完全小于，右移二分 +
			}
		if(key<t_index[0].first){
			fclose(f);
			return 0;//表示此文件区间完全小于，左移二分 -
		} 
			//读入数据并merge
	int low=0,high=num-1,middle=0;;
	bool flag=false;
	while(low<=high){
		middle=(low+high)/2;
		int tmp=t_index[middle].first;
		if(tmp>key) high=middle-1;
		else if(tmp<key) low=middle+1;
		else {
			flag=true;
			break;
		}
	}
	if(flag){
		//f=fopen(nf_name.c_str(),"rb");
		uint64_t start=t_index[middle].second+20;//+上12个byte以及前8个byte的key
		uint64_t end=(middle+1<num)?t_index[middle+1].second+12:offset;
		int size=end-start;
		char* f_value=new char[size];
		fseek(f,start,SEEK_SET);
		fread(f_value,size,1,f);
		//fflush(f);
		fclose(f);
		find_value.assign(f_value,size);

		return 1;//1表示存在
	
	}
	fclose(f);
	return -1;//-1表示不存在
}

void SStable::reset(){
	//删除记录
	int i=-1;
	while(true){
		string t="level"+to_string(i++);
			path tpath=root_dir;
			tpath/=t;
			if(exists(tpath)){
				if(!remove_all(tpath)){
					cout<<"delete "<<tpath<<" failed !"<<endl;
				}
			}
			else break;
	}
	max_level=cur_files_0=cur_files_last=cur_files_cache=0;
	indexes.clear();
	datas.clear();
	RW_files.clear();
	min_key=max_key=0;
	string t="level"+to_string(0);
		//创建level0
		path tpath=root_dir;//防止更改f_dir
		tpath/=t;
		if(!exists(tpath)){
			if(!create_directory(tpath))
			cout<<"create directory failed!";
		}	
		string c="level"+to_string(-1);   //创建缓冲层	
		tpath=root_dir;//防止更改f_dir
		tpath/=c;
		if(!exists(tpath)){
			if(!create_directory(tpath))
			cout<<"create directory failed!";
		}	
}

void SStable::handle_empty(int start){
	int move_size=RW_files.size()-start;//*
	RW_file file_start=RW_files[start];
	//RW_files.clear();//清除需要重写的文件列表
	for(int start_level=file_start.first;start_level<=max_level;start_level++){
		int max=(start_level!=max_level)?pow(2,1+start_level):cur_files_last;
		int start_seq=(start_level!=file_start.first)?0:file_start.second;
		for( start_seq;start_seq<max;start_seq++){
			RW_file old_file=calculate_old(start_level,start_seq,move_size);
		
			if(old_file.first==max_level&&old_file.second>=cur_files_last){ //后面空了
			  	while(max_level>start_level){
					 string t="level"+to_string(max_level--);
						path tpath=root_dir;
						tpath/=t;
						if(exists(tpath)){
						if(!remove_all(tpath))
						cout<<"delete "<<tpath<<" failed !"<<endl;
					}
				
				}
				cur_files_last=start_seq;
				return;
			}
			string new_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(start_seq);
			string old_name=root_dir+"/level"+to_string(old_file.first)+"/s-"+to_string(old_file.second);
			if(rename(old_name.c_str(),new_name.c_str())){ //命名/移动失败
				cout<<"move file failed in recent level!"<<endl;
			}
		}
/*	
		//同层移动                     11111111111111111
		for( start_seq;start_seq+move_size<max;start_seq++){
			string new_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(start_seq);
			string old_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(start_seq+move_size);
			if(rename(old_name.c_str(),new_name.c_str())){ //命名/移动失败
				cout<<"move file failed in recent level!"<<endl;
			}
		}
		if(start_level==max_level){		//在最后一层移动，需要重新计算maxlevel
			cur_files_last-=move_size;
			if(cur_files_last<=0){ 
				cur_files_last=pow(2,max_level)-1+cur_files_last;
				max_level--;
			}
			break;  //移动完毕
		}
		//下面是跨层移动
		int next_level=start_level+1;
		int max2=((next_level)!=max_level)?pow(2,1+next_level):cur_files_last;
		
		for(int i=0,start_seq;i<move_size;i++,start_seq++){
			string new_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(start_seq);
			string old_name=root_dir+"/level"+to_string(next_level)+"/s-"+to_string(i);
			if(rename(old_name.c_str(),new_name.c_str())){ //命名/移动失败
				cout<<"move file failed in recent level!"<<endl;
			}
		}*/

	}
	
}

void SStable::handle_overflow(int end){
	int move_size=cur_files_cache;
	cur_files_cache=0;
	RW_file file_end=RW_files[end];
		//	RW_files.clear();
			//所有文件向后移
	for(int start_level=max_level;start_level>=file_end.first;start_level--){
		int min_seq=(start_level!=file_end.first)?0:file_end.second+1;
		int max=pow(2,1+start_level);
		int start_seq=(start_level==max_level)?cur_files_last-1:max-1;
		for( int k=start_seq;k>=min_seq;k--){
			RW_file new_file=calculate_new(start_level,k,move_size);
			if(k==start_seq && start_level==max_level){ //对于最后一个文件的移动需要特殊处理
				cur_files_last=new_file.second+1; 	//最后一层的文件数量改变
				if(new_file.first>max_level){  //需要新建层
					while(++max_level<=new_file.first){
						path tpath=root_dir;//防止更改root_dir
						string t="level"+to_string(max_level);
						tpath/=t;
						if(!exists(tpath)){
							if(!create_directory(tpath))
								cout<<"create directory failed!";
						}
					}
				}
			}
			string old_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(k);
			string new_name=root_dir+"/level"+to_string(new_file.first)+"/s-"+to_string(new_file.second);
			if(rename(old_name.c_str(),new_name.c_str())){ //命名/移动失败
						cout<<"move file failed in recent level!"<<endl;
					}
		}
		
		/*
			int max=pow(2,1+start_level);
			int min_seq=(start_level!=file_end.first)?0:file_end.second+1;
			if(min_seq>=max-1)break;//
			int start_seq=(start_level==max_level)?cur_files_last-1:max-1;
			if(start_seq+move_size>=max){ //跨层后移++++++++++++
				int over_flow=start_seq+1+move_size-max;
				if(start_level==max_level){ //需要创建新的层
					max_level++;cur_files_last=0;
					path tpath=root_dir;//防止更改root_dir
					string t="level"+to_string(max_level);
					tpath/=t;
					if(!exists(tpath)){
						if(!create_directory(tpath))
						cout<<"create directory failed!";
					}
					cur_files_last+=over_flow;
				}	
				for(int i=over_flow-1;i>=0;i--){
					string new_name=root_dir+"/level"+to_string(start_level+1)+"/s-"+to_string(i);
					string old_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(start_seq--);
					if(rename(old_name.c_str(),new_name.c_str())){ //命名/移动失败
						cout<<"move file failed in recent level!"<<endl;
					}
				}
			}
			else if(start_level==max_level)cur_files_last+=move_size;
			for( start_seq;start_seq>=min_seq;start_seq--){
					string new_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(start_seq+move_size);
					string old_name=root_dir+"/level"+to_string(start_level)+"/s-"+to_string(start_seq);
					if(rename(old_name.c_str(),new_name.c_str())){ //命名/移动失败
						cout<<"move file failed in recent level!"<<endl;
					}
			}*/
	}
	// 将缓存区文件移动归位
	int start_seq=file_end.second+1;//从溢出的下一个文件开始算起
		int start_level=file_end.first;
	for(int i=0;i<move_size;i++){
		string old_name=root_dir+"/level-1/s-"+to_string(i);//在缓冲区的文件名
		RW_file new_file=calculate_new(start_level,start_seq,i);
		string new_name=root_dir+"/level"+to_string(new_file.first)+"/s-"+to_string(new_file.second);
		if(rename(old_name.c_str(),new_name.c_str())){ //命名/移动失败
				cout<<"move file failed in recent level!"<<endl;
			}
	}
	
}


RW_file SStable::calculate_old(int level,int seq,int movestep){
	RW_file old_file(level,seq+movestep);
	while(true){
		if(old_file.first==max_level)break;//最大层了，不管seq最大限制，方便比较
		int max=pow(2,old_file.first+1)-1;
		if(old_file.second>max){
			old_file.first++;
			old_file.second-=max+1;
		}
		break;
	}
	return old_file; 
}

RW_file SStable::calculate_new(int level,int seq,int movestep){
	RW_file new_file(level,seq+movestep);
	while(true){
		int max=pow(2,new_file.first+1)-1;
		if(new_file.second>max){
			new_file.first++;
			new_file.second-=max+1;
		}
		else break;
	}
	return new_file;
}