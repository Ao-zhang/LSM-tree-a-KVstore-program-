#include <iostream>
#include <cstdint>
#include <string>
#include "test.h"
//windows测试用，不测试可以注释掉
#include<chrono>  
using namespace std::chrono;

class CorrectnessTest : public Test {
private:
	const uint64_t SIMPLE_TEST_MAX = 1024;
	const uint64_t LARGE_TEST_MAX =1024*64; 

	void regular_test(uint64_t max)
	{
		uint64_t i;
		
		store.reset();
		 //Test a single key
		EXPECT(not_found, store.get(1));
		store.put(1, "SE");
		EXPECT("SE", store.get(1));
		EXPECT(true, store.del(1));
		EXPECT(not_found, store.get(1));
		EXPECT(false, store.del(1));
		
		phase();

		auto time_start=steady_clock::now();
		for (i = 0; i < max; ++i) {
			/*if(i%2048==0){
				auto time_over=steady_clock::now();
				auto run_time=duration_cast<microseconds>(time_over - time_start).count();
        		cout<<"put  "<<i<<" kvdatas cost :"<<run_time<<" us"<<endl;
				time_start=steady_clock::now();
			}*/
			if(i%1000==0)
			cout<<" put "<<i<<endl;
			store.put(i, std::string(i+1, 's'));
			EXPECT(std::string(i+1, 's'), store.get(i));
		}
	/*	 auto time_over=steady_clock::now();
	     auto run_time=duration_cast<microseconds>(time_over - time_start).count();
        cout<<"put  "<<i<<" kvdatas cost :"<<run_time<<" us"<<endl;*/
		phase();


		//time_start=steady_clock::now();
		 //Test after all insertions
		for (i = 0; i < max; ++i){
			if(i%1000==0)
			cout<<" get "<<i<<endl;
			EXPECT(std::string(i+1, 's'), store.get(i));
		}
		//time_over=steady_clock::now();
	    //run_time=duration_cast<microseconds>(time_over - time_start).count();
		//乘以1000000把单位由秒化为微秒，精度为1000 000/（cpu主频）微秒
        //cout<<"get  "<<max<<" kvdatas cost :"<<run_time<<" us"<<endl;
		phase();

		//time_start=steady_clock::now();
		 //Test deletions
		for (i = 0; i < max; i+=2){
			if(i%1000==0)
			cout<<" del "<<i<<endl;
			EXPECT(true, store.del(i));
		}
		//time_over=steady_clock::now();
	   // run_time=duration_cast<microseconds>(time_over - time_start).count();
		//乘以1000000把单位由秒化为微秒，精度为1000 000/（cpu主频）微秒
        //cout<<"del  "<<max/2<<" kvdatas cost :"<<run_time<<" us"<<endl;	
		phase();

		//time_start=steady_clock::now();
		for (i = 0; i < max; ++i){
			if(i%1000==0)
			cout<<" get "<<i<<endl;
			EXPECT((i & 1) ? std::string(i+1, 's') : not_found,
			       store.get(i));
		}
		//time_over=steady_clock::now();
	    //run_time=duration_cast<microseconds>(time_over - time_start).count();
		//cout<<"get  "<<max<<" kvdatas after del cost :"<<run_time<<" us"<<endl;

		phase();
		//time_start=steady_clock::now();
		for (i = 1; i < max; ++i){
			if(i%1000==0)
			cout<<" del "<<i<<endl;
			EXPECT(i & 1, store.del(i));
		}
		//time_over=steady_clock::now();
	    //run_time=duration_cast<microseconds>(time_over - time_start).count();
		//cout<<"del  "<<max-1<<" kvdatas after del cost :"<<run_time<<" us"<<endl;	

		phase();

		report();
	}

public:
	CorrectnessTest(const std::string &dir, bool v=true) : Test(dir, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Correctness Test" << std::endl;

		/*std::cout << "[Simple Test]" << std::endl;
		regular_test(SIMPLE_TEST_MAX);*/

		std::cout << "[Large Test]" << std::endl;
		regular_test(LARGE_TEST_MAX);
	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");
	//bool verbose=true;
	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF")<< "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	CorrectnessTest test("./data", verbose);

	test.start_test();
	
	return 0;
}
