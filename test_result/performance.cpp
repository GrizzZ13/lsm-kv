//#include "test.h"
//#include <ctime>
//
////performance test do not check correctness
//class PerformanceTest :public Test {
//private:
//	double put(uint64_t max, int inum)
//	{
//		uint64_t i;
//		//begin timing
//		clock_t begin = clock();
//		for (i = 0;i < max;++i)
//		{
//			store.put(i, std::string(inum, 's'));
//		}
//		clock_t end = clock();
//		double cost = end - begin;
//		return ((double)cost) / CLOCKS_PER_SEC;
//	}
//
//	double between_put(uint64_t max)
//	{
//		uint64_t i;
//		//begin timing
//		clock_t begin = clock();
//		for (i = 0;i < max;i+=2)
//		{
//			store.put(i, std::string(i, 's'));
//		}
//		clock_t end = clock();
//		double cost = end - begin;
//		return ((double)cost) / CLOCKS_PER_SEC;
//	}
//
//	double get(uint64_t max)
//	{
//		uint64_t i;
//		//begin timing
//		clock_t begin = clock();
//		for (i = 0;i < max;++i)
//		{
//			store.get(i);
//		}
//		clock_t end = clock();
//		double cost = end - begin;
//		return ((double)cost) / CLOCKS_PER_SEC;
//	}
//
//	double get_no_bloom(uint64_t max)
//	{
//		uint64_t i;
//		//begin timing
//		clock_t begin = clock();
//		for (i = 0;i < max;++i)
//		{
//			store.get_no_bloom(i);
//		}
//		clock_t end = clock();
//		double cost = end - begin;
//		return ((double)cost) / CLOCKS_PER_SEC;
//	}
//
//	double get_no_sst(uint64_t max)
//	{
//		uint64_t i;
//		//begin timing
//		clock_t begin = clock();
//		for (i = 0;i < max;++i)
//		{
//			store.get_no_sst(i);
//		}
//		clock_t end = clock();
//		double cost = end - begin;
//		return ((double)cost) / CLOCKS_PER_SEC;
//	}
//
//	double del(uint64_t max)
//	{
//		uint64_t i;
//		//begin timing
//		clock_t begin = clock();
//		for (i = 0;i < max;++i)
//		{
//			store.del(i);
//		}
//		clock_t end = clock();
//		double cost = end - begin;
//		return ((double)cost) / CLOCKS_PER_SEC;
//	}
//
//
//
//
//	void test()
//	{
//		double cost;
//		int value_size;
//		uint64_t max;
//
//		store.reset();
//		max = 1000;
//		value_size = 1;
//		//test get with no compaction
//		cost = put(max, value_size);
//
//		std::cout << "--------------------------------------" << std::endl;
//		std::cout << "all key size:" << 8 * max << " all value size:" << value_size * max << std::endl;
//		std::cout << "PUT NO COMPACT time:" << cost << " Operation:" << max << " Throughout:" << (double)max / (double)cost
//			<< " Latency:" << (double)cost / (double)max << std::endl;
//		std::cout << "--------------------------------------" << std::endl;
//
//		store.reset();
//		max = 1024 * 32;
//		value_size = 2000;
//		cost = put(max, value_size);
//		std::cout << "--------------------------------------" << std::endl;
//		std::cout << "all key size:" << 8 * max << " all value size:" << value_size * max << std::endl;
//		std::cout << "PUT time:" << cost << " Operation:" << max << " Throughout:" << (double)max / (double)cost
//			<< " Latency:" << (double)cost / (double)max << std::endl;
//		std::cout << "--------------------------------------" << std::endl;
//
//		cost = get(max);
//		std::cout << "--------------------------------------" << std::endl;
//		std::cout << "all key size:" << 8 * max << " all value size:" << value_size * max << std::endl;
//		std::cout << "GET time:" << cost << " Operation:" << max << " Throughout:" << (double)max / (double)cost
//			<< " Latency:" << (double)cost / (double)max << std::endl;
//		std::cout << "--------------------------------------" << std::endl;
//
//		cost = del(max);
//		std::cout << "--------------------------------------" << std::endl;
//		std::cout << "all key size:" << 8 * max << " all value size:" << value_size * max << std::endl;
//		std::cout << "DEL time:" << cost << " Operation:" << max << " Throughout:" << (double)max / (double)cost
//			<< " Latency:" << (double)cost / (double)max << std::endl;
//		std::cout << "--------------------------------------" << std::endl;
//	}
//
//	void compaction_test()
//	{
//		store.reset();
//
//		double cost;
//		int value_size;
//		uint64_t max=1024*50;
//		int turn = 40;
//		int i;
//		std::vector<double> throughout;
//
//		value_size = 10000;
//		int step = max / turn;
//
//		for (i = 0;i < turn;++i)
//		{
//			double cost = put(step, 100);
//			throughout.push_back(cost);
//			std::cout << cost << std::endl;
//		}
//
//		//compaction begin
//		for (i = 0;i < turn ;++i)
//		{
//			double cost = put(step, value_size);
//			throughout.push_back(cost);
//			std::cout << cost << std::endl;
//		}
//
//		//calculate accumulate time
//		for (auto it = throughout.begin();it + 1 != throughout.end();++it)
//		{
//			*(it + 1) += *it;
//		}
//
//		//calculate throughout put
//		int accum_step = step;
//		for (auto& it : throughout)
//		{
//			it = accum_step/it;
//			accum_step += step;
//		}
//
//		std::cout << "--------------------------------------" << std::endl;
//		std::cout << "throughout£º" << std::endl;
//		for (auto& it : throughout)
//		{
//			std::cout << it << std::endl;
//		}
//		std::cout << "--------------------------------------" << std::endl;
//	}
//
//	void get_test()
//	{
//		double cost;
//		int value_size;
//		uint64_t max;
//
//		//prepare data
//		max = 1024 * 32;
//		value_size = 10000;
//
//		int operation = max / 2;
//		//store.reset();
//		//cost = between_put(max);
//		//std::cout << "--------------------------------------" << std::endl;
//		//std::cout << "all key size:" << 8 * operation << " all value size:" << (1+max)*operation/2 << std::endl;
//		//std::cout << "put time:" << cost << " operation:" << operation << " throughout:" << (double)operation / (double)cost
//		//	<< " latency:" << (double)cost / (double)operation << std::endl;
//		//std::cout << "--------------------------------------" << std::endl;
//
//		//half in , half not in
//		operation = max*10;
//		int expect_get = max / 2;
//
//		cost = get(operation);
//		std::cout << "--------------------------------------" << std::endl;
//		std::cout << "GET time:" << cost << " Operation:" << operation << " Throughout:" << (double)operation / (double)cost
//			<< " Latency:" << (double)cost / (double)operation << std::endl;
//		std::cout << "Expect GET:" << expect_get << " Expect fail to GET:" << operation - expect_get << std::endl;
//		std::cout << "--------------------------------------" << std::endl;
//
//		cost = get_no_bloom(operation);
//		std::cout << "NO BLOOM FILTER--------------------------------------" << std::endl;
//		std::cout << "GET time:" << cost << " Operation:" << operation << " Throughout:" << (double)operation / (double)cost
//			<< " Latency:" << (double)cost / (double)operation << std::endl;
//		std::cout << "Expect GET:" << expect_get << " Expect fail to GET:" << operation - expect_get << std::endl;
//		std::cout << "--------------------------------------" << std::endl;
//
//		//cost = get_no_sst(operation);
//		//std::cout << "NO SSTable Cache--------------------------------------" << std::endl;
//		//std::cout << "GET time:" << cost << " Operation:" << operation << " Throughout:" << (double)operation / (double)cost
//		//	<< " Latency:" << (double)cost / (double)operation << std::endl;
//		//std::cout << "Expect GET:" << expect_get << " Expect fail to GET:" << operation - expect_get << std::endl;
//		//std::cout << "--------------------------------------" << std::endl;
//
//
//	}
//
//
//public:
//	PerformanceTest(const std::string& dir, bool v = true) : Test(dir, v)
//	{
//
//	}
//
//	void start_test(void* args = NULL) override
//	{
//		//test();
//		//compaction_test();
//		get_test();
//	}
//
//};
//
//int main()
//{
//	bool verbose = true;
//	PerformanceTest test("./data", verbose);
//	test.start_test();
//}