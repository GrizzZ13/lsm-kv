#include <iostream>
#include <cstdint>
#include <string>
#include <cassert>
#include <ctime>
#include <fstream>
#include <algorithm>

#include "test.h"

class LatencyTest {
private:
    const uint64_t TEST_MAX = 1024 * 32;
    KVStore *store;

    std::vector<clock_t> put_latency;

    double put_ltc, del_ltc, get_ltc;
    clock_t put_acc, del_acc, get_acc;
    uint64_t put_cnt, del_cnt, get_cnt;
    clock_t beg, end;

    void put_plot(uint64_t key, uint64_t len){
        clock_t start, end;

        start = clock();
        store->put(key, std::string(len, 'f'));
        end = clock();
        double ops = end-start;
        put_latency.push_back(ops);
    }

    void test_plot(uint64_t fixed){
        uint64_t max = TEST_MAX * (TEST_MAX - 1) / 2 / fixed;
        std::vector<uint64_t> v;
        for (uint64_t i = 0; i < max; i++) {
            v.push_back(i);
        }
        random_shuffle(v.begin(), v.end());
        put_latency.clear();
        for (uint64_t i = 0; i < max; ++i) {
            put_plot(v[i], fixed);
        }

        std::string fp_path = "../plot.csv";
        FILE *fp=fopen(fp_path.c_str(),"w");
        for (uint64_t i = 0; i < max; ++i) {
            fprintf(fp,"%lu,%lu\n", i, put_latency[i]);
        }
        fclose(fp);
    }

    void test_gap(){
        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i+=2) {
            store->put(i, std::string((i+1)*2, 'f'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = TEST_MAX/2;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i+=2) {
            store->get(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = TEST_MAX/2;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i+=2) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = TEST_MAX/2;
        del_ltc = ((double)del_acc)/del_cnt;

        report("gap test");
        store->reset();
    }

    void test_fixed_length(uint64_t fixed){
        uint64_t max = TEST_MAX * (TEST_MAX - 1) / 2 / fixed;

        ///////////////////////////////////////////////////////
        beg = clock();
        for (uint64_t i = 0; i < max; i++) {
            store->put(i, std::string(fixed, 'f'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = max;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < max; i++) {
            store->get(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = max;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < max; i++) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = max;
        del_ltc = ((double)del_acc)/del_cnt;
        //////////////////////////////////////////////////////

        report("fixed length " + std::to_string(fixed) + " bytes");
        store->reset();
    }

    void test_common_3(const std::vector<uint64_t> &v)
    {
        ////////////////////////////////////////////////////////

        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->put(v[i], std::string(8192, 's'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = TEST_MAX;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->get(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = TEST_MAX;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = TEST_MAX;
        del_ltc = ((double)del_acc)/del_cnt;

        /////////////////////////////////////////////////////////
        report("optimization-3 common ");
        store->reset();
    }

    void test_random_key()
    {
        std::vector<uint64_t> v;
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            v.push_back(i);
        }
        random_shuffle(v.begin(), v.end());

        //////////////////////////////////////////////////////////

        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            uint64_t key = v[i];
            store->put(key, std::string(i+1, 's'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = TEST_MAX;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            store->get(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = TEST_MAX;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = TEST_MAX;
        del_ltc = ((double)del_acc)/del_cnt;

        ///////////////////////////////////////////////////////////
        report("random key");
        store->reset();
    }

    void test_random_value()
    {
        std::vector<uint64_t> v;
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            v.push_back(i);
        }
        random_shuffle(v.begin(), v.end());

        //////////////////////////////////////////////////////////

        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            uint64_t val = v[i]+1;
            store->put(i, std::string(val, 's'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = TEST_MAX;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            store->get(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = TEST_MAX;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = TEST_MAX;
        del_ltc = ((double)del_acc)/del_cnt;

        ///////////////////////////////////////////////////////////
        report("random value");
        store->reset();
    }

    void test_amount(uint64_t length){
        beg = clock();
        for (uint64_t i = 0; i < 1024*16; i++) {
            store->put(i, std::string(length, 'f'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = 1024*16;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < 1024*16; i++) {
            store->get(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = 1024*16;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < 1024*16; i++) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = 1024*16;
        del_ltc = ((double)del_acc)/del_cnt;

        report("amount test :"+ std::to_string(length)+ " bytes per value");
        store->reset();
    }

    void test_common_1(const std::vector<uint64_t> &v)
    {
        ////////////////////////////////////////////////////////

        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->put(v[i], std::string(8192, 's'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = TEST_MAX;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->get_1(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = TEST_MAX;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = TEST_MAX;
        del_ltc = ((double)del_acc)/del_cnt;

        /////////////////////////////////////////////////////////
        report("optimization-1 common ");
        store->reset();
    }

    void test_common_2(const std::vector<uint64_t> &v)
    {
        ////////////////////////////////////////////////////////

        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->put(v[i], std::string(8192, 's'));
        }
        end = clock();
        put_acc = end-beg;
        put_cnt = TEST_MAX;
        put_ltc = ((double)put_acc)/put_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->get_2(i);
        }
        end = clock();
        get_acc = end-beg;
        get_cnt = TEST_MAX;
        get_ltc = ((double)get_acc)/get_cnt;


        beg = clock();
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            store->del(i);
        }
        end = clock();
        del_acc = end-beg;
        del_cnt = TEST_MAX;
        del_ltc = ((double)del_acc)/del_cnt;

        /////////////////////////////////////////////////////////
        report("optimization-2 common ");
        store->reset();
    }

    void report(const std::string& str){
        std::cout << str << " test:" << std::endl;
        printf("put latency:[%f]\tdel latency:[%f]\tget latency:[%f] (ms per operation)\n", put_ltc, del_ltc, get_ltc);
    }

    void test_cache(){
        std::vector<uint64_t> v;
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            v.push_back(i);
        }
        random_shuffle(v.begin(), v.end());

        test_common_3(v);
        test_common_2(v);
        test_common_1(v);
    }

public:
    LatencyTest(const std::string& dir)
    {
        store = new  KVStore(dir);
        store->reset();
    }

    ~LatencyTest(){
        delete store;
    }

    void start_test()
    {
//        test_common();
//        test_common_2();
//        test_common_1();
//        test_cache();

        test_gap();
        test_gap();
        for(uint64_t i = 1024*16;i < 1024*32;i+=1024*2){
            test_amount(i);
        }
//        test_common();
//        test_gap();
//        test_random_key();
//        test_random_value();
//        uint64_t fixed = 1024 * 2;
//        for (; fixed < 1024 * 10; fixed+=1024) {
//            test_fixed_length(fixed);
//        }
//
    }
};

int main()
{
    LatencyTest test("./data");
    test.start_test();
    return 0;
}
