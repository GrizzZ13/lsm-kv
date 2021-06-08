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
    // count
    uint64_t put_cnt, del_cnt, get_cnt;
    // accumulation
    clock_t put_acc, del_acc, get_acc;

    std::vector<clock_t> put_latency;

    void put(uint64_t key, const std::string &s){
        clock_t start, end;

        start = clock();
        store->put(key, s);
        end = clock();

        put_acc += (end - start);
        put_cnt++;
    }

    void put_plot(uint64_t key, uint64_t len){
        clock_t start, end;

        start = clock();
        store->put(key, std::string(len, 'f'));
        end = clock();
        double ops = end-start;
        put_latency.push_back(ops);
    }


    void del(uint64_t key){
        clock_t start, end;

        start = clock();
        store->del(key);
        end = clock();

        del_acc += (end - start);
        del_cnt++;
    }

    void get(uint64_t key){
        clock_t start, end;

        start = clock();
        store->get(key);
        end = clock();

        get_acc += (end - start);
        get_cnt++;
    }

    void reTest(){
        store->reset();
        put_cnt = 0;
        del_cnt = 0;
        get_cnt = 0;
        put_acc = 0;
        del_acc = 0,
        get_acc = 0;
    }

    void reset(){
        put_cnt = 0;
        del_cnt = 0;
        get_cnt = 0;
        put_acc = 0;
        del_acc = 0,
        get_acc = 0;
    }

    void test_gap(){
        for (uint64_t i = 0; i < TEST_MAX; i+=2) {
            put(i, std::string((i+1)*2, 'f'));
        }
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            get(i);
        }
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            del(i);
        }
        report("gap");
        reTest();
    }

    void test_plot(uint64_t fixed){
        uint64_t max = TEST_MAX * (TEST_MAX - 1) / 2 / fixed;
        put_latency.clear();
        for (uint64_t i = 0; i < max; ++i) {
            put_plot(i, fixed);
        }

        std::string fp_path = "../plot.csv";
        FILE *fp=fopen(fp_path.c_str(),"w");
        for (uint64_t i = 0; i < max - 1; ++i) {
            fprintf(fp,"%lu,", i);
        }
        fprintf(fp,"%lu\n", max-1);

        for (uint64_t i = 0; i < max-1; ++i) {
            fprintf(fp,"%lu,", put_latency[i]);
        }
        fprintf(fp,"%lu", put_latency[max-1]);
        fclose(fp);
    }

    void test_fixed_length(uint64_t fixed){
        uint64_t max = TEST_MAX * (TEST_MAX - 1) / 2 / fixed;
        for (uint64_t i = 0; i < max; i++) {
            put(i, std::string(fixed, 'f'));
        }
        for (uint64_t i = 0; i < max; i++) {
            get(i);
        }
        for (uint64_t i = 0; i < max; i++) {
            del(i);
        }
        report("fixed length " + std::to_string(fixed) + " bytes");
        reTest();
    }

    void test_common()
    {
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            put(i, std::string(i+1, 's'));
        }
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            get(i);
        }
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            del(i);
        }
        report("common");
        reTest();
    }

    void test_random_key()
    {
        std::vector<uint64_t> v;
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            v.push_back(i);
        }
        random_shuffle(v.begin(), v.end());
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            uint64_t key = v[i];
            put(key, std::string(i+1, 's'));
        }
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            get(i);
        }
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            del(i);
        }
        report("random key");
        reTest();
    }

    void test_random_value()
    {
        std::vector<uint64_t> v;
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            v.push_back(i);
        }
        random_shuffle(v.begin(), v.end());
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            uint64_t val = v[i]+1;
            put(i, std::string(val, 's'));
        }
        for (uint64_t i = 0; i < TEST_MAX; i++) {
            put(i, std::string(i+1, 's'));
        }
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            get(i);
        }
        for (uint64_t i = 0; i < TEST_MAX ; i++) {
            del(i);
        }
        report("random value");
        reTest();
    }

    void report(const std::string& str){
        // latency
        double put_ltc, del_ltc, get_ltc;
        put_ltc = ((double)put_acc)/put_cnt;
        del_ltc = ((double)del_acc)/del_cnt;
        get_ltc = ((double)get_acc)/get_cnt;
        std::cout << str << " test:" << std::endl;
        printf("put latency:[%f]\tdel latency:[%f]\tget_latency[%f] (ms per operation)\n", put_ltc, del_ltc, get_ltc);
    }

public:
    LatencyTest(const std::string& dir)
    {
        store = new  KVStore(dir);
        store->reset();
        put_cnt = 0;
        del_cnt = 0;
        get_cnt = 0;
        put_acc = 0;
        del_acc = 0,
        get_acc = 0;
    }

    ~LatencyTest(){
        delete store;
    }

    void start_test()
    {
//        test_common();
//        test_fixed_length(1024*8);
//        test_gap();
//        test_random_key();
//        test_random_value();
//        uint64_t fixed = 1024 * 2;
//        for (; fixed < 1024 * 10; fixed+=1024) {
//            test_fixed_length(fixed);
//        }
        test_plot(1024*16);
    }
};

int main()
{
    LatencyTest test("./data");
    test.start_test();
    return 0;
}
