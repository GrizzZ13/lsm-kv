#include <iostream>
#include <cstdint>
#include <string>

#include "test.h"

class CorrectnessTest : public Test {
private:
    const uint64_t LARGE_TEST_MAX = 1024 * 32;

    void regular_test(uint64_t max)
    {
        uint64_t i;
        store.reset();

        // Test multiple key-value pairs
        for (i = 0; i < max; i++) {
            switch (i&3) {
                case 0:
                    store.put(i, std::string(i+1, 's'));
                    break;
                case 1:
                    store.put(i, std::string(i+1, 't'));
                    break;
                case 2:
                    store.put(i, std::string(i+1, 'y'));
                    break;
                case 3:
                    store.put(i, std::string(i+1, 'o'));
                    break;
            }
        }

        for (i = 0; i < max; i+=2)
            if(i==30288)
                store.del(i);
            else
                store.del(i);

//        for (i = 0; i < max; ++i) {
//            switch (i & 3) {
//                case 0:
//                    EXPECT(not_found, store.get(i));
//                    store.put(i, std::string(i+1, 't'));
//                    break;
//            }
//        }
        store.get(30288);
        phase();

//        // Test deletions
//        for (i = 0; i < max; i+=2)
//            EXPECT(true, store.del(i));
//
//        for (i = 0; i < max; ++i)
//            EXPECT((i & 1) ? std::string(i+1, 's') : not_found,
//                   store.get(i));
//
//        for (i = 1; i < max; ++i)
//            EXPECT(i & 1, store.del(i));
//
//        phase();

        report();
    }

public:
    CorrectnessTest(const std::string &dir, bool v=true) : Test(dir, v)
    {
    }

    void start_test(void *args = NULL) override
    {
        std::cout << "KVStore Correctness Test" << std::endl;

        std::cout << "[Large Test]" << std::endl;
        regular_test(LARGE_TEST_MAX);
    }
};

int main(int argc, char *argv[])
{
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF")<< "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    CorrectnessTest test("./data", verbose);

    test.start_test();

    return 0;
}
