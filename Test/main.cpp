#include <iostream>
#include <cstdint>
#include <string>
#include <cassert>

#include "test.h"

class PersistenceTest : public Test {
private:
    const uint64_t TEST_MAX = 1024 * 32;
    void prepare(uint64_t max)
    {
        uint64_t i;

        // Clean up
        store.reset();

        // Test multiple key-value pairs
        for (i = 0; i < max; ++i) {
            store.put(i, std::string(i+1, 's'));
            EXPECT(std::string(i+1, 's'), store.get(i));
        }
        phase();

        // Test after all insertions
        for (i = 0; i < max; ++i)
            EXPECT(std::string(i+1, 's'), store.get(i));
        phase();

        // Test deletions
        for (i = 0; i < max; i+=2)
            EXPECT(true, store.del(i));

        // Prepare data for Test Mode
        for (i = 0; i < max; ++i) {
            switch (i & 3) {
                case 0:
                    EXPECT(not_found, store.get(i));
                    store.put(i, std::string(i+1, 't'));
                    break;
                case 1:
                    EXPECT(std::string(i+1, 's'), store.get(i));
                    store.put(i, std::string(i+1, 't'));
                    break;
                case 2:
                    EXPECT(not_found, store.get(i));
                    break;
                case 3:
                    EXPECT(std::string(i+1, 's'), store.get(i));
                    break;
                default:
                    assert(0);
            }
        }

        phase();

        for (i = 0; i < max; ++i) {
            switch (i & 3) {
                case 0:
                    EXPECT(std::string(i+1, 't'), store.get(i));
                    break;
                case 1:
                    EXPECT(std::string(i+1, 't'), store.get(i));
                    break;
                case 2:
                    EXPECT(not_found, store.get(i));
                    break;
                case 3:
                    EXPECT(std::string(i+1, 's'), store.get(i));
                    break;
                default:
                    assert(0);
            }
        }

        phase();

        report();

    }

    void test(uint64_t max)
    {
        uint64_t i;
        // Test data
        for (i = 0; i < max; ++i) {
            switch (i & 3) {
                case 0:
                    EXPECT(std::string(i+1, 't'), store.get(i));
                    break;
                case 1:
                    EXPECT(std::string(i+1, 't'), store.get(i));
                    break;
                case 2:
                    EXPECT(not_found, store.get(i));
                    break;
                case 3:
                    EXPECT(std::string(i+1, 's'), store.get(i));
                    break;
                default:
                    assert(0);
            }
        }

        phase();

        report();
    }

public:
    PersistenceTest(const std::string &dir, bool v=true) : Test(dir, v)
    {
    }

    void start_test(void *args = NULL) override
    {
        bool testmode = (args && *static_cast<bool *>(args));

        std::cout << "KVStore Persistence Test" << std::endl;

        if (testmode) {
            std::cout << "<<Test Mode>>" << std::endl;
            test(TEST_MAX);
        } else {
            std::cout << "<<Preparation Mode>>" << std::endl;
            prepare(TEST_MAX);
        }
    }
};

void usage(const char *prog, const char *verb, const char *mode)
{
    std::cout << "Usage: " << prog  << " [-t] [-v]" << std::endl;
    std::cout << "  -t: test mode for persistence test,"
                 " if -t is not given, the program only prepares data for test."
                 " [currently " << mode << "]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << verb << "]" << std::endl;
    std::cout << std::endl;
    std::cout << " NOTE: A normal usage is as follows:" << std::endl;
    std::cout << "    1. invoke `" << prog << "`;" << std::endl;
    std::cout << "    2. terminate (kill) the program when data is ready;";
    std::cout << std::endl;
    std::cout << "    3. invoke `" << prog << "-t ` to test." << std::endl;
    std::cout << std::endl;
    std::cout.flush();
}


int main(int argc, char *argv[])
{
    bool verbose = false;
    bool testmode = false;

    PersistenceTest test("./data", verbose);

    test.start_test(static_cast<void *>(&testmode));

    return 0;
}
