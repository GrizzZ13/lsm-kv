```
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
    report("random value");
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
```