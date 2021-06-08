cmake_minimum_required(VERSION 3.19)
project(LSMTree)

set(CMAKE_CXX_STANDARD 14)

file(GLOB SOURCES
        Header/utils.h
        Header/kvstore_api.h
        Header/MurmurHash3.h

        Header/kvstore.h
        Source/kvstore.cpp

        Header/SkipList.h
        Source/SkipList.cpp

        Source/CompactionNode.cpp
        Header/CompactionNode.h

        Source/SSTable.cpp
        Header/SSTable.h

        Source/BloomFilter.cpp
        Header/BloomFilter.h

        Source/Pair.cpp
        Header/Pair.h

        Source/kvstore.cpp
        Source/CacheList.cpp
        Header/CacheList.h
#===========================Test==========================#
        Test/test.h
#        Test/main.cpp
#        Test/correctness.cc
#        Test/persistence.cc
#        Test/ts_test.cpp
#        Test/latency.cpp
        Test/latency2.cpp
#        Test/throughput.cpp
        )

add_executable(LSMTree ${SOURCE_FILES} ${SOURCES})
