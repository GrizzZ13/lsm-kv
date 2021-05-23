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
        Test/persistence.cc
        )

add_executable(LSMTree Test/persistence.cc ${SOURCE_FILES} ${SOURCES})
