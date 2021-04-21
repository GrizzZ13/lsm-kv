cmake_minimum_required(VERSION 3.19)
project(LSMTree)

set(CMAKE_CXX_STANDARD 14)

file(GLOB SOURCES
        Header/utils.h
        Header/kvstore_api.h
        Header/MurmurHash3.h

        Header/kvstore.h
        Source/kvstore.cc

        Header/SkipList.h
        Source/SkipList.cpp

#===========================Test==========================#
        Test/test.h
        Test/correctness.cc
#        Test/persistence.cc
        )

add_executable(LSMTree Test/correctness.cc ${SOURCE_FILES} ${SOURCES} Source/SSTable.cpp Header/SSTable.h Source/BloomFilter.cpp Header/BloomFilter.h Source/Pair.cpp Header/Pair.h)
