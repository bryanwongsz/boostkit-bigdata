/*
 * Copyrights (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_TEST_UTILS_H
#define SPARK_THESTRAL_PLUGIN_TEST_UTILS_H

#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include "../../src/shuffle/splitter.h"
#include "../../src/jni/concurrent_mao.h"

static ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;

static std::string s_shuffle_tests_dir = "/tmp/shuffleTests";

VectorBatch* CreateInputData(const int32_t numRows, const int32_t numCols, int32_t* inputTypeIds, int64_t* allData);

Vector *buildVector(const DataType &aggType, int32_t rowNumber);

VectorBatch* createVectorBatch_1row_varchar_withPid(int pid, std::string inputChar);

VectorBatch* createVectorBatch_4col_withPid(int parNum, int rowNum);

VectorBatch* createVectorBatch_1longCol_withPid(int parNum, int rowNum);

VectorBatch* createVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar);

VectorBatch* createVectorBatch_4varcharCols_withPid(int parNum, int rowNum);

VectorBatch* createVectorBatch_4charCols_withPid(int parNum, int rowNum);

VectorBatch* createVectorBatch_3fixedCols_withPid(int parNum, int rowNum);

VectorBatch* createVectorBatch_2dictionaryCols_withPid(int partitionNum);

VectorBatch* createVectorBatch_1decimal128Col_withPid(int partitionNum, int rowNum);

VectorBatch* createVectorBatch_1decimal64Col_withPid(int partitionNum, int rowNum);

VectorBatch* createVectorBatch_2decimalCol_withPid(int partitionNum, int rowNum);
VectorBatch* createVectorBatch_someNullRow_vectorBatch();

VectorBatch* CreateVectorBatch_someNullCol_vectorBatch();

void Test_Shuffle_Compression(std::string comPtr, int32_t numPartition, int32_t numVb, int32_t numRow);

long Test_splitter_nativeMake(std::string partitioning_name,
                              int num_numPartitions,
                              InputDataTypes inputDataTypes,
                              int numCols,
                              int buffer_size,
                              const char* compression_type_jstr,
                              std::string data_file_jstr,
                              int num_sub_dirs,
                              std::string local_dirs_jstr);

int Test_splitter_split(long splitter_id, VectorBatch* vb);

void Test_splitter_stop(long splitter_id);

void Test_splitter_close(long splitter_id);

template <typename T, typename V> T *CreateVector(V *values, int32_t length)
{
    VectorAllocator *vecAllocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    auto vvector = new T(vecAllocator, length);
    vector->SetValues(0, values, length);
    return vector;
}

void GetFilePath(const char *path, const char *filename, char *filepath);

void DeletePathAll(const char* path);

##endif //SPARK_THESTRAL_PLUGIN_TEST_UTILS_H