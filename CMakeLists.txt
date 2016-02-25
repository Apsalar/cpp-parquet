#
# Build CMakeFile for cpp-parquet

CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)
PROJECT(cpp-parquet-proj)

# Set to use C++11

ADD_DEFINITIONS(-std=c++11)
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")


# External projects for parquet-format and thrift
INCLUDE(ExternalProject)
INCLUDE(ParquetFormatCMakeLists)
INCLUDE(GoogleGlogCMakeLists)
INCLUDE(GoogleTestCMakeLists)
# INCLUDE(SnappyCMakeLists)
INCLUDE(CheckIncludeFileCXX)

FIND_PACKAGE(Thrift REQUIRED)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR})
INCLUDE_DIRECTORIES(${THRIFT_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${OPENSSL_INCLUDE_DIRS})
SET(LIBS ${LIBS} ${THRIFT_LIBRARIES} ${OPENSSL_LIBRARIES})

ExternalProject_Get_Property(google-glog install_dir)
include_directories (${install_dir}/include)

ADD_SUBDIRECTORY(parquet-file)
ADD_SUBDIRECTORY(parquet-file2)
# ADD_SUBDIRECTORY(avro-schema)
ADD_SUBDIRECTORY(protobuf-schema)
ADD_SUBDIRECTORY(examples)
