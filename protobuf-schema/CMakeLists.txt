#
# Build CMakeFile for libprotobufschemawalker

ADD_LIBRARY(libprotobufschemawalker protobuf-schema-walker.cc)

ADD_EXECUTABLE(protobuf-parquet-encoder protobuf-parquet-encoder.cc)
TARGET_LINK_LIBRARIES (protobuf-parquet-encoder libprotobufschemawalker glog libcppparquet libprotobufschemawalker protobuf parquet-thrift thrift pthread)
