#
# Build CMakeFile for libprotobufschemawalker

ADD_LIBRARY(libprotobufschemawalker protobuf-schema-walker.cc)

ADD_EXECUTABLE(proto2parq proto2parq.cc)
TARGET_LINK_LIBRARIES (proto2parq libprotobufschemawalker glog libcppparquet2 libprotobufschemawalker protobuf parquet-thrift thrift snappy z pthread ${OPENSSL_LIBRARIES})
