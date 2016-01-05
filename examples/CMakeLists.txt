ADD_EXECUTABLE(cppparquet parquet-file-driver.cc)
# Source files for main executable.
TARGET_LINK_LIBRARIES(cppparquet glog pthread libcppparquet parquet-thrift thrift)

ADD_EXECUTABLE(cppparquet2 parquet-file-driver2.cc)
# Source files for main executable.
TARGET_LINK_LIBRARIES(cppparquet2 glog pthread libcppparquet2 parquet-thrift thrift)

ADD_EXECUTABLE(avro-schema-walker-example avro-schema-walker-example.cc)
TARGET_LINK_LIBRARIES(avro-schema-walker-example glog pthread libcppparquet parquet-thrift thrift libavroschemawalker avrocpp)

ADD_EXECUTABLE(pb_sample pb_sample.cc pb_sample.pb.cc)
TARGET_LINK_LIBRARIES(pb_sample glog protobuf pthread)
