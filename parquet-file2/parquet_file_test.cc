//
// Parquet File Test
//
// Copyright (c) 2015 Apsalar Inc.
// All rights reserved.
//

#include <algorithm>
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <parquet-file2/parquet_file.h>

using namespace std;

using parquet_file2::ParquetFile;

namespace parquet_file2 {

// The fixture for testing class ParquetFile.
class ParquetFileTest : public ::testing::Test {
 protected:
  ParquetFileTest() {
    snprintf(template_, sizeof(template_), "/tmp/parquetFileTmp.XXXXXX");
    char* parquet_dump_path = getenv("PARQUET_DUMP_PATH");
    if (parquet_dump_path) {
      parquet_dump_executable_path_.assign(parquet_dump_path);
      VLOG(2) << "Using " << parquet_dump_executable_path_ << " to validate files";
    }
  }

  virtual void SetUp() {
    output_filename_.assign(mktemp(template_));
    VLOG(2) << "Assigning filename: " << output_filename_;
  }

  virtual void TearDown() {
    if (!parquet_dump_executable_path_.empty()) {
      const ::testing::TestInfo* const test_info =
          ::testing::UnitTest::GetInstance()->current_test_info();
      char golden_filename[1024];
      snprintf(golden_filename, 1024, "%s-golden", test_info->name());
      string golden_filename_no_slashes(golden_filename);
      std::replace(golden_filename_no_slashes.begin(), golden_filename_no_slashes.end(), '/','-');
      char command_line[1024];
      snprintf(command_line, 1024, "%s %s > %s", parquet_dump_executable_path_.c_str(), output_filename_.c_str(), golden_filename_no_slashes.c_str());
      system(command_line);
    }

  }

  // Objects declared here can be used by all tests in the test case for Foo.
  string output_filename_;
  char template_[32];
  string parquet_dump_executable_path_;
};

}  // end namespace parquet_file2

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
