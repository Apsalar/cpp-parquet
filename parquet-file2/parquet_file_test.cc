//
// Parquet File Test
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#include <algorithm>
#include <memory>
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <parquet-file2/parquet_file.h>

using namespace std;
using namespace parquet;

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
    m_output_filename.assign(mktemp(template_));
    VLOG(2) << "Assigning filename: " << m_output_filename;
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
      snprintf(command_line, 1024, "%s %s > %s", parquet_dump_executable_path_.c_str(), m_output_filename.c_str(), golden_filename_no_slashes.c_str());
      system(command_line);
    }

  }

  // Objects declared here can be used by all tests in the test case for Foo.
  string m_output_filename;
  char template_[32];
  string parquet_dump_executable_path_;
};

// Test that we can add a column and some data.
TEST_F(ParquetFileTest, AddColumn) {
    ParquetFile pqfile(m_output_filename);

    ParquetColumnHandle pqh0 =
        make_shared<ParquetColumn>(StringSeq({"root"}),
                                   Type::INT32,	// ignored
                                   ConvertedType::INT_32,
                                   1, 1,
                                   FieldRepetitionType::REQUIRED,
                                   Encoding::PLAIN,
                                   CompressionCodec::UNCOMPRESSED);

    ParquetColumnHandle pqh1 =
        make_shared<ParquetColumn>(StringSeq({"root", "AllInts"}),
                                   Type::INT32,
                                   ConvertedType::INT_32,
                                   1, 1,
                                   FieldRepetitionType::REQUIRED,
                                   Encoding::PLAIN,
                                   CompressionCodec::UNCOMPRESSED);
    pqh0->add_child(pqh1);

    ParquetColumnHandle pqh2 =
        make_shared<ParquetColumn>(StringSeq({"root", "AllInts1"}),
                                   Type::INT32,
                                   ConvertedType::INT_32,
                                   1, 1,
                                   FieldRepetitionType::REQUIRED,
                                   Encoding::PLAIN,
                                   CompressionCodec::UNCOMPRESSED);
    pqh0->add_child(pqh2);

    pqfile.set_root(pqh0);

    int nrecs = 100;
    
    for (int32_t ii = 0; ii < nrecs; ++ii) {
        pqh1->add_datum(&ii, sizeof(ii), 0, 0);
        pqh2->add_datum(&ii, sizeof(ii), 0, 0);
    }

    CHECK_EQ(pqh1->num_records(), nrecs) << "num_records incorrect";

    pqfile.flush();
}

}  // end namespace parquet_file2

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
