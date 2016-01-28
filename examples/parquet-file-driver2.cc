#include <string>     // std::string, std::stoi

#include <glog/logging.h>

#include "parquet_types.h"

#include "parquet-file2/parquet_file.h"

using std::stoi;

using namespace parquet;
using namespace parquet_file2;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("Specify filename and number of items:\n\n\t%s <output file> <NNN>\n\n", argv[0]);
    return 1;
  }
  google::InitGoogleLogging(argv[0]);

  ParquetFile output(argv[1], 256);
  int NN = stoi(argv[2], NULL, 0);

  ParquetColumnHandle ch0
      (new ParquetColumn({"root"},
                         parquet::Type::INT32,
                         parquet::ConvertedType::INT_32,
                         1, 1,
                         FieldRepetitionType::REQUIRED,
                         Encoding::PLAIN,
                         CompressionCodec::UNCOMPRESSED));

  ParquetColumnHandle ch1
      (new ParquetColumn({"root", "AllInts"},
                         parquet::Type::INT32,
                         parquet::ConvertedType::INT_32,
                         1, 1,
                         FieldRepetitionType::REQUIRED,
                         Encoding::PLAIN,
                         CompressionCodec::UNCOMPRESSED));
  ch0->add_child(ch1);

  ParquetColumnHandle ch2
      (new ParquetColumn({"root", "AllInts1"},
                         parquet::Type::INT32,
                         parquet::ConvertedType::INT_32,
                         1, 1,
                         FieldRepetitionType::REQUIRED,
                         Encoding::PLAIN,
                         CompressionCodec::UNCOMPRESSED));
  ch0->add_child(ch2);

  output.set_root(ch0);

  for (int32_t ii = 0; ii < NN; ++ii) {
      ch1->add_datum(&ii, sizeof(ii), false, 0, 0);
      ch2->add_datum(&ii, sizeof(ii), false, 0, 0);
  }

  output.flush();
}
