#include <string>     // std::string, std::stoi

#include <parquet-file/parquet-file.h>
#include <glog/logging.h>

using std::stoi;

using parquet_file::ParquetColumn;
using parquet_file::ParquetFile;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("Specify filename and number of items:\n\n\t%s <output file> <NNN>\n\n", argv[0]);
    return 1;
  }
  google::InitGoogleLogging(argv[0]);

  ParquetFile output(argv[1]);
  int NN = stoi(argv[2], NULL, 0);

  ParquetColumn* one_column =
    new ParquetColumn({"AllInts"}, parquet::Type::INT32,
          1, 1,
          FieldRepetitionType::REQUIRED,
          Encoding::PLAIN,
          CompressionCodec::UNCOMPRESSED);

  ParquetColumn* two_column =
    new ParquetColumn({"AllInts1"}, parquet::Type::INT32,
          1, 1,
          FieldRepetitionType::REQUIRED,
          Encoding::PLAIN,
          CompressionCodec::UNCOMPRESSED);

  ParquetColumn* root_column =
    new ParquetColumn({"root"}, parquet::Type::INT32,
          1, 1,
          FieldRepetitionType::REQUIRED,
          Encoding::PLAIN,
          CompressionCodec::UNCOMPRESSED);
  root_column->SetChildren({one_column, two_column});
  output.SetSchema(root_column);

  uint32_t data[NN];
  for (int i = 0; i < NN; ++i) {
    data[i] = i;
  }
  one_column->AddRecords(data, 0, NN);
  for (int i = 0; i < NN; ++i) {
    data[i] = i;
  }
  two_column->AddRecords(data, 0, NN);
  output.Flush();
}
