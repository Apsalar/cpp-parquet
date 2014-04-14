// Copyright 2014 Mount Sinai School of Medicine.

#include "./parquet-column.h"

#include <boost/algorithm/string/join.hpp>
#include <parquet-file/util/rle-encoding.h>
#include <thrift/protocol/TCompactProtocol.h>

using apache::thrift::protocol::TCompactProtocol;
using parquet::PageHeader;
using parquet::PageType;

namespace parquet_file {

ParquetColumn::ParquetColumn(const vector<string>& column_name,
                             parquet::Type::type data_type,
                             uint16_t column_level,
                             FieldRepetitionType::type repetition_type,
                             Encoding::type encoding,
                             CompressionCodec::type compression_codec)
  : column_name_(column_name),
    repetition_type_(repetition_type),
    column_level_(column_level),
    encoding_(encoding),
    data_type_(data_type),
    num_rows_(0),
    num_datums_(0),
    compression_codec_(compression_codec),
    // I'm purposely using the constructor parameter in the next line,
    // as opposed to data_type_, in order to be clear that I'm am
    // avoiding a dependency on the order of variable declarations in
    // the class.
    bytes_per_datum_(BytesForDataType(data_type)),
    data_ptr_(data_buffer_),
    column_write_offset_(-1L) {
}

const vector<ParquetColumn*>& ParquetColumn::Children() const {
  return children_;
}

FieldRepetitionType::type ParquetColumn::RepetitionType() const {
  return repetition_type_;
}

Encoding::type ParquetColumn::Encoding() const {
  return encoding_;
}

Type::type ParquetColumn::Type() const {
  return data_type_;
}

CompressionCodec::type ParquetColumn::CompressionCodec() const {
  return compression_codec_;
}

string ParquetColumn::FullSchemaPath() const {
  if (column_name_.size() > 0) {
    return boost::algorithm::join(column_name_, ".");
  } else {
    return "";
  }
}

string ParquetColumn::Name() const {
  if (column_name_.size() > 0) {
    return column_name_.back();
  } else {
    return "";
  }
}

string ParquetColumn::ToString() const {
  // TODO: there has got to be an alternative to BOOST that supports
  // sane string concatenation and formatting.
  return this->FullSchemaPath() + "/" +
    parquet::_FieldRepetitionType_VALUES_TO_NAMES.at(this->RepetitionType())
    + "/" + to_string(Children().size()) + " children"
    + "/" + parquet::_Type_VALUES_TO_NAMES.at(this->Type())
    + "/" + to_string(num_rows_) + " rows"
    + "/" + to_string(num_datums_) + " pieces of data"
    + "/" + to_string(bytes_per_datum_) + " bytes per datum";
}

void ParquetColumn::AddRows(void* buf, uint16_t repetition_level,
                            uint32_t n) {
  CHECK_LT(repetition_level, column_level_) <<
    "For adding repeated data as part of 1 record, using AddRepeatedData";
  // TODO: check for overflow of multiply
  size_t num_bytes = n * bytes_per_datum_;
  memcpy(data_buffer_, buf, n * bytes_per_datum_);
  data_ptr_ += num_bytes;
  num_rows_ += n;
  num_datums_ += n;
  for (int i = 0; i < n; ++i) {
    repetition_levels_.push_back(repetition_level);
    definition_levels_.push_back(column_level_);
  }
}

// Adds repeated data to this column.  All data is considered part
// of the same record.
void ParquetColumn::AddRepeatedData(void *buf,
                                    uint16_t current_repetition_level,
                                    uint32_t n) {
  LOG_IF(FATAL, RepetitionType() != FieldRepetitionType::REPEATED) <<
    "Cannot add repeated data to a non-repeated column: " << FullSchemaPath();
  size_t num_bytes = n * bytes_per_datum_;
  memcpy(data_buffer_, buf, n * bytes_per_datum_);
  data_ptr_ += num_bytes;
  repetition_levels_.push_back(current_repetition_level);
  definition_levels_.push_back(column_level_);
  for (int i = 1; i < n; ++i) {
    repetition_levels_.push_back(column_level_);
    definition_levels_.push_back(column_level_);
  }
  num_rows_ += 1;
  num_datums_ += n;
}

uint32_t ParquetColumn::NumRows() const {
  return num_rows_;
}

uint32_t ParquetColumn::NumDatums() const {
  return num_datums_;
}

// static
uint8_t ParquetColumn::BytesForDataType(Type::type dataType) {
  // TODO support boolean (which is 1 bit)
  switch (dataType) {
  case Type::INT32:
  case Type::FLOAT:
    return 4;
  case Type::INT64:
  case Type::DOUBLE:
    return 8;
  case Type::INT96:
    return 12;
  case Type::BYTE_ARRAY:
  case Type::BOOLEAN:
  default:
    assert(0);
  }
}

void ParquetColumn::SetChildren(const vector<ParquetColumn*>& children) {
  if (children_.size() > 0) {
    LOG(WARNING) << "Clearing pre-existing children in column: " << ToString();
    // NB The memory ownership semantics of children column pointers
    // needs to be worked out, but I know in this code path there is a
    // memory leak so I will just call delete here.
    for (auto c : children_) {
      delete c;
    }
  }
  children_.assign(children.begin(), children.end());
}

void ParquetColumn::AddChild(ParquetColumn* child) {
  children_.push_back(child);
}

void ParquetColumn::Flush(int fd, TCompactProtocol* protocol) {
  LOG_IF(FATAL, Encoding() != Encoding::PLAIN)
    << "Encoding can only be plain at this time.";
  LOG_IF(FATAL, CompressionCodec() != CompressionCodec::UNCOMPRESSED)
    << "Compression is not supported at this time.";
  LOG_IF(FATAL, RepetitionType() == FieldRepetitionType::OPTIONAL)
    << "Optional fields are not supported at this time.";

  column_write_offset_ = lseek(fd, 0, SEEK_CUR);
  uint32_t repetition_level_size = 0, definition_level_size = 0;
  VLOG(2) << "Inside flush for " << FullSchemaPath();
  VLOG(2) << "\tData size: " << BytesForDataType(data_type_) * num_datums_
          << " bytes.";
  VLOG(2) << "\tNumber of rows: " << NumRows();
  VLOG(2) << "\tFile offset: " << column_write_offset_;

  uint8_t encoded_repetition_levels[1024], encoded_definition_levels[1024];
  if (RepetitionType() == FieldRepetitionType::REPEATED) {
    VLOG(2) << "\tNon-required field, encoding R&D levels";

    impala::RleEncoder rep_encoder(encoded_repetition_levels, 1024,
                                   column_level_);
    CHECK_GE(1024, rep_encoder.MaxBufferSize(repetition_levels_.size()))
      << "Hardcoded buffer for 1k repetition levels is not big enough";
    VLOG(2) << "\tRepetition levels size: " << repetition_levels_.size();
    VLOG(3) << "\tRepetition Level dump (" << definition_levels_.size() << " elements)";
    for (uint8_t rep_level : repetition_levels_) {
      VLOG(3) << "\t\t" << to_string(rep_level);
      rep_encoder.Put(rep_level);
    }
    rep_encoder.Flush();
    repetition_level_size = rep_encoder.len();
    VLOG(2) << "\tRepetition levels occupy " << repetition_level_size
            << " bytes encoded";
    VLOG(2) << "\tRepetition level bitstream: " << std::bitset<8>(encoded_repetition_levels[0]) << std::bitset<8>(encoded_repetition_levels[1]);
    impala::RleEncoder def_encoder(encoded_definition_levels, 1024,
                                   column_level_);
    CHECK_GE(1024, def_encoder.MaxBufferSize(definition_levels_.size()))
      << "Hardcoded buffer for 1k definition levels is not big enough";
    VLOG(3) << "\tDefinition level dump (" << definition_levels_.size() << " elements)";
    for (auto def_level : definition_levels_) {
      VLOG(3) << "\t\t" << to_string(def_level);
      def_encoder.Put(def_level);
    }
    def_encoder.Flush();
    definition_level_size = def_encoder.len();
    VLOG(2) << "\tDefinition levels occupy " << definition_level_size
            << " bytes encoded";
    VLOG(2) << "\tDefinition level bitstream: " << std::bitset<8>(encoded_definition_levels[0]) << std::bitset<8>(encoded_definition_levels[1]);
  }

  PageHeader page_header;
  page_header.__set_type(PageType::DATA_PAGE);
  // We add 8 to this for the two ints at that indicate the length of
  // the rep & def levels.
  uncompressed_bytes_ = BytesForDataType(data_type_) * NumDatums()
    + repetition_level_size + definition_level_size + 8;
  page_header.__set_uncompressed_page_size(uncompressed_bytes_);
  // Obviously, this is a stop gap until compression support is added.
  page_header.__set_compressed_page_size(uncompressed_bytes_);

  DataPageHeader data_header;
  data_header.__set_num_values(NumDatums());
  data_header.__set_encoding(Encoding::PLAIN);
  // NB: For some reason, the following two must be set, even though
  // they can default to PLAIN, even for required/nonrepeating fields.
  // I'm not sure if it's part of the Parquet spec or a bug in
  // parquet-dump.
  data_header.__set_definition_level_encoding(Encoding::RLE);
  data_header.__set_repetition_level_encoding(Encoding::RLE);
  page_header.__set_data_page_header(data_header);
  uint32_t page_header_size = page_header.write(protocol);
  uncompressed_bytes_ += page_header_size;
  VLOG(2) << "\tPage header size: " << page_header_size;
  VLOG(2) << "\tTotal uncompressed bytes: " << uncompressed_bytes_;

  if (repetition_level_size > 0) {
    write(fd, &repetition_level_size, 4);
    size_t bytes_written =
      write(fd, encoded_repetition_levels, repetition_level_size);
    if (bytes_written != repetition_level_size) {
      LOG(WARNING) << "Only " << bytes_written << " of "
                   << repetition_level_size
                   << " for repetition levels were written.";
    }
  }

  if (definition_level_size > 0) {
    write(fd, &definition_level_size, 4);
    size_t bytes_written =
      write(fd, encoded_definition_levels, definition_level_size);
    if (bytes_written != definition_level_size) {
      LOG(WARNING) << "Only " << bytes_written << " of "
                   << definition_level_size
                   << " for definition levels were written.";
    }
  }

  // We don't write definition levels, because, for now, fields are
  // required.

  for (int i = 0; i < NumDatums(); ++i) {
    LOG_IF(FATAL, data_buffer_ + (i * bytes_per_datum_) >= data_ptr_)
      << "Exceeded data added to internal buffer";
    ssize_t written = write(fd, data_buffer_ + i * bytes_per_datum_,
                            bytes_per_datum_);
    if (written != bytes_per_datum_) {
      LOG(FATAL) << "Did not write correct number of bytes for element %d\n"
                 << i;
    }
  }
  VLOG(2) << "\tFinal offset after write: " << lseek(fd, 0, SEEK_CUR);
}

ColumnMetaData ParquetColumn::ParquetColumnMetaData() const {
  ColumnMetaData column_metadata;
  column_metadata.__set_type(Type());
  column_metadata.__set_encodings({Encoding()});
  column_metadata.__set_path_in_schema(column_name_);
  column_metadata.__set_codec(CompressionCodec());
  column_metadata.__set_num_values(NumDatums());
  column_metadata.__set_total_uncompressed_size(uncompressed_bytes_);
  column_metadata.__set_total_compressed_size(uncompressed_bytes_);
  column_metadata.__set_data_page_offset(column_write_offset_);
  return column_metadata;
}
}  // namespace parquet_file
