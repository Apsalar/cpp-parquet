//
// Parquet Column Writer
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <deque>
#include <fstream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include <thrift/protocol/TCompactProtocol.h>

#include "parquet_types.h"

namespace parquet_file2 {

typedef std::vector<std::string> StringSeq;
typedef std::vector<uint8_t> OctetSeq;
typedef std::deque<uint8_t> OctetBuffer;


class ParquetColumn;
typedef std::shared_ptr<ParquetColumn> ParquetColumnHandle;
typedef std::vector<ParquetColumnHandle> ParquetColumnSeq;

class ParquetColumn
{
public:
    ParquetColumn(StringSeq const & i_path,
                  parquet::Type::type i_data_type,
                  parquet::ConvertedType::type i_converted_type,
                  int i_maxreplvl,
                  int i_maxdeflvl,
                  parquet::FieldRepetitionType::type i_repetition_type,
                  parquet::Encoding::type i_encoding,
                  parquet::CompressionCodec::type i_compression_codec);

    void add_child(ParquetColumnHandle const & ch);

    void add_datum(void const * i_ptr, size_t i_size,
                   int i_replvl, int i_deflvl);

    std::string name() const;

    parquet::Type::type data_type() const;

    parquet::ConvertedType::type converted_type() const;

    parquet::FieldRepetitionType::type repetition_type() const;
    
    std::string path_string() const;

    ParquetColumnSeq const & children() const;

    bool is_leaf() const;
    
    size_t num_records() const;

    size_t num_datum() const;

    size_t data_size() const;

    class Traverser
    {
    public:
        virtual void operator()(ParquetColumnHandle const & ch) = 0;
    };

    void traverse(Traverser & tt);

    void flush(int fd, apache::thrift::protocol::TCompactProtocol* protocol);

    parquet::SchemaElement schema_element() const;
    
    parquet::ColumnMetaData metadata() const;

private:
    struct MetaData {
        size_t	m_beg;
        size_t	m_size;
        int		m_replvl;
        int		m_deflvl;

        MetaData(size_t i_beg, size_t i_size, int i_replvl, int i_deflvl)
            : m_beg(i_beg), m_size(i_size)
            , m_replvl(i_replvl), m_deflvl(i_deflvl)
        {}
    };
    typedef std::deque<MetaData> MetaDataSeq;
    
    OctetSeq encode_repetition_levels();

    OctetSeq encode_definition_levels();

    void flush_buffer(int fd, OctetBuffer const & i_buffer);

    void flush_seq(int fd, OctetSeq const & i_seq);

    StringSeq m_path;
    int m_maxreplvl;
    int m_maxdeflvl;
    parquet::Type::type m_data_type;
    parquet::ConvertedType::type m_converted_type;
    parquet::FieldRepetitionType::type m_repetition_type;
    parquet::Encoding::type m_encoding;
    parquet::CompressionCodec::type m_compression_codec;

    ParquetColumnSeq m_children;
    
    OctetBuffer m_data;
    MetaDataSeq m_meta;

    size_t m_numrecs;
    off_t m_column_write_offset;
    size_t m_uncompressed_size;
};

} // end namespace parquet_file2

// Local Variables:
// mode: C++
// End:
