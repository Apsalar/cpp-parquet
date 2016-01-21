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

#include "parquet-file/util/rle-encoding.h"

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

    void add_fixed_datum(void const * i_ptr, size_t i_size,
                         int i_replvl, int i_deflvl);

    void add_varlen_datum(void const * i_ptr, size_t i_size,
                          int i_replvl, int i_deflvl);

    std::string name() const;

    parquet::Type::type data_type() const;

    parquet::ConvertedType::type converted_type() const;

    parquet::FieldRepetitionType::type repetition_type() const;
    
    std::string path_string() const;

    ParquetColumnSeq const & children() const;

    bool is_leaf() const;
    
    size_t num_rowgrp_records() const;

    size_t data_size() const;

    class Traverser
    {
    public:
        virtual void operator()(ParquetColumnHandle const & ch) = 0;
    };

    void traverse(Traverser & tt);

    parquet::ColumnMetaData flush_row_group(int fd,
                    apache::thrift::protocol::TCompactProtocol * protocol);

    parquet::SchemaElement schema_element() const;

private:
    static size_t const PAGE_SIZE = 64 * 1024;

    struct DataPage
    {
        parquet::PageHeader	m_page_header;
        OctetSeq			m_page_data;

        size_t flush(int fd,
                     apache::thrift::protocol::TCompactProtocol* protocol);
    };
    typedef std::shared_ptr<DataPage> DataPageHandle;
    typedef std::deque<DataPageHandle> DataPageSeq;

    void add_levels(size_t i_size, int i_replvl, int i_deflvl);

    void push_page();
    
    void concatenate_page_data(OctetSeq & buffer);

    void reset_page_state();
    
    void reset_row_group_state();

    StringSeq m_path;
    int m_maxreplvl;
    int m_maxdeflvl;
    parquet::Type::type m_data_type;
    parquet::ConvertedType::type m_converted_type;
    parquet::FieldRepetitionType::type m_repetition_type;
    parquet::Encoding::type m_encoding;
    parquet::CompressionCodec::type m_compression_codec;

    ParquetColumnSeq m_children;

    // Page accumulation
    OctetBuffer m_data;
    size_t m_num_page_values;
    impala::RleEncoder m_rep_enc;
    impala::RleEncoder m_def_enc;
    uint8_t m_rep_buf[PAGE_SIZE];
    uint8_t m_def_buf[PAGE_SIZE];
    OctetSeq m_concat_buffer;
    
    // Row-Group accumulation
    DataPageSeq m_pages;
    size_t m_num_rowgrp_recs;
    size_t m_num_rowgrp_values;
    off_t m_column_write_offset;
    size_t m_uncompressed_size;
    size_t m_compressed_size;
};

} // end namespace parquet_file2

// Local Variables:
// mode: C++
// End:
