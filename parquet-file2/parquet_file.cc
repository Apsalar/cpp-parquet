//
// Parquet File Writer
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#include <fcntl.h>

#include <set>

#include <glog/logging.h>

#include "parquet_file.h"

using namespace std;
using namespace parquet;

namespace parquet_file2 {

char const * PARQUET_MAGIC = "PAR1";

ParquetFile::ParquetFile(string const & i_path)
    : m_path(i_path)
    , m_num_rows(0)
{
    m_fd = open(i_path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0700);
    
    LOG_IF(FATAL, m_fd == -1)
        << "trouble creating file " << i_path.c_str()
        << ": " << strerror(errno);

    write(m_fd, PARQUET_MAGIC, strlen(PARQUET_MAGIC));

    m_file_transport.reset(new TFDTransport(m_fd));
    m_protocol.reset(new TCompactProtocol(m_file_transport));

    // Parquet-specific metadata for the file.
    m_file_meta_data.__set_version(1);
    // m_file_meta_data.__set_created_by("Apsalar");
    m_file_meta_data.__set_created_by("Neal sid");
}

class SchemaBuilder : public ParquetColumn::Traverser
{
public:
    virtual void operator()(ParquetColumnHandle const & ch) {
        m_schema.push_back(ch->schema_element());
    }
    vector<SchemaElement> m_schema;
};

class ColumnListing : public ParquetColumn::Traverser
{
public:
    virtual void operator()(ParquetColumnHandle const & ch) {
        m_cols.push_back(ch);
    }
    ParquetColumnSeq m_cols;
};

void
ParquetFile::set_root(ParquetColumnHandle const & col)
{
    m_root = col;

    SchemaBuilder sb;
    sb(m_root);
    m_root->traverse(sb);
    m_file_meta_data.__set_schema(sb.m_schema);

    // Enumerate the leaf columns
    ColumnListing cl;
    cl(m_root);
    m_root->traverse(cl);
    for (auto it = cl.m_cols.begin(); it != cl.m_cols.end(); ++it) {
        ParquetColumnHandle const & ch = *it;
        if (ch->is_leaf())
            m_leaf_cols.push_back(ch);
    }
}

void
ParquetFile::flush_row_group()
{
    // Make sure we have the same number of records in all leaf
    // columns.
    set<size_t> colnrecs;
    for (auto it = m_leaf_cols.begin(); it != m_leaf_cols.end(); ++it) {
        ParquetColumnHandle const & ch = *it;
            colnrecs.insert((*it)->num_records());
    }
    LOG_IF(FATAL, colnrecs.size() > 1)
        << "all leaf columns must have the same number of record; "
        << "saw " << colnrecs.size() << " different sizes";
    size_t numrecs = *(colnrecs.begin());

    m_num_rows += numrecs;
    
    RowGroup row_group;
    row_group.__set_num_rows(numrecs);
    vector<ColumnChunk> column_chunks;
    for (auto it = m_leaf_cols.begin(); it != m_leaf_cols.end(); ++it) {
        ParquetColumnHandle const & ch = *it;

        ch->flush(m_fd, m_protocol.get());
        ColumnMetaData column_metadata = ch->metadata();

        row_group.__set_total_byte_size
            (row_group.total_byte_size +
             column_metadata.total_uncompressed_size);

        ColumnChunk column_chunk;
        column_chunk.__set_file_path("");	// Neal didn't initialize
        column_chunk.__set_file_offset(column_metadata.data_page_offset);
        column_chunk.__set_meta_data(column_metadata);
        column_chunks.push_back(column_chunk);

        ch->reset_row_group_state();
    }
    row_group.__set_columns(column_chunks);

    m_row_groups.push_back(row_group);
}

void
ParquetFile::flush()
{
    flush_row_group();
    
    m_file_meta_data.__set_num_rows(m_num_rows);
    m_file_meta_data.__set_row_groups(m_row_groups);
    
    uint32_t file_metadata_length = m_file_meta_data.write(m_protocol.get());
    write(m_fd, &file_metadata_length, sizeof(file_metadata_length));
    write(m_fd, PARQUET_MAGIC, strlen(PARQUET_MAGIC));
    close(m_fd);
}

} // end namespace parquet_file2
