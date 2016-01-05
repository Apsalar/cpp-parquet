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

void
ParquetFile::add_column(ParquetColumnHandle const & col)
{
    m_cols.push_back(col);
}

void
ParquetFile::flush()
{
    // Make sure we have the same number of records in all
    // columns.
    set<size_t> colnrecs;
    for (auto it = m_cols.begin(); it != m_cols.end(); ++it)
        colnrecs.insert((*it)->num_records());
    LOG_IF(FATAL, colnrecs.size() > 1)
        << "all columns must have the same number of record; "
        << "saw " << colnrecs.size() << " different sizes";
    size_t numrecs = *(colnrecs.begin());
    
    m_file_meta_data.__set_num_rows(numrecs);

    RowGroup row_group;
    row_group.__set_num_rows(numrecs);
    vector<ColumnChunk> column_chunks;
    for (auto it = m_cols.begin(); it != m_cols.end(); ++it) {
        auto column = *it;
        VLOG(2) << "Writing column: " << column->path_string();
        column->flush(m_fd, m_protocol.get());
        ColumnMetaData column_metadata = column->metadata();
        row_group.__set_total_byte_size(row_group.total_byte_size +
                                        column_metadata.total_uncompressed_size);
        VLOG(2) << "Wrote " << to_string(column_metadata.total_uncompressed_size)
                << " bytes for column: " << column->path_string();
        ColumnChunk column_chunk;
#if defined(FIXME)
        column_chunk.__set_file_path(m_path.c_str());
#else
        // Neal didn't initialize his file path ...
        column_chunk.__set_file_path("");
#endif
        column_chunk.__set_file_offset(column_metadata.data_page_offset);
        column_chunk.__set_meta_data(column_metadata);
        column_chunks.push_back(column_chunk);
    }
    VLOG(2) << "Total bytes for all columns: " << row_group.total_byte_size;
    row_group.__set_columns(column_chunks);

    m_file_meta_data.__set_row_groups({row_group});
    uint32_t file_metadata_length = m_file_meta_data.write(m_protocol.get());
    VLOG(2) << "File metadata length: " << file_metadata_length;
    write(m_fd, &file_metadata_length, sizeof(file_metadata_length));
    write(m_fd, PARQUET_MAGIC, strlen(PARQUET_MAGIC));
    VLOG(2) << "Done.";
    close(m_fd);
}

} // end namespace parquet_file2
