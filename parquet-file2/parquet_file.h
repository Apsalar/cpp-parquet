//
// Parquet File Writer
//
// Copyright (c) 2015 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TFDTransport.h>

#include "parquet_column.h"

using apache::thrift::transport::TFDTransport;
using apache::thrift::protocol::TCompactProtocol;

using parquet::FileMetaData;

namespace parquet_file2 {

typedef std::shared_ptr<ParquetColumn> ParquetColumnHandle;
typedef std::vector<ParquetColumnHandle> ParquetColumnSeq;

class ParquetFile
{
public:
    ParquetFile(std::string const & i_path);

    void add_column(ParquetColumnHandle const & col);

    void flush();

private:
    std::string m_path;
    int m_fd;
    FileMetaData m_file_meta_data;
    boost::shared_ptr<TFDTransport> m_file_transport;
    boost::shared_ptr<TCompactProtocol> m_protocol;
    ParquetColumnSeq m_cols;
};

} // end namespace parquet_file2

// Local Variables:
// mode: C++
// End:
