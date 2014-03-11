// Copyright 2014 Mount Sinai School of Medicine

#include "parquet_types.h"
#include "parquet-column.h"

#include <fcntl.h>
#include <boost/shared_ptr.hpp>
#include <glog/logging.h>
#include <string>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TFDTransport.h>
#include <vector>

#ifndef __PARQUET_FILE_H__
#define __PARQUET_FILE_H__

using apache::thrift::transport::TFDTransport;
using apache::thrift::protocol::TCompactProtocol;
using parquet::FileMetaData;
using parquet::SchemaElement;
using std::string;
using std::vector;

const uint32_t kDataBytesPerPage = 81920000;

namespace parquet_file {

class ParquetColumnWalker;
// Main class that represents a Parquet file on disk. 
class ParquetFile {
 public:
  ParquetFile(string file_base, int num_files = 1);
  void SetSchema(const vector<ParquetColumn*>& schema);
  void Flush();
  void Close();
  bool IsOK() { return ok_; }
 private:
  int num_rows;

  // Parquet Thrift structure that has metadata about the entire file.
  FileMetaData file_meta_data_;

  /* Variables that represent file system location and data. */
  string file_base_;
  int num_files_;
  int fd_;

  // Member variables used to actually encode & write the data to
  // disk.
  boost::shared_ptr<TFDTransport> file_transport_;
  boost::shared_ptr<TCompactProtocol> protocol_;
  
  // Walker for the schema.  Parquet requires columns specified as a
  // vector that is the depth first preorder traversal of the schema,
  // which is what this method does.
  void DepthFirstSchemaTraversal(const ParquetColumn* root_column,
				 ParquetColumnWalker* callback);

  // A bit indicating that we've initialized OK, defined the schema,
  // and are ready to start accepting & writing data.
  bool ok_;
};

// A callback class for use with DepthFirstSchemaTraversal.  The
// callback is called for each column in a depth-first, preorder,
// traversal.
class ParquetColumnWalker {
 public:
  // A vector in which nodes are appended according to their order in
  // the depth first traversal.  We do not take ownership of the
  // vector.
  ParquetColumnWalker(vector<SchemaElement*>* dfsVector);

  void ColumnCallback(const ParquetColumn* column);
 private:
  vector<SchemaElement*>* dfsVector_;
};

}  // namespace parquet_file

#endif  // #ifndef __PARQUET_FILE_H__
