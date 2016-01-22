//
// Schema related functions
//
// Copyright (c) 2015, 2016 Apsalar Inc. All rights reserved.
//

#pragma once

#include <iostream>
#include <ostream>
#include <string>
#include <vector>

#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

#include <parquet-file2/parquet_column.h>
#include <parquet-file2/parquet_file.h>

namespace protobuf_schema_walker {

typedef std::vector<std::string> StringSeq;

class SchemaNode;
typedef std::vector<SchemaNode *> SchemaNodeSeq;

class NodeTraverser;

class SchemaNode {
public:
    SchemaNode(StringSeq const & i_path,
               google::protobuf::Descriptor const * i_dp,
               google::protobuf::FieldDescriptor const * i_fdp,
               int i_maxreplvl,
               int i_maxdeflvl,
               bool i_dotrace);

    void add_child(SchemaNode * i_child);

    std::string const & name() {
        return m_path.back();
    }

    parquet_file2::ParquetColumnHandle const & column() {
        return m_pqcol;
    }
    
    void traverse(NodeTraverser & nt);

    void propagate_message(google::protobuf::Message const * i_msg,
                           int replvl, int deflvl);

    void propagate_field(google::protobuf::Reflection const * i_reflp,
                         google::protobuf::Message const * i_msg,
                         int replvl, int deflvl);

    void propagate_value(google::protobuf::Reflection const * i_reflp,
                         google::protobuf::Message const * i_msg,
                         int ndx,
                         int replvl, int deflvl);
    
    StringSeq                               m_path;
    google::protobuf::Descriptor const *    m_dp;
    google::protobuf::FieldDescriptor const * m_fdp;
    int                                     m_maxreplvl;
    int                                     m_maxdeflvl;
    parquet_file2::ParquetColumnHandle      m_pqcol;
    SchemaNodeSeq                           m_children;
    bool                                    m_dotrace;
};

class NodeTraverser
{
public:
    virtual void visit(SchemaNode const * node) = 0;
};

class Schema
{
public:
    Schema(std::string const & i_protodir,
           std::string const & i_protofile,
           std::string const & i_rootmsg,
           std::string const & i_outfile,
           bool i_dotrace);

    void dump(std::ostream & ostrm);

    void convert(std::string const & infile);

private:
    void traverse(NodeTraverser & nt);

    bool process_record(std::istream & istrm, size_t recnum);
    
    google::protobuf::compiler::DiskSourceTree  m_srctree;
    google::protobuf::compiler::MultiFileErrorCollector * m_errcollp;
    google::protobuf::compiler::Importer *      m_importerp;
    google::protobuf::Descriptor const *        m_typep;
    google::protobuf::DynamicMessageFactory     m_dmsgfact;
    google::protobuf::Message const *           m_proto;

    parquet_file2::ParquetFile * m_output;

    SchemaNode * m_root;
    bool m_dotrace;
};

} // end protobuf_schema_walker

// Local Variables:
// mode: C++
// End:
