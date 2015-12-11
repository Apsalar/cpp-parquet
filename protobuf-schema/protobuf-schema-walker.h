//
// Schema related functions
//
// Copyright (c) 2015 Apsalar Inc. All rights reserved.
//

#pragma once

#include <iostream>
#include <ostream>
#include <string>
#include <vector>

#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

#include <parquet-file/parquet-column.h>

namespace protobuf_schema_walker {

typedef std::vector<std::string> StringSeq;

class SchemaNode;
typedef std::vector<SchemaNode *> SchemaNodeSeq;

class NodeTraverser;

class SchemaNode {
public:
    // "Group" node (has children, no type)
    SchemaNode(StringSeq const & i_name,
               google::protobuf::Descriptor const * i_dp)
        : m_name(i_name)
        , m_dp(i_dp)
        , m_fdp(NULL)
    {}

    // "Leaf" node (no children, has type)
    SchemaNode(StringSeq const & i_name,
               google::protobuf::FieldDescriptor const * i_fdp)
        : m_name(i_name)
        , m_dp(NULL)
        , m_fdp(i_fdp)
    {}

    void traverse(NodeTraverser & nt);

    StringSeq								m_name;
    google::protobuf::Descriptor const *	m_dp;
    google::protobuf::FieldDescriptor const * m_fdp;
    parquet_file::ParquetColumn *			m_parqcolp;
    SchemaNodeSeq							m_children;
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
           std::string const & i_rootmsg);

    void dump(std::ostream & ostrm);

    void convert(std::string const & infile);

private:
    void traverse(NodeTraverser & nt);

    bool process_record(std::istream & istrm);
    
    google::protobuf::compiler::DiskSourceTree	m_srctree;
    google::protobuf::compiler::MultiFileErrorCollector	* m_errcollp;
    google::protobuf::compiler::Importer *		m_importerp;
    google::protobuf::Descriptor const *		m_typep;
    google::protobuf::DynamicMessageFactory		m_dmsgfact;
    google::protobuf::Message const *			m_proto;

    SchemaNode * m_root;
};

} // end protobuf_schema_walker

// Local Variables:
// mode: C++
// End:
