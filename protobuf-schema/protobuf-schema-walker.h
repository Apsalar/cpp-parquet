//
// Schema related functions
//
// Copyright (c) 2015 Apsalar Inc. All rights reserved.
//

#pragma once

#include <string>
#include <ostream>
#include <iostream>

#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

class FieldTraverser
{
public:
    typedef std::vector<std::string> PathSeq;

    virtual void visit(PathSeq const & i_path,
                       google::protobuf::FieldDescriptor const * i_fd) = 0;
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
    void traverse(FieldTraverser & ft);

    bool process_record(std::istream & istrm);
    
    google::protobuf::compiler::DiskSourceTree	m_srctree;
    google::protobuf::compiler::MultiFileErrorCollector	* m_errcollp;
    google::protobuf::compiler::Importer *		m_importerp;
    google::protobuf::Descriptor const *		m_typep;
    google::protobuf::DynamicMessageFactory		m_dmsgfact;
    google::protobuf::Message const *			m_proto;
};

// Local Variables:
// mode: C++
// End:
