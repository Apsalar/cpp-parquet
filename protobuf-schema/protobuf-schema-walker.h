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
    Schema(std::string const & i_dirpath,
           std::string const & i_filename,
           std::string const & i_topmsg);

    void dump(std::ostream & ostrm);

private:
    void traverse(FieldTraverser & ft);
    
    google::protobuf::compiler::DiskSourceTree	m_srctree;
    google::protobuf::compiler::MultiFileErrorCollector	* m_errcollp;
    google::protobuf::compiler::Importer *		m_importerp;
    google::protobuf::Descriptor const *		m_typep;
};

// Local Variables:
// mode: C++
// End:
