//
// Schema related functions
//
// Copyright (c) 2015 Apsalar Inc. All rights reserved.
//

#include <iostream>
#include <vector>
#include <string>
#include <sstream>

#include <glog/logging.h>

#include <google/protobuf/compiler/importer.h>

#include "protobuf-schema-walker.h"

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::compiler;

namespace {

string
pathstr(FieldTraverser::PathSeq const & path)
{
    ostringstream ostrm;
    for (size_t ndx = 0; ndx < path.size(); ++ndx) {
        if (ndx != 0)
            ostrm << '.';
        ostrm << path[ndx];
    }
    return ostrm.str();
}

string
repstr(FieldDescriptor const * fd)
{
    // Return string expressing the field repetition type.
    if (fd->is_required())
        return "REQ";
    else if (fd->is_optional())
        return "OPT";
    else if (fd->is_repeated())
        return "RPT";
    else
        return "???";
}

class FieldDumper : public FieldTraverser
{
public:
    FieldDumper(ostream & ostrm) : m_ostrm(ostrm) {}

    virtual void visit(PathSeq const & i_path,
                       FieldDescriptor const * i_fd)
    {
        m_ostrm << pathstr(i_path)
                << ' ' << i_fd->cpp_type_name()
                << ' ' << repstr(i_fd)
                << endl;
    }

private:
    ostream & m_ostrm;
};
    
void
traverse_message(FieldTraverser::PathSeq & path,
                 Descriptor const * dd,
                 FieldTraverser & ft)
{
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            traverse_message(path, fd->message_type(), ft);
            break;
        default:
            ft.visit(path, fd);
            break;
        }
        path.pop_back();
    }
}

class MyErrorCollector : public MultiFileErrorCollector
{
    virtual void AddError(string const & filename,
                          int line,
                          int column,
                          string const & message)
    {
        cerr << filename
             << ':' << line
             << ':' << column
             << ':' << message << endl;
    }
};
    
} // end namespace

Schema::Schema(string const & i_protodir,
               string const & i_protofile,
               string const & i_rootmsg)
{
    m_srctree.MapPath("", i_protodir);
    m_errcollp = new MyErrorCollector();
    m_importerp = new Importer(&m_srctree, m_errcollp);
    FileDescriptor const * fdp = m_importerp->Import(i_protofile);
    if (!fdp) {
        LOG(FATAL) << "trouble opening proto file " << i_protofile
                   << " in directory " << i_protodir;
    }
    m_typep = fdp->FindMessageTypeByName(i_rootmsg);
    if (!m_typep) {
        LOG(FATAL) << "couldn't find root message: " << i_rootmsg;
    }
}

void
Schema::dump(ostream & ostrm)
{
    FieldDumper fdump(ostrm);
    traverse(fdump);
}

void
Schema::traverse(FieldTraverser & ft)
{
    FieldTraverser::PathSeq path;
    traverse_message(path, m_typep, ft);
}
