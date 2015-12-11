//
// Schema related functions
//
// Copyright (c) 2015 Apsalar Inc. All rights reserved.
//

#include <arpa/inet.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include <google/protobuf/compiler/importer.h>

#include "protobuf-schema-walker.h"

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::compiler;

using namespace protobuf_schema_walker;

namespace {

string
pathstr(StringSeq const & path)
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

class FieldDumper : public NodeTraverser
{
public:
    FieldDumper(ostream & ostrm) : m_ostrm(ostrm) {}

    virtual void visit(SchemaNode const * np)
    {
        if (np->m_fdp) {
            m_ostrm << pathstr(np->m_name)
                    << ' ' << np->m_fdp->cpp_type_name()
                    << ' ' << repstr(np->m_fdp)
                    << endl;
        }
    }

private:
    ostream & m_ostrm;
};

SchemaNode *
traverse_leaf(StringSeq & path, FieldDescriptor const * fd)
{
    SchemaNode * retval = new SchemaNode(path, fd);
    return retval;
}

SchemaNode *
traverse_group(StringSeq & path, Descriptor const * dd)
{
    SchemaNode * retval = new SchemaNode(path, dd);
    
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            retval->m_children.push_back
                (traverse_group(path, fd->message_type()));
            break;
        default:
            retval->m_children.push_back(traverse_leaf(path, fd));
            break;
        }
        path.pop_back();
    }

    return retval;
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

namespace protobuf_schema_walker {

void
SchemaNode::traverse(NodeTraverser & nt)
{
    nt.visit(this);
    for (SchemaNodeSeq::iterator it = m_children.begin();
         it != m_children.end();
         ++it) {
        SchemaNode * np = *it;
        np->traverse(nt);
    }
}

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

    m_proto = m_dmsgfact.GetPrototype(m_typep);

    StringSeq path;
    m_root = traverse_group(path, m_typep);
}

void
Schema::dump(ostream & ostrm)
{
    FieldDumper fdump(ostrm);
    traverse(fdump);
}

void
Schema::convert(string const & infile)
{
    ifstream istrm(infile.c_str(), ifstream::in | ifstream::binary);
    if (!istrm.good()) {
        LOG(FATAL) << "trouble opening input file: " << infile;
    }

    size_t nrecs = 0;
    bool more = true;
    while (more) {
        more = process_record(istrm);
        if (more)
            ++nrecs;
    }
    cerr << "processed " << nrecs << " records" << endl;
}

void
Schema::traverse(NodeTraverser & nt)
{
    m_root->traverse(nt);
}

bool
Schema::process_record(istream & istrm)
{
    // Read the record header, swap bytes as necessary.
    int16_t proto;
    int8_t type;
    int32_t size;

    istrm.read((char *) &proto, sizeof(proto));
    istrm.read((char *) &type, sizeof(type));
    istrm.read((char *) &size, sizeof(size));

    if (!istrm.good())
        return false;
    
    proto = ntohs(proto);
    size = ntohl(size);

    string buffer(size_t(size), '\0');
    istrm.read(&buffer[0], size);

    if (!istrm.good())
        return false;

#if 0    
    Message * inmsg = m_proto->New();
    inmsg->ParseFromString(buffer);

    FieldDescriptor const * docid_fd = m_typep->FindFieldByName("DocId");
    FieldDescriptor const * name_fd = m_typep->FindFieldByName("Name");

    Reflection const * reflp = inmsg->GetReflection();

    cout << "DocId " << reflp->GetInt64(*inmsg, docid_fd) << endl;

    size_t nnames = reflp->FieldSize(*inmsg, name_fd);
    for (size_t ndx = 0; ndx < nnames; ++ndx) {
        Message const & name_msg =
            reflp->GetRepeatedMessage(*inmsg, name_fd, ndx);

        Reflection const * name_reflp = name_msg.GetReflection();

        FieldDescriptor const * url_fd =
            name_msg.GetDescriptor()->FindFieldByName("Url");

        if (name_reflp->HasField(name_msg, url_fd))
            cout << "    Url "
                 << name_reflp->GetString(name_msg, url_fd) << endl;
    }
#endif
    
    return true;
}

} // end namespace protobuf_schema_walker
