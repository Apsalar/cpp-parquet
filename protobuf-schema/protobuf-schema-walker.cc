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
            m_ostrm << pathstr(np->m_path)
                    << ' ' << np->m_fdp->cpp_type_name()
                    << ' ' << repstr(np->m_fdp)
                    << endl;
        }
    }

private:
    ostream & m_ostrm;
};

SchemaNode *
traverse_leaf(StringSeq & path,
              FieldDescriptor const * i_fd,
              int i_replvl,
              bool i_dotrace)
{
    int replvl = i_fd->is_repeated() ? i_replvl + 1 : i_replvl;

    SchemaNode * retval = new SchemaNode(path, NULL, i_fd, replvl, i_dotrace);
    return retval;
}

SchemaNode *
traverse_group(StringSeq & path,
               FieldDescriptor const * i_fd,
               int i_replvl,
               bool i_dotrace)
{
    Descriptor const * dd = i_fd->message_type();

    int replvl = i_fd->is_repeated() ? i_replvl + 1 : i_replvl;

    SchemaNode * retval = new SchemaNode(path, dd, i_fd, replvl, i_dotrace);
    
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            retval->m_children.push_back(traverse_group(path, fd, replvl,
                                                        i_dotrace));
            break;
        default:
            retval->m_children.push_back(traverse_leaf(path, fd, replvl,
                                                       i_dotrace));
            break;
        }
        path.pop_back();
    }

    return retval;
}

SchemaNode *
traverse_root(StringSeq & path, Descriptor const * dd, bool dotrace)
{
    SchemaNode * retval = new SchemaNode(path, dd, NULL, 0, dotrace);
    
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            retval->m_children.push_back(traverse_group(path, fd, 0, dotrace));
            break;
        default:
            retval->m_children.push_back(traverse_leaf(path, fd, 0, dotrace));
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

void
SchemaNode::propagate_message(Message const * i_msg,
                              int replvl, int deflvl)
{
    Reflection const * reflp =
        i_msg ? i_msg->GetReflection() : NULL;

    for (SchemaNodeSeq::iterator it = m_children.begin();
         it != m_children.end();
         ++it) {
        SchemaNode * np = *it;
        np->propagate_field(reflp, i_msg, replvl, deflvl);
    }
}

void
SchemaNode::propagate_field(Reflection const * i_reflp,
                            Message const * i_msg,
                            int replvl, int deflvl)
{
    if (m_fdp->is_required()) {
        propagate_value(i_reflp, i_msg, -1, replvl, deflvl);
    }
    else if (m_fdp->is_optional()) {
        if (i_msg != NULL &&
            i_reflp->HasField(*i_msg, m_fdp)) {
            propagate_value(i_reflp, i_msg, -1, replvl, deflvl+1);
        }
        else {
            propagate_value(NULL, NULL, -1, replvl, deflvl);
        }
    }
    else if (m_fdp->is_repeated()) {
        size_t nvals = i_reflp->FieldSize(*i_msg, m_fdp);
        if (nvals == 0) {
            propagate_value(NULL, NULL, -1, replvl, deflvl);
        }
        else {
            for (size_t ndx = 0; ndx < nvals; ++ndx) {
                if (ndx == 0)
                    propagate_value(i_reflp, i_msg, ndx, replvl, deflvl+1);
                else
                    propagate_value(i_reflp, i_msg, ndx, m_replvl, deflvl+1);
            }
        }
    }
    else {
        LOG(FATAL) << "field " << pathstr(m_path)
                   << " isn't required, optional or repeated";
    }
}

void
SchemaNode::propagate_value(Reflection const * i_reflp,
                            Message const * i_msg,
                            int ndx,
                            int i_replvl, int deflvl)
{
    int replvl = ndx == 0 ? 0 : i_replvl;

#if 0
    cerr << pathstr(m_path) << ": "
         << ", R:" << replvl << ", D:" << deflvl
         << endl;
#endif
    
    switch (m_fdp->cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
        {
            if (i_msg == NULL) {
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << "NULL"
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
            }
            else {
                int32_t val = ndx == -1
                    ? i_reflp->GetInt32(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedInt32(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
            }
        }
        break;
    case FieldDescriptor::CPPTYPE_INT64:
        {
            if (i_msg == NULL) {
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << "NULL"
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
            }
            else {
                int64_t val = ndx == -1
                    ? i_reflp->GetInt64(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedInt64(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
            }
        }
        break;
    case FieldDescriptor::CPPTYPE_UINT32:
    case FieldDescriptor::CPPTYPE_UINT64:
    case FieldDescriptor::CPPTYPE_DOUBLE:
    case FieldDescriptor::CPPTYPE_FLOAT:
    case FieldDescriptor::CPPTYPE_BOOL:
    case FieldDescriptor::CPPTYPE_ENUM:
        LOG(FATAL) << "field " << pathstr(m_path)
                   << " is of unknown type: " << int(m_fdp->cpp_type());
        break;
    case FieldDescriptor::CPPTYPE_STRING:
        {
            if (i_msg == NULL) {
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << "NULL"
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
            }
            else {
                string val = ndx == -1
                    ? i_reflp->GetString(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedString(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
            }
        }
        break;
    case FieldDescriptor::CPPTYPE_MESSAGE:
        {
            if (i_msg == NULL)
                propagate_message(NULL, replvl, deflvl);
            else {
                Message const & cmsg = ndx == -1
                    ? i_reflp->GetMessage(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedMessage(*i_msg, m_fdp, ndx);
                propagate_message(&cmsg, i_replvl, deflvl);
            }
        }
        break;
    default:
        LOG(FATAL) << "field " << pathstr(m_path)
                   << " is of unknown type: " << int(m_fdp->cpp_type());
        break;
    }
}

Schema::Schema(string const & i_protodir,
               string const & i_protofile,
               string const & i_rootmsg,
               bool i_dotrace)
    : m_dotrace(i_dotrace)
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
    m_root = traverse_root(path, m_typep, m_dotrace);
}

void
Schema::dump(ostream & ostrm)
{
    FieldDumper fdump(ostrm);
    traverse(fdump);
    ostrm << endl;
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
        if (m_dotrace) {
            cerr << endl;
        }
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

    // Looks like these fields aren't network order after all.
    // proto = ntohs(proto);
    // size = ntohl(size);

    string buffer(size_t(size), '\0');
    istrm.read(&buffer[0], size);

    if (!istrm.good())
        return false;

    Message * inmsg = m_proto->New();
    inmsg->ParseFromString(buffer);

    m_root->propagate_message(inmsg, 0, 0);
    
    return true;
}

} // end namespace protobuf_schema_walker
