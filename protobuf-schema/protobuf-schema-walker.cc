//
// Schema related functions
//
// Copyright (c) 2015, 2016 Apsalar Inc. All rights reserved.
//

#include <arpa/inet.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "parquet_types.h"

#include <google/protobuf/compiler/importer.h>

#include "protobuf-schema-walker.h"

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::compiler;

using namespace protobuf_schema_walker;
using namespace parquet;
using namespace parquet_file2;

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
              int i_maxreplvl,
              int i_maxdeflvl,
              bool i_dotrace)
{
    int maxreplvl = i_fd->is_repeated() ? i_maxreplvl + 1 : i_maxreplvl;
    int maxdeflvl = i_fd->is_required() ? i_maxdeflvl : i_maxdeflvl + 1;

    SchemaNode * retval = new SchemaNode(path, NULL, i_fd,
                                         maxreplvl, maxdeflvl, i_dotrace);
    return retval;
}

SchemaNode *
traverse_group(StringSeq & path,
               FieldDescriptor const * i_fd,
               int i_maxreplvl,
               int i_maxdeflvl,
               bool i_dotrace)
{
    Descriptor const * dd = i_fd->message_type();

    int maxreplvl = i_fd->is_repeated() ? i_maxreplvl + 1 : i_maxreplvl;
    int maxdeflvl = i_fd->is_required() ? i_maxdeflvl : i_maxdeflvl + 1;

    SchemaNode * retval = new SchemaNode(path, dd, i_fd,
                                         maxreplvl, maxdeflvl, i_dotrace);
    
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            {
                SchemaNode * child =
                    traverse_group(path, fd, maxreplvl, maxdeflvl, i_dotrace);
                retval->add_child(child);
            }
            break;
        default:
            {
                SchemaNode * child =
                    traverse_leaf(path, fd, maxreplvl, maxdeflvl, i_dotrace);
                retval->add_child(child);
            }
            break;
        }
        path.pop_back();
    }

    return retval;
}

SchemaNode *
traverse_root(StringSeq & path, Descriptor const * dd, bool dotrace)
{
    SchemaNode * retval = new SchemaNode(path, dd, NULL, 0, 0, dotrace);
    
    for (int ndx = 0; ndx < dd->field_count(); ++ndx) {
        FieldDescriptor const * fd = dd->field(ndx);
        path.push_back(fd->name());
        switch (fd->cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE:
            {
                SchemaNode * child =
                    traverse_group(path, fd, 0, 0, dotrace);
                retval->add_child(child);
            }
            break;
        default:
            {
                SchemaNode * child =
                    traverse_leaf(path, fd, 0, 0, dotrace);
                retval->add_child(child);
            }
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

SchemaNode::SchemaNode(StringSeq const & i_path,
                       Descriptor const * i_dp,
                       FieldDescriptor const * i_fdp,
                       int i_maxreplvl,
                       int i_maxdeflvl,
                       bool i_dotrace)
        : m_path(i_path)
        , m_dp(i_dp)
        , m_fdp(i_fdp)
        , m_maxreplvl(i_maxreplvl)
        , m_maxdeflvl(i_maxdeflvl)
        , m_dotrace(i_dotrace)
{
    // Are we the root node?
    if (m_fdp == NULL) {
        parquet::Type::type data_type = parquet::Type::INT32;
        parquet::ConvertedType::type converted_type =
            parquet::ConvertedType::INT_32;
        FieldRepetitionType::type repetition_type =
            FieldRepetitionType::REQUIRED;
        Encoding::type encoding = Encoding::PLAIN;
        CompressionCodec::type compression_codec =
            CompressionCodec::UNCOMPRESSED;

        m_pqcol =
            make_shared<ParquetColumn>(i_path,
                                       data_type,
                                       converted_type,
                                       m_maxreplvl,
                                       m_maxdeflvl,
                                       repetition_type,
                                       encoding,
                                       compression_codec);
    }
    else {
        parquet::Type::type data_type;
        parquet::ConvertedType::type converted_type =
            parquet::ConvertedType::type(-1);

        switch (m_fdp->type()) {
        case FieldDescriptor::TYPE_DOUBLE:
            data_type = parquet::Type::DOUBLE;
            break;
        case FieldDescriptor::TYPE_FLOAT:
            data_type = parquet::Type::FLOAT;
            break;
        case FieldDescriptor::TYPE_INT64:
        case FieldDescriptor::TYPE_SINT64:
        case FieldDescriptor::TYPE_SFIXED64:
            data_type = parquet::Type::INT64;
            break;
        case FieldDescriptor::TYPE_UINT64:
        case FieldDescriptor::TYPE_FIXED64:
            data_type = parquet::Type::INT64;
            converted_type = parquet::ConvertedType::UINT_64;
            break;
        case FieldDescriptor::TYPE_INT32:
        case FieldDescriptor::TYPE_SINT32:
        case FieldDescriptor::TYPE_SFIXED32:
            data_type = parquet::Type::INT32;
            break;
        case FieldDescriptor::TYPE_UINT32:
        case FieldDescriptor::TYPE_FIXED32:
            data_type = parquet::Type::INT32;
            converted_type = parquet::ConvertedType::UINT_32;
            break;
        case FieldDescriptor::TYPE_BOOL:
            data_type = parquet::Type::BOOLEAN;
            break;
        case FieldDescriptor::TYPE_STRING:
            data_type = parquet::Type::BYTE_ARRAY;
            converted_type = parquet::ConvertedType::UTF8;
            break;
        case FieldDescriptor::TYPE_BYTES:
            data_type = parquet::Type::BYTE_ARRAY;
            break;
        case FieldDescriptor::TYPE_MESSAGE:
        case FieldDescriptor::TYPE_GROUP:
            // This strikes me as bad; is there an out-of-band value instead?
            data_type = parquet::Type::INT32;
            converted_type = parquet::ConvertedType::INT_32;
            break;
        case FieldDescriptor::TYPE_ENUM:
            LOG(FATAL) << "enum currently unsupported";
            converted_type = parquet::ConvertedType::ENUM;
            break;
        default:
            LOG(FATAL) << "unsupported type: " << int(m_fdp->type());
            break;
        }
        
        FieldRepetitionType::type repetition_type =
            m_fdp->is_required() ? FieldRepetitionType::REQUIRED :
            m_fdp->is_optional() ? FieldRepetitionType::OPTIONAL :
            FieldRepetitionType::REPEATED;

        Encoding::type encoding = Encoding::PLAIN;

        CompressionCodec::type compression_codec =
            CompressionCodec::UNCOMPRESSED;

        m_pqcol = make_shared<ParquetColumn>(i_path,
                                             data_type,
                                             converted_type,
                                             m_maxreplvl,
                                             m_maxdeflvl,
                                             repetition_type,
                                             encoding,
                                             compression_codec);
    }
}

void
SchemaNode::add_child(SchemaNode * i_child)
{
    m_children.push_back(i_child);
    m_pqcol->add_child(i_child->m_pqcol);
}

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
            if (m_fdp->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                propagate_message(NULL, replvl, deflvl);
            }
            else {
                propagate_value(NULL, NULL, -1, replvl, deflvl);
            }
        }
    }
    else if (m_fdp->is_repeated()) {
        size_t nvals = i_reflp->FieldSize(*i_msg, m_fdp);
        if (nvals > 0) {
            for (size_t ndx = 0; ndx < nvals; ++ndx) {
                if (ndx == 0)
                    propagate_value(i_reflp, i_msg, ndx, replvl, deflvl+1);
                else
                    propagate_value(i_reflp, i_msg, ndx, m_maxreplvl, deflvl+1);
            }
        }
        else {
            if (m_fdp->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                propagate_message(NULL, replvl, deflvl);
            }
            else {
                propagate_value(NULL, NULL, -1, replvl, deflvl);
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

    if (!i_reflp) {
        if (m_dotrace) {
            cerr << pathstr(m_path) << ": " << "NULL"
                 << ", R:" << replvl << ", D:" << deflvl
                 << endl;
        }
        switch (m_fdp->cpp_type()) {
        case FieldDescriptor::CPPTYPE_STRING:
            m_pqcol->add_varlen_datum(NULL, 0, replvl, deflvl);
            break;
        default:
            m_pqcol->add_fixed_datum(NULL, 0, replvl, deflvl);
            break;
        }
    }
    else {
        switch (m_fdp->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            {
                int32_t val = ndx == -1
                    ? i_reflp->GetInt32(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedInt32(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_fixed_datum(&val, sizeof(val), replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            {
                int64_t val = ndx == -1
                    ? i_reflp->GetInt64(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedInt64(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_fixed_datum(&val, sizeof(val), replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            {
                uint32_t val = ndx == -1
                    ? i_reflp->GetUInt32(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedUInt32(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_fixed_datum(&val, sizeof(val), replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            {
                uint64_t val = ndx == -1
                    ? i_reflp->GetUInt64(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedUInt64(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_fixed_datum(&val, sizeof(val), replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            {
                double val = ndx == -1
                    ? i_reflp->GetDouble(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedDouble(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_fixed_datum(&val, sizeof(val), replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            {
                float val = ndx == -1
                    ? i_reflp->GetFloat(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedFloat(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_fixed_datum(&val, sizeof(val), replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            {
                bool val = ndx == -1
                    ? i_reflp->GetBool(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedBool(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_fixed_datum(&val, sizeof(val), replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            LOG(FATAL) << "field " << pathstr(m_path)
                       << " is of unknown type: " << m_fdp->cpp_type_name();
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            {
                string val = ndx == -1
                    ? i_reflp->GetString(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedString(*i_msg, m_fdp, ndx);
                if (m_dotrace) {
                    cerr << pathstr(m_path) << ": " << val
                         << ", R:" << replvl << ", D:" << deflvl
                         << endl;
                }
                m_pqcol->add_varlen_datum(val.data(), val.size(),
                                          replvl, deflvl);
            }
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            {
                Message const & cmsg = ndx == -1
                    ? i_reflp->GetMessage(*i_msg, m_fdp)
                    : i_reflp->GetRepeatedMessage(*i_msg, m_fdp, ndx);
                propagate_message(&cmsg, i_replvl, deflvl);
            }
            break;
        default:
            LOG(FATAL) << "field " << pathstr(m_path)
                       << " is of unknown type: " << int(m_fdp->cpp_type());
            break;
        }
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

    string outfile = "./output.parquet";
    unlink(outfile.c_str());
    m_output = new ParquetFile(outfile);

    StringSeq path = { m_typep->full_name() };
    m_root = traverse_root(path, m_typep, m_dotrace);

    m_output->set_root(m_root->column());
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
        more = process_record(istrm, nrecs);
        if (m_dotrace) {
            cerr << endl;
        }
        if (more)
            ++nrecs;
    }
    cerr << "processed " << nrecs << " records" << endl;
}

void
Schema::flush()
{
    m_output->flush();
}

void
Schema::traverse(NodeTraverser & nt)
{
    m_root->traverse(nt);
}

bool
Schema::process_record(istream & istrm, size_t recnum)
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

    if (m_dotrace)
        cerr << "Record: " << recnum+1 << endl
             << endl
             << inmsg->DebugString() << endl;
    
    m_root->propagate_message(inmsg, 0, 0);
    
    return true;
}

} // end namespace protobuf_schema_walker
