//
// Parquet Column Writer
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#include "parquet_types.h"

#include "parquet-file/util/bit-util.h"
#include "parquet-file/util/rle-encoding.h"

#include "parquet_column.h"

using namespace std;
using namespace parquet;

using apache::thrift::protocol::TCompactProtocol;

namespace parquet_file2 {

size_t const PAGE_SIZE = 16 * 1024;

ParquetColumn::ParquetColumn(StringSeq const & i_path,
                             Type::type i_data_type,
                             ConvertedType::type i_converted_type,
                             int i_maxreplvl,
                             int i_maxdeflvl,
                             FieldRepetitionType::type i_repetition_type,
                             Encoding::type i_encoding,
                             CompressionCodec::type i_compression_codec)
    : m_path(i_path)
    , m_data_type(i_data_type)
    , m_converted_type(i_converted_type)
    , m_maxreplvl(i_maxreplvl)
    , m_maxdeflvl(i_maxdeflvl)
    , m_repetition_type(i_repetition_type)
    , m_encoding(i_encoding)
    , m_compression_codec(i_compression_codec)
    , m_num_recs(0)
    , m_num_values(0)
    , m_column_write_offset(-1L)
    , m_uncompressed_size(0)
{
}

void
ParquetColumn::add_child(ParquetColumnHandle const & ch)
{
    m_children.push_back(ch);
}

void
ParquetColumn::add_fixed_datum(void const * i_ptr,
                               size_t i_size,
                               int i_replvl,
                               int i_deflvl)
{
    if (m_data.size() + i_size > PAGE_SIZE)
        push_page();
    
    if (i_ptr)
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(i_ptr),
                      static_cast<uint8_t const *>(i_ptr) + i_size);
                      
    m_meta.push_back(MetaData(i_replvl, i_deflvl));

    if (i_replvl == 0)
        ++m_num_recs;
}

void
ParquetColumn::add_varlen_datum(void const * i_ptr,
                                size_t i_size,
                                int i_replvl,
                                int i_deflvl)
{
    if (m_data.size() + i_size > PAGE_SIZE)
        push_page();
    
    if (i_ptr) {
        uint32_t len = i_size;
        uint8_t * lenptr = (uint8_t *) &len;
        m_data.insert(m_data.end(), lenptr, lenptr + sizeof(len));
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(i_ptr),
                      static_cast<uint8_t const *>(i_ptr) + i_size);
    }
                      
    m_meta.push_back(MetaData(i_replvl, i_deflvl));

    if (i_replvl == 0)
        ++m_num_recs;
}

string
ParquetColumn::name() const
{
    return m_path.back();
}

Type::type
ParquetColumn::data_type() const
{
    return m_data_type;
}

ConvertedType::type
ParquetColumn::converted_type() const
{
    return m_converted_type;
}

FieldRepetitionType::type
ParquetColumn::repetition_type() const
{
    return m_repetition_type;
}

string
ParquetColumn::path_string() const
{
    ostringstream ostrm;
    bool firsttime = true;
    for (string elem : m_path) {
        if (firsttime)
            firsttime = false;
        else
            ostrm << '.';
        ostrm << elem;
    }
    return move(ostrm.str());
}

ParquetColumnSeq const &
ParquetColumn::children() const
{
    return m_children;
}

bool
ParquetColumn::is_leaf() const
{
    return m_children.empty();
}

size_t
ParquetColumn::num_records() const
{
    return m_num_recs;
}

size_t
ParquetColumn::data_size() const
{
    return m_data.size();
}

void
ParquetColumn::traverse(Traverser & tt)
{
    for (auto child : m_children) {
        tt(child);
        child->traverse(tt);
    }
}

void
ParquetColumn::flush(int fd, TCompactProtocol * protocol)
{
    // If there are no pages or any remaining data push a page.
    if (m_pages.empty() || !m_meta.empty())
        push_page();

    m_column_write_offset = lseek(fd, 0, SEEK_CUR);

    for (DataPageHandle dph : m_pages)
        m_uncompressed_size += dph->flush(fd, protocol);
}

SchemaElement
ParquetColumn::schema_element() const
{
    SchemaElement elem;
    elem.__set_name(name());
    elem.__set_repetition_type(repetition_type());
    // Parquet requires that we don't set the number of children if
    // the schema element is for a data column.
    if (children().size() > 0) {
        elem.__set_num_children(children().size());
    } else {
        elem.__set_type(data_type());
        if (converted_type() != -1)
            elem.__set_converted_type(converted_type());
    }
    return move(elem);
}

ColumnMetaData
ParquetColumn::metadata() const
{
    // We don't want the top-level name in the path here.
    StringSeq topless(m_path.begin() + 1, m_path.end());
    
    ColumnMetaData column_metadata;
    column_metadata.__set_type(m_data_type);
    column_metadata.__set_encodings({m_encoding});
    column_metadata.__set_codec(m_compression_codec);
    column_metadata.__set_num_values(m_num_values);
    column_metadata.__set_total_uncompressed_size(m_uncompressed_size);
    column_metadata.__set_total_compressed_size(m_uncompressed_size);
    column_metadata.__set_data_page_offset(m_column_write_offset);
    column_metadata.__set_path_in_schema(topless);
    return column_metadata;
}

void
ParquetColumn::reset_row_group_state()
{
    reset_page_state();

    m_pages.clear();
    m_num_recs = 0L;
    m_num_values = 0L;
    m_column_write_offset = -1L;
    m_uncompressed_size = 0L;
}

OctetSeq
ParquetColumn::encode_repetition_levels()
{
    OctetSeq retval;

    if (m_maxreplvl > 0) {
        int bitwidth = impala::BitUtil::Log2(m_maxreplvl + 1);
        size_t maxbufsz =
            impala::RleEncoder::MaxBufferSize(bitwidth, m_meta.size());
        retval.resize(maxbufsz);
        impala::RleEncoder encoder(retval.data(), maxbufsz, bitwidth);
        for (auto md : m_meta)
            encoder.Put(md.m_replvl);
        encoder.Flush();
        retval.resize(encoder.len());
    }

    return move(retval);
}

OctetSeq
ParquetColumn::encode_definition_levels()
{
    OctetSeq retval;

    if (m_maxdeflvl > 0) {
        int bitwidth = impala::BitUtil::Log2(m_maxdeflvl + 1);
        size_t maxbufsz =
            impala::RleEncoder::MaxBufferSize(bitwidth, m_meta.size());
        retval.resize(maxbufsz);
        impala::RleEncoder encoder(retval.data(), maxbufsz, bitwidth);
        for (auto md : m_meta)
            encoder.Put(md.m_deflvl);
        encoder.Flush();
        retval.resize(encoder.len());
    }

    return move(retval);
}

void
flush_seq(int fd, OctetSeq const & i_seq)
{
    ssize_t rv = write(fd, i_seq.data(), i_seq.size());
    if (rv < 0) {
        LOG(FATAL) << "flush_seq: write failed: " << strerror(errno);
    }
    else if (rv != i_seq.size()) {
        LOG(FATAL) << "flush_seq: unexpected write size:"
                   << " expecting " << i_seq.size() << ", saw " << rv;
    }
}

void
flush_levels(int fd, OctetSeq const & i_seq)
{
    // Write the size of the level data.
    uint32_t sz = i_seq.size();
    ssize_t rv = write(fd, &sz, sizeof(sz));
    if (rv < 0) {
        LOG(FATAL) << "flush_levels: write failed: " << strerror(errno);
    }
    else if (rv != sizeof(sz)) {
        LOG(FATAL) << "flush_levels: unexpected write size:"
                   << " expecting " << sizeof(sz) << ", saw " << rv;
    }

    // Write the level data itself.
    flush_seq(fd, i_seq);
}

size_t
ParquetColumn::DataPage::flush(int fd, TCompactProtocol * protocol)
{
    size_t header_size = m_page_header.write(protocol);
    if (!m_rep_levels.empty())
        flush_levels(fd, m_rep_levels);
    if (!m_def_levels.empty())
        flush_levels(fd, m_def_levels);
    flush_seq(fd, m_data);
    return header_size;
}

void
ParquetColumn::push_page()
{
    DataPageHandle dph = make_shared<DataPage>();

    dph->m_rep_levels = encode_repetition_levels();
    dph->m_def_levels = encode_definition_levels();

    dph->m_data.reserve(m_data.size());
    dph->m_data.insert(dph->m_data.end(), m_data.begin(), m_data.end());

    size_t uncompressed_page_size =
        m_data.size() + dph->m_rep_levels.size() + dph->m_def_levels.size();

    if (! dph->m_rep_levels.empty())
        uncompressed_page_size += 4;
    if (! dph->m_def_levels.empty())
        uncompressed_page_size += 4;

    dph->m_page_header.__set_type(PageType::DATA_PAGE);
    dph->m_page_header.__set_uncompressed_page_size(uncompressed_page_size);
    // Obviously, this is a stop gap until compression support is added.
    dph->m_page_header.__set_compressed_page_size(uncompressed_page_size);

    DataPageHeader data_header;
    data_header.__set_num_values(m_meta.size());
    data_header.__set_encoding(Encoding::PLAIN);
    // NB: For some reason, the following two must be set, even though
    // they can default to PLAIN, even for required/nonrepeating fields.
    // I'm not sure if it's part of the Parquet spec or a bug in
    // parquet-dump.
    data_header.__set_definition_level_encoding(Encoding::RLE);
    data_header.__set_repetition_level_encoding(Encoding::RLE);

    dph->m_page_header.__set_data_page_header(data_header);

    m_pages.push_back(dph);
    m_num_values += m_meta.size();
    m_uncompressed_size += uncompressed_page_size;

    reset_page_state();
}

void
ParquetColumn::reset_page_state()
{
    m_data.clear();
    m_meta.clear();
}

} // end namespace parquet_file2
