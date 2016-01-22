//
// Parquet Column Writer
//
// Copyright (c) 2015, 2016 Apsalar Inc.
// All rights reserved.
//

#include <snappy.h>
#include <zlib.h>

#include "parquet_types.h"

#include "parquet-file/util/bit-util.h"
#include "parquet-file/util/rle-encoding.h"

#include "parquet_column.h"

using namespace std;
using namespace parquet;

using apache::thrift::protocol::TCompactProtocol;

namespace parquet_file2 {

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
    , m_num_page_values(0)
    , m_rep_enc(m_rep_buf, sizeof(m_rep_buf),
                impala::BitUtil::Log2(i_maxreplvl + 1))
    , m_def_enc(m_def_buf, sizeof(m_def_buf),
                impala::BitUtil::Log2(i_maxdeflvl + 1))
    , m_bool_buf(0)
    , m_bool_cnt(0)
    , m_num_rowgrp_recs(0)
    , m_num_rowgrp_values(0)
    , m_column_write_offset(-1L)
    , m_uncompressed_size(0)
    , m_compressed_size(0)
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
    add_levels(i_size, i_replvl, i_deflvl);
    
    if (i_ptr)
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(i_ptr),
                      static_cast<uint8_t const *>(i_ptr) + i_size);
}

void
ParquetColumn::add_varlen_datum(void const * i_ptr,
                                size_t i_size,
                                int i_replvl,
                                int i_deflvl)
{
    add_levels(i_size, i_replvl, i_deflvl);
    
    if (i_ptr) {
        uint32_t len = i_size;
        uint8_t * lenptr = (uint8_t *) &len;
        m_data.insert(m_data.end(), lenptr, lenptr + sizeof(len));
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(i_ptr),
                      static_cast<uint8_t const *>(i_ptr) + i_size);
    }
}

void
ParquetColumn::add_boolean_datum(bool i_val,
                                 int i_replvl,
                                 int i_deflvl)
{
    add_levels(1, i_replvl, i_deflvl);

    if (i_val)
        m_bool_buf |= (1 << m_bool_cnt);
    ++m_bool_cnt;

    if (m_bool_cnt == 8) {
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(&m_bool_buf),
                      static_cast<uint8_t const *>(&m_bool_buf) + 1);
        m_bool_buf = 0;
        m_bool_cnt = 0;
    }
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
ParquetColumn::num_rowgrp_records() const
{
    return m_num_rowgrp_recs;
}

size_t
ParquetColumn::rowgrp_size() const
{
    return m_compressed_size;
}

void
ParquetColumn::traverse(Traverser & tt)
{
    for (auto child : m_children) {
        tt(child);
        child->traverse(tt);
    }
}

ColumnMetaData
ParquetColumn::flush_row_group(int fd, TCompactProtocol * protocol)
{
    // If there are no pages or any remaining data push a page.
    if (m_num_page_values)
        push_page();

    m_column_write_offset = lseek(fd, 0, SEEK_CUR);

    for (DataPageHandle dph : m_pages) {
        size_t header_size = dph->flush(fd, protocol);
        m_uncompressed_size += header_size;
        m_compressed_size += header_size;
    }

    // We don't want the top-level name in the path here.
    StringSeq topless(m_path.begin() + 1, m_path.end());
    
    ColumnMetaData column_metadata;
    column_metadata.__set_type(m_data_type);
    column_metadata.__set_encodings({m_encoding});
    column_metadata.__set_codec(m_compression_codec);
    column_metadata.__set_num_values(m_num_rowgrp_values);
    column_metadata.__set_total_uncompressed_size(m_uncompressed_size);
    column_metadata.__set_total_compressed_size(m_compressed_size);
    column_metadata.__set_data_page_offset(m_column_write_offset);
    column_metadata.__set_path_in_schema(topless);

    reset_row_group_state();
    
    return column_metadata;
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

size_t
ParquetColumn::DataPage::flush(int fd, TCompactProtocol * protocol)
{
    size_t header_size = m_page_header.write(protocol);
    ssize_t rv = write(fd, m_page_data.data(), m_page_data.size());
    if (rv < 0) {
        LOG(FATAL) << "flush: write failed: " << strerror(errno);
    }
    else if (rv != m_page_data.size()) {
        LOG(FATAL) << "flush: unexpected write size:"
                   << " expecting " << m_page_data.size() << ", saw " << rv;
    }
    return header_size;
}

void
ParquetColumn::add_levels(size_t i_size, int i_replvl, int i_deflvl)
{
    if (m_data.size() + i_size > PAGE_SIZE ||
        m_rep_enc.IsFull() ||
        m_def_enc.IsFull())
        push_page();

    if (m_maxreplvl > 0)
        m_rep_enc.Put(i_replvl);

    if (m_maxdeflvl > 0)
        m_def_enc.Put(i_deflvl);

    ++m_num_page_values;

    if (i_replvl == 0)
        ++m_num_rowgrp_recs;
}

void
ParquetColumn::push_page()
{
    DataPageHandle dph = make_shared<DataPage>();

    m_rep_enc.Flush();
    m_def_enc.Flush();

    if (m_bool_cnt) {
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(&m_bool_buf),
                      static_cast<uint8_t const *>(&m_bool_buf) + 1);
        m_bool_buf = 0;
        m_bool_cnt = 0;
    }

    size_t uncompressed_page_size =
        m_data.size() + m_rep_enc.len() + m_def_enc.len();

    size_t compressed_page_size;
    
    if (m_rep_enc.len())
        uncompressed_page_size += 4;
    if (m_def_enc.len())
        uncompressed_page_size += 4;

    OctetSeq & out = dph->m_page_data;
    
    if (m_compression_codec == CompressionCodec::UNCOMPRESSED) {
        out.reserve(uncompressed_page_size);
        concatenate_page_data(out);
        compressed_page_size = uncompressed_page_size;
    }
    else if (m_compression_codec == CompressionCodec::SNAPPY) {
        // NOTE - I don't see how to run snappy in partial increments,
        // so I think we are forced to pre-concatenate the input into a
        // single buffer prior to compression.
        //
        m_concat_buffer.reserve(uncompressed_page_size);
        concatenate_page_data(m_concat_buffer);
        OctetSeq tmp(snappy::MaxCompressedLength(uncompressed_page_size));
        snappy::RawCompress((char const *) m_concat_buffer.data(),
                            m_concat_buffer.size(),
                            (char *) tmp.data(),
                            &compressed_page_size);
        tmp.resize(compressed_page_size);
        out.assign(tmp.begin(), tmp.end());
    }
    else if (m_compression_codec == CompressionCodec::GZIP) {
        // FIXME - this routine can be improved by running the
        // compressor over the three input "batches".  For simplicity
        // we concatenate all the input batches together and compress
        // in one swoop.
        //
        m_concat_buffer.reserve(uncompressed_page_size);
        concatenate_page_data(m_concat_buffer);

        int window_bits = 15 + 16; // maximum window + GZIP
        z_stream stream;
        memset(&stream, '\0', sizeof(stream));
        int rv = deflateInit2(&stream,
                              Z_DEFAULT_COMPRESSION,
                              Z_DEFLATED,
                              window_bits,
                              9,
                              Z_DEFAULT_STRATEGY);
        if (rv != Z_OK) {
            LOG(FATAL) << "deflateInit2 failed: " << rv;
        }
        size_t maxsz = deflateBound(&stream, uncompressed_page_size);
        OctetSeq tmp(maxsz);
        stream.next_in =
            const_cast<Bytef*>
            (reinterpret_cast<const Bytef*>
             (m_concat_buffer.data()));
        stream.avail_in = m_concat_buffer.size();
        stream.next_out = reinterpret_cast<Bytef*>(tmp.data());
        stream.avail_out = tmp.size();
        rv = deflate(&stream, Z_FINISH);
        if (rv != Z_STREAM_END) {
            LOG(FATAL) << "gzip deflate failed: " << rv;
        }

        out.assign(tmp.begin(), tmp.begin() + stream.total_out);
        compressed_page_size = stream.total_out;
        deflateEnd(&stream);
    }
    else {
        LOG(FATAL) << "unsupported compression codec: "
                   << int(m_compression_codec);
    }

    DataPageHeader data_header;
    data_header.__set_num_values(m_num_page_values);
    data_header.__set_encoding(Encoding::PLAIN);
    // NB: For some reason, the following two must be set, even though
    // they can default to PLAIN, even for required/nonrepeating fields.
    // I'm not sure if it's part of the Parquet spec or a bug in
    // parquet-dump.
    data_header.__set_definition_level_encoding(Encoding::RLE);
    data_header.__set_repetition_level_encoding(Encoding::RLE);

    dph->m_page_header.__set_type(PageType::DATA_PAGE);
    dph->m_page_header.__set_uncompressed_page_size(uncompressed_page_size);
    dph->m_page_header.__set_compressed_page_size(compressed_page_size);
    dph->m_page_header.__set_data_page_header(data_header);

    m_pages.push_back(dph);
    m_num_rowgrp_values += m_num_page_values;
    m_uncompressed_size += uncompressed_page_size;
    m_compressed_size += compressed_page_size;

    reset_page_state();
}

void
ParquetColumn::concatenate_page_data(OctetSeq & buf)
{
    buf.clear();		// Doesn't release memory

    uint32_t len;
    uint8_t * lenptr = (uint8_t *) &len;
    len = m_rep_enc.len();
    if (len) {
        buf.insert(buf.end(), lenptr, lenptr + sizeof(len));
        buf.insert(buf.end(), m_rep_buf, m_rep_buf + len);
    }
    len = m_def_enc.len();
    if (len) {
        buf.insert(buf.end(), lenptr, lenptr + sizeof(len));
        buf.insert(buf.end(), m_def_buf, m_def_buf + len);
    }
    buf.insert(buf.end(), m_data.begin(), m_data.end());
}

void
ParquetColumn::reset_page_state()
{
    m_data.clear();
    m_num_page_values = 0;
    m_rep_enc.Clear();
    m_def_enc.Clear();
    m_bool_buf = 0;
    m_bool_cnt = 0;
}

void
ParquetColumn::reset_row_group_state()
{
    reset_page_state();

    m_pages.clear();
    m_num_rowgrp_recs = 0L;
    m_num_rowgrp_values = 0L;
    m_column_write_offset = -1L;
    m_uncompressed_size = 0L;
    m_compressed_size = 0L;
}

} // end namespace parquet_file2
