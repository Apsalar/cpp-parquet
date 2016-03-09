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
    , m_original_encoding(i_encoding)
    , m_encoding(i_encoding)
    , m_encodings({i_encoding})
    , m_compression_codec(i_compression_codec)
    , m_compressor(i_compression_codec)
    , m_num_page_values(0)
    , m_rep_enc(m_rep_buf, sizeof(m_rep_buf),
                impala::BitUtil::Log2(i_maxreplvl + 1))
    , m_def_enc(m_def_buf, sizeof(m_def_buf),
                impala::BitUtil::Log2(i_maxdeflvl + 1))
    , m_val_enc(m_val_buf, sizeof(m_val_buf), 16)
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
ParquetColumn::add_datum(void const * i_ptr,
                         size_t i_size,
                         bool i_isvarlen,
                         int i_replvl,
                         int i_deflvl)
{
    // Dictionary encoding, if applicable.
    uint32_t enc_val;
    size_t check_size = 0;
    if (i_ptr) {
        switch (m_encoding) {
        case Encoding::PLAIN:
            check_size = i_size;
            break;
        case Encoding::PLAIN_DICTIONARY:
            try {
                enc_val = m_dict_enc.encode_datum(i_ptr, i_size, i_isvarlen);
                check_size = 0; // We don't use conventional buffer space.
            }
            catch (overflow_error const & ex) {
                // We've overflowed the dictionary, fallback to PLAIN.
                finalize_page();
                m_encoding = Encoding::PLAIN;
                m_encodings.push_back(Encoding::PLAIN);
            }
            break;
        default:
            LOG(FATAL) << "unsupported encoding: " << int(m_encoding);
            break;
        }
    }

    check_full(check_size);

    add_levels(i_replvl, i_deflvl);
    
    if (i_ptr) {
        switch (m_encoding) {
        case Encoding::PLAIN:
            if (i_isvarlen) {
                uint32_t len = i_size;
                uint8_t * lenptr = (uint8_t *) &len;
                m_data.insert(m_data.end(), lenptr, lenptr + sizeof(len));
            }
            m_data.append(static_cast<char const *>(i_ptr), i_size);
            break;
        case Encoding::PLAIN_DICTIONARY:
            m_val_enc.Put(enc_val);
            break;
        default:
            LOG(FATAL) << "unsupported encoding: " << int(m_encoding);
            break;
        }
    }
}

void
ParquetColumn::add_boolean_datum(bool i_val,
                                 int i_replvl,
                                 int i_deflvl)
{
    check_full(1);

    add_levels(i_replvl, i_deflvl);
    
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

size_t
ParquetColumn::estimated_rowgrp_size() const
{
    // Provide a very rough estimate of the size of this rowgroup.
    // Presumes a compression ratio of 3 on the dictionary.  The
    // compressed page data size is in m_compressed_size.  Add some
    // per-page hesader overhead as well.
    return
        m_dict_enc.m_data.size() / 3 + 100 +
        m_pages.size() * 100 +
        m_compressed_size;
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
ParquetColumn::write_row_group(int fd, TCompactProtocol * protocol)
{
    // Finialize any remaining data.
    if (m_num_page_values)
        finalize_page();

    m_column_write_offset = lseek(fd, 0, SEEK_CUR);

    if (m_original_encoding == Encoding::PLAIN_DICTIONARY) {
        size_t dictsz = m_dict_enc.m_data.size();

        m_concat_buffer.assign(m_dict_enc.m_data.begin(),
                               m_dict_enc.m_data.end());
        string out;
        m_compressor.compress(m_concat_buffer, out);
        
        DictionaryPageHeader dph;
        dph.__set_num_values(m_dict_enc.m_nvals);
        dph.__set_encoding(Encoding::PLAIN_DICTIONARY);

        PageHeader ph;
        ph.__set_type(PageType::DICTIONARY_PAGE);
        ph.__set_uncompressed_page_size(dictsz);
        ph.__set_compressed_page_size(out.size());
        ph.__set_dictionary_page_header(dph);

        size_t header_size = ph.write(protocol);
        m_uncompressed_size += header_size;
        m_compressed_size += header_size;

        ssize_t rv = write(fd, out.data(), out.size());
        if (rv < 0) {
            LOG(FATAL) << "write dict failed: " << strerror(errno);
        }
        else if (rv != out.size()) {
            LOG(FATAL) << "write: unexpected size:"
                       << " expecting " << out.size() << ", saw " << rv;
        }
        m_uncompressed_size += dictsz;
        m_compressed_size += out.size();

        VLOG(2) << path_string()
                << " dictionary page header_size " + header_size
                << " data_size " + dictsz;
    }
    
    size_t pgndx = 0;
    for (DataPageHandle dph : m_pages) {
        // The m_uncompressed_page_size and m_compressed_size
        // were updated during in finalize_page ...
        size_t header_size = dph->write_page(fd, protocol);
        VLOG(2) << path_string()
                << " pg " << pgndx << " header_size " << header_size;
        m_uncompressed_size += header_size;
        m_compressed_size += header_size;
        ++pgndx;
    }

    // We don't want the top-level name in the path here.
    StringSeq topless(m_path.begin() + 1, m_path.end());
    
    VLOG(2) << path_string()
            << " total_uncompressed_size " << m_uncompressed_size;

    ColumnMetaData column_metadata;
    column_metadata.__set_type(m_data_type);
    column_metadata.__set_encodings(m_encodings);
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
ParquetColumn::DataPage::write_page(int fd, TCompactProtocol * protocol)
{
    size_t header_size = m_page_header.write(protocol);
    ssize_t rv = write(fd, m_page_data.data(), m_page_data.size());
    if (rv < 0) {
        LOG(FATAL) << "DataPage write failed: " << strerror(errno);
    }
    else if (rv != m_page_data.size()) {
        LOG(FATAL) << "DataPage write: unexpected size:"
                   << " expecting " << m_page_data.size() << ", saw " << rv;
    }
    return header_size;
}

void
ParquetColumn::add_levels(int i_replvl, int i_deflvl)
{
    if (m_maxreplvl > 0)
        m_rep_enc.Put(i_replvl);

    if (m_maxdeflvl > 0)
        m_def_enc.Put(i_deflvl);

    ++m_num_page_values;

    if (i_replvl == 0)
        ++m_num_rowgrp_recs;
}

void
ParquetColumn::finalize_page()
{
    size_t pgndx = m_pages.size();

    DataPageHandle dph = make_shared<DataPage>();

    m_rep_enc.Flush();
    m_def_enc.Flush();
    m_val_enc.Flush();

    if (m_bool_cnt) {
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(&m_bool_buf),
                      static_cast<uint8_t const *>(&m_bool_buf) + 1);
        m_bool_buf = 0;
        m_bool_cnt = 0;
    }

    size_t uncompressed_page_size;
    switch (m_encoding) {
    case Encoding::PLAIN:
        uncompressed_page_size =
            m_rep_enc.len() + m_def_enc.len() + m_data.size();
        break;
    case Encoding::PLAIN_DICTIONARY:
        uncompressed_page_size =
            // RLE(replvl) + RLE(deflvl) + bitwidth + RLE(encoded-values)
            m_rep_enc.len() + m_def_enc.len() + 1 + m_val_enc.len();
        break;
    default:
        LOG(FATAL) << "unsupported encoding: " << int(m_encoding);
        break;
    }

    if (m_rep_enc.len())
        uncompressed_page_size += 4;
    if (m_def_enc.len())
        uncompressed_page_size += 4;

    VLOG(2) << path_string()
            << " pg " << pgndx
            << " m_data.size() " << m_data.size()
            << " m_rep_enc.len() " << m_rep_enc.len()
            << " m_def_enc.len() " << m_def_enc.len();

    string & out = dph->m_page_data;
    
    m_concat_buffer.reserve(uncompressed_page_size);
    concatenate_page_data(m_concat_buffer);
    m_compressor.compress(m_concat_buffer, out);
    size_t compressed_page_size = out.size();

    DataPageHeader data_header;
    data_header.__set_num_values(m_num_page_values);
    data_header.__set_encoding(m_encoding);
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

    VLOG(2) << path_string()
            << " pg " << pgndx
            << " uncompressed_page_size " << uncompressed_page_size;
    
    m_pages.push_back(dph);
    m_num_rowgrp_values += m_num_page_values;
    m_uncompressed_size += uncompressed_page_size;
    m_compressed_size += compressed_page_size;

    reset_page_state();
}

void
ParquetColumn::concatenate_page_data(string & buf)
{
    buf.clear();		// Doesn't release memory

    uint8_t bitwidth = 16;
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
    switch (m_encoding) {
    case Encoding::PLAIN:
        buf.insert(buf.end(), m_data.begin(), m_data.end());
        break;
    case Encoding::PLAIN_DICTIONARY:
        buf.insert(buf.end(), &bitwidth, &bitwidth + 1);
        len = m_val_enc.len();
        buf.insert(buf.end(), m_val_buf, m_val_buf + len);
        break;
    default:
        LOG(FATAL) << "unsupported encoding: " << int(m_encoding);
        break;
    }
}

void
ParquetColumn::reset_page_state()
{
    m_data.clear();
    m_num_page_values = 0;
    m_rep_enc.Clear();
    m_def_enc.Clear();
    m_val_enc.Clear();
    m_bool_buf = 0;
    m_bool_cnt = 0;
}

void
ParquetColumn::reset_row_group_state()
{
    reset_page_state();

    m_encoding = m_original_encoding;
    m_encodings.clear();
    m_encodings.push_back(m_original_encoding);
    
    m_pages.clear();
    m_num_rowgrp_recs = 0L;
    m_num_rowgrp_values = 0L;
    m_column_write_offset = -1L;
    m_uncompressed_size = 0L;
    m_compressed_size = 0L;
    m_dict_enc.clear();
}

} // end namespace parquet_file2
