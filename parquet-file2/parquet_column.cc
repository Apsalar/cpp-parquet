//
// Parquet Column Writer
//
// Copyright (c) 2015 Apsalar Inc.
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

ParquetColumn::ParquetColumn(StringSeq const & i_name,
                             int i_maxreplvl,
                             int i_maxdeflvl,
                             Type::type i_data_type,
                             FieldRepetitionType::type i_repetition_type,
                             Encoding::type i_encoding,
                             CompressionCodec::type i_compression_codec)
    : m_name(i_name)
    , m_maxreplvl(i_maxreplvl)
    , m_maxdeflvl(i_maxdeflvl)
    , m_data_type(i_data_type)
    , m_repetition_type(i_repetition_type)
    , m_encoding(i_encoding)
    , m_compression_codec(i_compression_codec)
    , m_numrecs(0)
    , m_column_write_offset(-1L)
{
}

void
ParquetColumn::add_datum(void const * i_ptr,
                         size_t i_size,
                         int i_replvl,
                         int i_deflvl)
{
    off_t beg = m_data.size();

    if (i_ptr && i_size)
        m_data.insert(m_data.end(),
                      static_cast<uint8_t const *>(i_ptr),
                      static_cast<uint8_t const *>(i_ptr) + i_size);
                      
    m_meta.push_back(MetaData(beg, i_size, i_replvl, i_deflvl));

    if (i_replvl == 0)
        ++m_numrecs;
}

size_t
ParquetColumn::num_records() const
{
    return m_numrecs;
}

size_t
ParquetColumn::num_datum() const
{
    return m_meta.size();
}

size_t
ParquetColumn::data_size() const
{
    return m_data.size();
}

string
ParquetColumn::path_string() const
{
    LOG(FATAL) << "not implemented yet";
}

void
ParquetColumn::flush(int fd, TCompactProtocol * protocol)
{
    m_column_write_offset = lseek(fd, 0, SEEK_CUR);

    VLOG(2) << "Inside flush for " << path_string();
    VLOG(2) << "\tData size: " << m_data.size() << " bytes.";
    VLOG(2) << "\tNumber of records for this flush: " <<  num_records();
    VLOG(2) << "\tFile offset: " << m_column_write_offset;

    OctetSeq enc_rep_lvls = encode_repetition_levels();
    OctetSeq enc_def_lvls = encode_definition_levels();
    
    m_uncompressed_size =
        m_data.size() + enc_rep_lvls.size() + enc_def_lvls.size();

    // We add 8 to this for the two ints at that indicate the length of
    // the rep & def levels.
    if (!enc_rep_lvls.empty())
        m_uncompressed_size += 4;
    if (!enc_def_lvls.empty())
        m_uncompressed_size += 4;

    DataPageHeader data_header;
    PageHeader page_header;
    page_header.__set_type(PageType::DATA_PAGE);

    page_header.__set_uncompressed_page_size(m_uncompressed_size);
    // Obviously, this is a stop gap until compression support is added.
    page_header.__set_compressed_page_size(m_uncompressed_size);
    data_header.__set_num_values(num_datum());
    data_header.__set_encoding(Encoding::PLAIN);
    // NB: For some reason, the following two must be set, even though
    // they can default to PLAIN, even for required/nonrepeating fields.
    // I'm not sure if it's part of the Parquet spec or a bug in
    // parquet-dump.
    data_header.__set_definition_level_encoding(Encoding::RLE);
    data_header.__set_repetition_level_encoding(Encoding::RLE);
    page_header.__set_data_page_header(data_header);
    uint32_t page_header_size = page_header.write(protocol);
    m_uncompressed_size += page_header_size;

    VLOG(2) << "\tPage header size: " << page_header_size;
    VLOG(2) << "\tTotal uncompressed bytes: " << m_uncompressed_size;

    if (!enc_rep_lvls.empty())
        flush_seq(fd, enc_rep_lvls);

    if (!enc_def_lvls.empty())
        flush_seq(fd, enc_def_lvls);

    VLOG(2) << "\tData size: " << m_data.size();

    flush_buffer(fd, m_data);

    VLOG(2) << "\tFinal offset after write: " << lseek(fd, 0, SEEK_CUR);
}

ColumnMetaData
ParquetColumn::metadata() const
{
    ColumnMetaData column_metadata;
    column_metadata.__set_type(m_data_type);
    column_metadata.__set_encodings({m_encoding});
    column_metadata.__set_codec(m_compression_codec);
    column_metadata.__set_num_values(num_datum());
    column_metadata.__set_total_uncompressed_size(m_uncompressed_size);
    column_metadata.__set_total_compressed_size(m_uncompressed_size);
    column_metadata.__set_data_page_offset(m_column_write_offset);
    column_metadata.__set_path_in_schema(m_name);
    return column_metadata;
}

OctetSeq
ParquetColumn::encode_repetition_levels()
{
    OctetSeq retval;

    if (m_repetition_type == FieldRepetitionType::REPEATED) {
        int bitwidth = impala::BitUtil::Log2(m_maxreplvl + 1);
        size_t maxbufsz =
            impala::RleEncoder::MaxBufferSize(bitwidth, m_meta.size());
        retval.resize(maxbufsz);
        impala::RleEncoder encoder(retval.data(), maxbufsz, m_maxreplvl);
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

    if (m_repetition_type != FieldRepetitionType::REQUIRED) {
        int bitwidth = impala::BitUtil::Log2(m_maxdeflvl + 1);
        size_t maxbufsz =
            impala::RleEncoder::MaxBufferSize(bitwidth, m_meta.size());
        retval.resize(maxbufsz);
        impala::RleEncoder encoder(retval.data(), maxbufsz, m_maxdeflvl);
        for (auto md : m_meta)
            encoder.Put(md.m_deflvl);
        encoder.Flush();
        retval.resize(encoder.len());
    }

    return move(retval);
}

void
ParquetColumn::flush_buffer(int fd, OctetBuffer const & i_buffer)
{
    size_t blksz = 8192;
    OctetSeq blk;
    blk.reserve(blksz);
    size_t remain = i_buffer.size();
    size_t sz = min(blksz, remain);
    for (auto it = i_buffer.begin(); it != i_buffer.end();)
    {
        // Iterator at begining of next block.
        auto nit = next(it, sz);

        // Copy all bytes between iterators.
        blk.assign(it, nit);

        // Write them.
        flush_seq(fd, blk);

        // How many bytes left?
        remain -= sz;

        // How big is the next block?
        sz = min(blksz, remain);

        it = nit;
    }
}

void
ParquetColumn::flush_seq(int fd, OctetSeq const & i_seq)
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

} // end namespace parquet_file2