//
// Parquet Compression Utility
//
// Copyright (c) 2016 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <vector>

#include "parquet_types.h"

namespace parquet_file2 {

typedef std::vector<uint8_t> OctetSeq;

class Compressor
{
public:
    Compressor(parquet::CompressionCodec::type i_compression_codec);

    void compress(OctetSeq & in, OctetSeq & out);

private:
    parquet::CompressionCodec::type m_compression_codec;
    OctetSeq m_tmp;
};
    
} // end namespace parquet_file2

// Local Variables:
// mode: C++
// End:
