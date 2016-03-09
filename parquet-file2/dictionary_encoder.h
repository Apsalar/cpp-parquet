//
// Parquet Dictionary Encoder
//
// Copyright (c) 2016 Apsalar Inc.
// All rights reserved.
//

#pragma once

#include <stdint.h>

#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace std {

template<>
struct hash<vector<uint8_t>>
    : public __hash_base<size_t, vector<uint8_t> >
{
    size_t operator()(vector<uint8_t> const & __v) const noexcept {
        return std::_Hash_impl::hash(__v.data(), __v.size());
    }
};

} // end namespace std

namespace parquet_file2 {

typedef std::vector<uint8_t> OctetSeq;

typedef std::unordered_map<OctetSeq, uint32_t> ValueIndexMap;

class DictionaryEncoder
{
public:
    DictionaryEncoder();

    uint32_t encode_datum(void const * i_ptr, size_t i_size, bool i_isvarlen)
        throw(std::overflow_error);

    void clear();

    static size_t const MAX_NVALS = 40 * 1000;
    
    size_t m_nvals;
    OctetSeq m_data;

private:
    ValueIndexMap m_map;
};

} // end namespace parquet_file2

// Local Variables:
// mode: C++
// End:
