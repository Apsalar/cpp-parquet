//
// Parquet File Writer
//
// Copyright (c) 2015 Apsalar Inc.
// All rights reserved.
//

#include "parquet_file.h"

using namespace std;

namespace parquet_file2 {

ParquetFile::ParquetFile(ostream & ostrm)
    : m_ostrm(ostrm)
{
}

} // end namespace parquet_file2
