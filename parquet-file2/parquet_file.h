//
// Parquet File Writer
//
// Copyright (c) 2015 Apsalar Inc.
// All rights reserved.
//

#include <fstream>

namespace parquet_file2 {

class ParquetFile
{
public:
    ParquetFile(std::ostream & ostrm);

private:
    std::ostream &		m_ostrm;
};

} // end namespace parquet_file2

// Local Variables:
// mode: C++
// End:
