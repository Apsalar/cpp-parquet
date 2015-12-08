//
// Convert protobuf files to parquet
//
// Copyright (c) 2015 Apsalar Inc. All rights reserved.
//

#include <getopt.h>

#include <iostream>
#include <stdexcept>

#include <google/protobuf/descriptor.h>

#include "protobuf-schema-walker.h"

using namespace std;

using namespace google::protobuf;

namespace {

char const * DEF_PROTODIR = "./";
char const * DEF_PROTOFILE = "";
char const * DEF_ROOTMSG = "";

string g_protodir = DEF_PROTODIR;
string g_protofile = DEF_PROTOFILE;
string g_rootmsg = DEF_ROOTMSG;
    
void
usage(int & argc, char ** & argv)
{
    cerr << "usage: " << argv[0] << " [options] -p <protofile> -m <rootmsg> -i <datafile>" << endl
         << "  options:" << endl
         << "    -h, --help            display usage" << endl
         << "    -d, --protodir=DIR    protobuf src dir    [" << DEF_PROTODIR << "]" << endl
         << "    -p, --protofile=FILE  protobuf src file   [" << DEF_PROTOFILE << "]" << endl
         << "    -m, --rootmsg=MSG     root message name   [" << DEF_ROOTMSG << "]" << endl
        ;
}

void
parse_arguments(int & argc, char ** & argv)
{
    // char * endp;

    static struct option long_options[] =
        {
            {"usage",                   no_argument,        0, 'h'},
            {"protodir",                required_argument,  0, 'd'},
            {"protofile",               required_argument,  0, 'p'},
            {"rootmsg",                 required_argument,  0, 'm'},
            {0, 0, 0, 0}
        };

    while (true)
    {
        int optndx = 0;
        int opt = getopt_long(argc, argv, "hd:p:m:",
                              long_options, &optndx);

        // Are we done processing arguments?
        if (opt == -1)
            break;

        switch (opt) {
        case 'h':
            usage(argc, argv);
            exit(0);
            break;

        case 'd':
            g_protodir = optarg;
            break;

        case 'p':
            g_protofile = optarg;
            break;

        case 'm':
            g_rootmsg = optarg;
            break;

        case'?':
            // getopt_long already printed an error message
            usage(argc, argv);
            exit(1);
            break;

        default:
            cerr << "unexpected option: " << char(opt) << endl;
            usage(argc, argv);
            exit(1);
            break;
        }
    }

    if (g_protofile.empty()) {
        cerr << "missing protofile argument" << endl;
        usage(argc, argv);
        exit(1);
    }
        
    if (g_rootmsg.empty()) {
        cerr << "missing rootmsg argument" << endl;
        usage(argc, argv);
        exit(1);
    }
}

    
int run(int & argc, char ** & argv)
{
    parse_arguments(argc, argv);
    Schema schema(g_protodir, g_protofile, g_rootmsg);
    schema.dump(cout);
    return 0;
}
    
}  // end namespace

int
main(int argc, char ** argv)
{
    try
    {
        return run(argc, argv);
    }
    catch (exception const & ex)
    {
        cerr << "EXCEPTION: " << ex.what() << endl;
        return 1;
    }
}
