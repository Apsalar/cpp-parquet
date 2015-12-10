//
// Convert protobuf files to parquet
//
// Copyright (c) 2015 Apsalar Inc. All rights reserved.
//

#include <getopt.h>

#include <iostream>
#include <stdexcept>

#include <glog/logging.h>

#include <google/protobuf/descriptor.h>

#include "protobuf-schema-walker.h"

using namespace std;

using namespace google::protobuf;

namespace {

char const * DEF_PROTODIR = "./";
char const * DEF_PROTOFILE = "";
char const * DEF_ROOTMSG = "";
char const * DEF_INFILE = "";

string g_protodir = DEF_PROTODIR;
string g_protofile = DEF_PROTOFILE;
string g_rootmsg = DEF_ROOTMSG;
string g_infile = DEF_INFILE;
bool g_dodump = false;    
    
void
usage(int & argc, char ** & argv)
{
    cerr << "usage: " << argv[0] << " [options] -p <protofile> -m <rootmsg> [-i <infile>]" << endl
         << "  options:" << endl
         << "    -h, --help            display usage" << endl
         << "    -d, --protodir=DIR    protobuf src dir    [" << DEF_PROTODIR << "]" << endl
         << "    -p, --protofile=FILE  protobuf src file   [" << DEF_PROTOFILE << "]" << endl
         << "    -m, --rootmsg=MSG     root message name   [" << DEF_ROOTMSG << "]" << endl
         << "    -i, --infile=FILE     protobuf data input [" << DEF_INFILE << "]" << endl
         << "    -u, --dump            pretty print the schema to stderr" << endl
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
            {"infile",                  required_argument,  0, 'i'},
            {"dump",                    no_argument,        0, 'u'},
            {0, 0, 0, 0}
        };

    while (true)
    {
        int optndx = 0;
        int opt = getopt_long(argc, argv, "hd:p:m:i:u",
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

        case 'i':
            g_infile = optarg;
            break;

        case 'u':
            g_dodump = true;
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
    google::InitGoogleLogging(argv[0]);
    parse_arguments(argc, argv);
    Schema schema(g_protodir, g_protofile, g_rootmsg);

    if (g_dodump)
        schema.dump(cerr);

    if (!g_infile.empty())
        schema.convert(g_infile);
    
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
