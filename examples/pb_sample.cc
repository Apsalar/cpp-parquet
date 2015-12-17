//
// Generate the Dremel paper sample data set in protobuf.
//
// Copyright (c) 2015 Apsalar Inc. All rights reserved.
//

#include <arpa/inet.h>

#include <iostream>

#include <glog/logging.h>

#include "pb_sample.pb.h"

using namespace std;
using namespace google;

namespace {

void
output_document(pb_sample::Document const & doc)
{
    ostringstream ostrm;
    if (! doc.SerializeToOstream(&ostrm)) {
        LOG(FATAL) << "trouble serializing Document" << endl;
    }

    // We write a record header which consists of:
    // Protocol int16
    // Type int8 // 1=owner,2=attr_feed
    // Size int32

    // Originaly thought these were network order
    // int16_t proto = htons(1);
    // int8_t type = 1;
    // int32_t size = htonl(ostrm.str().size());

    int16_t proto = 1;
    int8_t type = 1;
    int32_t size = ostrm.str().size();

    cout.write((char const *) &proto, sizeof(proto));
    cout.write((char const *) &type, sizeof(type));
    cout.write((char const *) &size, sizeof(size));
    cout.write(ostrm.str().data(), ostrm.str().size());
}

} // end namespace

int
main(int argc, char ** argv)
{
    InitGoogleLogging(argv[0]);

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    pb_sample::Document document;
    pb_sample::Document_mlinks * links;
    pb_sample::Document_mname * name;
    pb_sample::Document_mname_mlanguage * lang;

    document.Clear();
    document.set_docid(10);

    links = document.mutable_links();
    links->add_forward(20);
    links->add_forward(40);
    links->add_forward(60);

    name = document.add_name();
    lang = name->add_language();
    lang->set_code("en-us");
    lang->set_country("us");
    lang = name->add_language();
    lang->set_code("en");
    name->set_url("http://A");

    name = document.add_name();
    name->set_url("http://B");

    name = document.add_name();
    lang = name->add_language();
    lang->set_code("en-gb");
    lang->set_country("gb");

    output_document(document);

    document.Clear();
    document.set_docid(20);

    links = document.mutable_links();
    links->add_backward(10);
    links->add_backward(30);
    links->add_forward(80);

    name = document.add_name();
    name->set_url("http://C");

    output_document(document);
}
