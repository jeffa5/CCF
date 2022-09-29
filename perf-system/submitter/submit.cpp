// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#include <vector>
#include <chrono>
#include <iostream>
#include <curl/curl.h>
#include <bits/stdc++.h>
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <arrow/filesystem/localfs.h>

using namespace std;

std::vector<std::string> IDS;
std::vector<std::string> REQ_VERB;
std::vector<std::string> REQ_HOST;
std::vector<std::string> REQ_PATH;
std::vector<std::string> REQ_TYPE;
std::vector<std::string> REQ_HEADER;
std::vector<std::string> REQ_LENGTH;
std::vector<std::string> REQ_DATA;


const std::string WHITESPACE = "\", \n\r\t\f\v";

std::vector<std::string> splitString(const std::string& str, char splitter)
{
    std::vector<std::string> tokens;
 
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, splitter)) {
        tokens.push_back(token);
    }
 
    return tokens;
}

 
std::string ltrim(const std::string &s)
{
    size_t start = s.find_first_not_of(WHITESPACE);
    return (start == std::string::npos) ? "" : s.substr(start);
}
 
std::string rtrim(const std::string &s)
{
    size_t end = s.find_last_not_of(WHITESPACE);
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}
 
std::string trim(const std::string &s) {
    return rtrim(ltrim(s));
}

void readParquetFile(){
    arrow::Status st;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    arrow::fs::LocalFileSystem file_system;
    std::shared_ptr<arrow::io::RandomAccessFile> input = file_system.OpenInputFile("../generator/requests.parquet").ValueOrDie();

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
    if (!st.ok()) {
        std::cout<<"Couldn't open Input file"<<std::endl;
    }
    else {
        std::cout<<"read"<<std::endl;
    }

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    st = arrow_reader->ReadTable(&table);
    if (!st.ok()) {
        std::cout<<"Couldn't read Input file"<<std::endl;
    }
    else {
        std::cout<<"opened"<<std::endl;
    }
    std::shared_ptr<::arrow::ChunkedArray> out;
    ::arrow::Status col1St = arrow_reader->ReadColumn(1, &out);
    std::string scol1 = out->ToString();
    ::arrow::Status col2St = arrow_reader->ReadColumn(2, &out);
    std::string scol2 = out->ToString();

    std::vector<std::string> tokens = splitString(scol1, '\n');
    std::vector<std::string> reqs = splitString(scol2, '\n'); //ASSUME on data there is no new line

    // First two and last two rows are brackets
    //tokens and reqs should have the same rows, add assert to test that
    for (auto i=2; i < tokens.size()-2; i++) {
        IDS.push_back(trim(tokens[i]));
        string trimmed_req = trim(reqs[i]);
        std::vector<string> splitted_req= splitString(trimmed_req, '$'); //ASSUME om data there are now $, think of adding data to new parquet col

        REQ_VERB.push_back(splitted_req[0]);
        REQ_HOST.push_back(splitted_req[1]);
        REQ_PATH.push_back(splitted_req[2]);
        REQ_TYPE.push_back(splitted_req[3]);
        REQ_HEADER.push_back(splitted_req[4]);
        REQ_LENGTH.push_back(splitted_req[5]);

        if(splitted_req.size() >6)
            REQ_DATA.push_back(splitted_req[6]);
        else
            REQ_DATA.push_back("");
    }
}

size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {

    std::vector<char> *response = reinterpret_cast<std::vector<char> *>(userdata);
    
    response->insert(response->end(), ptr, ptr+nmemb);
    
    return nmemb;
}

std::vector<char> request(CURL *curl, std::vector<float> &times, int iter, std::vector<string> certificates)
{

	std::vector<char> response;

    cout<<REQ_HOST[iter]+REQ_PATH[iter]<<endl;
	curl_easy_setopt(curl, CURLOPT_URL, (REQ_HOST[iter]+REQ_PATH[iter]).c_str());

	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);

	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA,  &response);

    if(certificates.size()>0) {
        curl_easy_setopt(curl, CURLOPT_SSLCERT, certificates[0].c_str());
        curl_easy_setopt(curl, CURLOPT_SSLKEY, certificates[1].c_str());
        curl_easy_setopt(curl, CURLOPT_CAINFO, certificates[2].c_str());
    }

    curl_easy_setopt(curl, CURLOPT_HEADER, 1L); // add in the response the headers

    struct curl_slist *hs=NULL;
    hs = curl_slist_append(hs, REQ_HEADER[iter].c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hs);

    if(!REQ_DATA[iter].empty()) { // or compare with not GET
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, REQ_DATA[iter].c_str());
    }

    cout<<REQ_TYPE[iter]<<endl;
    if(REQ_TYPE[iter] == "HTTP/2"){
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
    }

	auto res = curl_easy_perform(curl);
    std::cout<<res<<std::endl;
    
    double total;
    curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total);
	std::cout << "request from getinfo: " << total << " s\n";
    times.push_back(total);
	
    return response;
}

int main(int argc, char **argv) {

    std::vector<string> certificates;

    if(argc > 2) {
        certificates.push_back(argv[1]);
        certificates.push_back(argv[2]);
        certificates.push_back(argv[3]);
    }

    curl_global_init(CURL_GLOBAL_ALL);
    CURL *curl = curl_easy_init();
    std::vector<float> times;
    
    readParquetFile();

    for (int iter = 0; iter < IDS.size(); ++iter) {
        std::vector<char> response = request(curl, times, iter, certificates);
        for (auto& n : response)
            std::cout << n;
        cout<<endl;
    }

    float sum_of_elems;

    for (auto& n : times)
        sum_of_elems += n;

	std::cout << "requests took: " << sum_of_elems << " s\n";

    sort(times.begin(), times.end());

	std::cout << "latency: " << times[times.size()/2]*1000 << " ms\n";

    
    curl_easy_cleanup(curl);
    curl_global_cleanup();
}
