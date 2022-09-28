#include <vector>
#include <chrono>
#include <iostream>
#include <curl/curl.h>
#include <bits/stdc++.h>
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <arrow/filesystem/localfs.h>

using namespace std;

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

const std::string WHITESPACE = "\", \n\r\t\f\v";
 
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
    std::cout<< scol1 <<std::endl;
    ::arrow::Status col2St = arrow_reader->ReadColumn(2, &out);
    std::string scol2 = out->ToString();
    std::cout<< scol2 <<std::endl;

    std::vector<std::string> tokens = splitString(scol1, '\n');
    std::vector<std::string> ids;
    std::vector<std::string> reqs = splitString(scol2, '\n'); //ASSUME on data there is no new line
    std::vector<std::string> req_verbs;
    std::vector<std::string> req_host;
    std::vector<std::string> req_path;
    std::vector<std::string> req_type;
    std::vector<std::string> req_header;
    std::vector<std::string> req_length;
    std::vector<std::string> req_data;



    // First two and last two rows are brackets
    //tokens and reqs should have the same rows, add assert to test that
    for (auto i=2; i < tokens.size()-2; i++) {
        ids.push_back(trim(tokens[i]));
        string trimmed_req = trim(reqs[i]);
        std::vector<string> splitted_req= splitString(trimmed_req, '$'); //ASSUME om data there are now $, think of adding data to new parquet col
        req_verbs.push_back(splitted_req[0]);
        req_host.push_back(splitted_req[1]);
        req_path.push_back(splitted_req[2]);
        req_type.push_back(splitted_req[3]);
        req_header.push_back(splitted_req[4]);
        req_length.push_back(splitted_req[5]);
        req_data.push_back(splitted_req[6]);
    }


    for (auto &t: ids)
        std::cout<<t<<std::endl;
    for (auto &t: req_verbs)
        std::cout<<t<<std::endl;
    for (auto &t: req_host)
        std::cout<<t<<std::endl;
    for (auto &t: req_path)
        std::cout<<t<<std::endl;
    for (auto &t: req_type)
        std::cout<<t<<std::endl;
    for (auto &t: req_header)
        std::cout<<t<<std::endl;
    for (auto &t: req_length)
        std::cout<<t<<std::endl;
    for (auto &t: req_data)
        std::cout<<t<<std::endl;

}

size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {

    std::vector<char> *response = reinterpret_cast<std::vector<char> *>(userdata);
    
    response->insert(response->end(), ptr, ptr+nmemb);
    
    return nmemb;
}

std::vector<char> request(CURL *curl, const std::string &url, std::vector<float> &times)
{

	std::vector<char> response;

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);

	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA,  &response);

	curl_easy_setopt(curl, CURLOPT_COOKIEFILE, "");
	curl_easy_setopt(curl, CURLOPT_COOKIE, "");

    curl_easy_setopt(curl, CURLOPT_SSLCERT, "./user0_cert.pem");
    curl_easy_setopt(curl, CURLOPT_SSLKEY, "./user0_privk.pem");
    curl_easy_setopt(curl, CURLOPT_CAINFO, "./service_cert.pem");

    curl_easy_setopt(curl, CURLOPT_HEADER, 1L); // add in the response the headers

    auto ret = curl_easy_perform(curl);

	const auto start = std::chrono::steady_clock::now();
	auto res = curl_easy_perform(curl);
	const auto end = std::chrono::steady_clock::now();
    std::cout<<res<<std::endl;
    

	std::chrono::duration<double> elapsed = end - start;
    times.push_back(elapsed.count());
	std::cout << "request took: " << elapsed.count() << " s\n";
    double total;
    curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total);
	std::cout << "request from getinfo: " << total << " s\n";

    // curl_off_t pretransfer;
    // curl_off_t start_T;
    // curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T, &pretransfer);
    // curl_easy_getinfo(curl, CURLINFO_STARTTRANSFER_TIME_T, &start_T);
	// std::cout << "request from 2: " << (long)(start_T % 1000000) - (long)(pretransfer % 1000000)<< " "<< (start_T) << " s\n";
	
    return response;
}

int main() {
    curl_global_init(CURL_GLOBAL_ALL);
    CURL *curl = curl_easy_init();
    std::vector<float> times;

    for (int i = 0; i < 20; ++i) {
        std::vector<char> response = request(curl, "https://127.0.0.1:8000/app/log/private?id=42", times);
        // std::cout << "response size: " << response.size() << '\n';
        
    }

    float sum_of_elems;

    for (auto& n : times)
        sum_of_elems += n;

	std::cout << "requests took: " << sum_of_elems << " s\n";

    sort(times.begin(), times.end());

	std::cout << "latency: " << times[times.size()/2]*1000 << " ms\n";

    readParquetFile();
    
    curl_easy_cleanup(curl);
    curl_global_cleanup();
}
