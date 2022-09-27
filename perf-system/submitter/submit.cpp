#include <vector>
#include <chrono>
#include <iostream>
#include <curl/curl.h>
#include <bits/stdc++.h>

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

    curl_off_t pretransfer;
    curl_off_t start_T;
    curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T, &pretransfer);
    curl_easy_getinfo(curl, CURLINFO_STARTTRANSFER_TIME_T, &start_T);
	std::cout << "request from 2: " << (long)(start_T % 1000000) - (long)(pretransfer % 1000000)<< " "<< (start_T) << " s\n";
	
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

    
    curl_easy_cleanup(curl);
    curl_global_cleanup();
}
