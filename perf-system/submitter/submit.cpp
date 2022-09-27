#include <vector>
#include <chrono>
#include <iostream>
#include <curl/curl.h>

size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {

    std::vector<char> *response = reinterpret_cast<std::vector<char> *>(userdata);
    
    response->insert(response->end(), ptr, ptr+nmemb);
    
    return nmemb;
}

std::vector<char> request(CURL *curl, const std::string &url)
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

	std::chrono::duration<double> elapsed = end - start;
	std::cout << "request took: " << elapsed.count() << " s\n";

	return response;
}

int main() {
    curl_global_init(CURL_GLOBAL_ALL);
    CURL *curl = curl_easy_init();

    for (int i = 0; i < 20; ++i) {
        std::vector<char> response = request(curl, "https://127.0.0.1:8000/app/log/private?id=42");
        std::cout << "response size: " << response.size() << '\n';
        
    }
    
    curl_easy_cleanup(curl);
    curl_global_cleanup();
}
