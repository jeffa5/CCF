// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include "handle_arguments.hpp"

#include <arrow/api.h>
#include <arrow/array/array_binary.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <bits/stdc++.h>
#include <chrono>
#include <curl/curl.h>
#include <iostream>
#include <parquet/arrow/reader.h>
#include <parquet/stream_writer.h>
#include <vector>

using namespace std;

std::vector<std::string> IDS;
std::vector<std::string> REQ_VERB;
std::vector<std::string> REQ_HOST;
std::vector<std::string> REQ_PATH;
std::vector<std::string> REQ_TYPE;
std::vector<std::string> REQ_HEADER;
std::vector<std::string> REQ_LENGTH;
std::vector<std::string> REQ_DATA;
std::vector<std::string> RAW_RESPONSE;
std::vector<double> RESPONSE_TIME;
std::vector<double> SEND_TIME;

const std::string WHITESPACE = "\", \n\r\t\f\v";

std::vector<std::string> splitString(const std::string& str, char splitter)
{
  std::vector<std::string> tokens;

  std::stringstream ss(str);
  std::string token;
  while (std::getline(ss, token, splitter))
  {
    tokens.push_back(token);
  }

  return tokens;
}

void readParquetFile(string generatorFilename)
{
  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::fs::LocalFileSystem file_system;
  std::shared_ptr<arrow::io::RandomAccessFile> input =
    file_system.OpenInputFile(generatorFilename).ValueOrDie();

  // Open Parquet file reader
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
  if (!st.ok())
  {
    std::cout << "Couldn't open Input file" << std::endl;
  }
  else
  {
    std::cout << "Read generator file" << std::endl;
  }

  // Read entire file as a single Arrow table
  auto selected_columns = {0, 1};
  std::shared_ptr<arrow::Table> table;
  st = arrow_reader->ReadTable(selected_columns, &table);
  if (!st.ok())
  {
    std::cout << "Couldn't read Input file" << std::endl;
  }
  else
  {
    std::cout << "Opened generator file" << std::endl;
  }

  std::shared_ptr<::arrow::ChunkedArray> column;

  ::arrow::Status column1Status = arrow_reader->ReadColumn(1, &column);
  std::shared_ptr<arrow::StringArray> col1Vals =
    std::dynamic_pointer_cast<arrow::StringArray>(column->chunk(
      0)); // ASSIGN there is only one chunk with col->num_chunks();

  ::arrow::Status column2Status = arrow_reader->ReadColumn(2, &column);
  std::shared_ptr<arrow::StringArray> col2Vals =
    std::dynamic_pointer_cast<arrow::StringArray>(column->chunk(
      0)); // ASSIGN there is only one chunk with col->num_chunks();
  for (int row = 0; row < col1Vals->length(); row++)
  {
    IDS.push_back(col1Vals->GetString(row));
    std::vector<string> splitted_req = splitString(
      col2Vals->GetString(row), '$'); // ASSUME om data there are now $, think
                                      // of adding data to new parquet col

    REQ_VERB.push_back(splitted_req.at(0));
    REQ_HOST.push_back(splitted_req.at(1));
    REQ_PATH.push_back(splitted_req.at(2));
    REQ_TYPE.push_back(splitted_req.at(3));
    REQ_HEADER.push_back(splitted_req.at(4));
    REQ_LENGTH.push_back(splitted_req.at(5));
    if (splitted_req.size() > 6)
      REQ_DATA.push_back(splitted_req.at(6));
    else
      REQ_DATA.push_back("");
  }
}

void writeResponseParquetFile(std::string filename)
{
  std::shared_ptr<arrow::io::FileOutputStream> outfile;

  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filename));

  parquet::WriterProperties::Builder builder;

  parquet::schema::NodeVector fields;

  fields.push_back(parquet::schema::PrimitiveNode::Make(
    "messageID",
    parquet::Repetition::REQUIRED,
    parquet::Type::BYTE_ARRAY,
    parquet::ConvertedType::UTF8));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
    "receiveTime",
    parquet::Repetition::REQUIRED,
    parquet::Type::DOUBLE,
    parquet::ConvertedType::NONE));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
    "rawResponse",
    parquet::Repetition::OPTIONAL,
    parquet::Type::BYTE_ARRAY,
    parquet::ConvertedType::UTF8));

  std::shared_ptr<parquet::schema::GroupNode> schema =
    std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make(
        "schema", parquet::Repetition::REQUIRED, fields));

  parquet::StreamWriter os{
    parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

  for (size_t i = 0; i < IDS.size(); i++)
  {
    os << IDS[i] << RESPONSE_TIME[i] << RAW_RESPONSE[i] << parquet::EndRow;
  }
}

void writeSendParquetFile(std::string filename)
{
  std::shared_ptr<arrow::io::FileOutputStream> outfile;

  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filename));

  parquet::WriterProperties::Builder builder;

  parquet::schema::NodeVector fields;

  fields.push_back(parquet::schema::PrimitiveNode::Make(
    "messageID",
    parquet::Repetition::REQUIRED,
    parquet::Type::BYTE_ARRAY,
    parquet::ConvertedType::UTF8));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
    "sendTime",
    parquet::Repetition::REQUIRED,
    parquet::Type::DOUBLE,
    parquet::ConvertedType::NONE));

  std::shared_ptr<parquet::schema::GroupNode> schema =
    std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make(
        "schema", parquet::Repetition::REQUIRED, fields));

  parquet::StreamWriter os{
    parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

  for (size_t i = 0; i < IDS.size(); i++)
  {
    os << IDS[i] << SEND_TIME[i] << parquet::EndRow;
  }
}

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata)
{
  std::vector<char>* response = reinterpret_cast<std::vector<char>*>(userdata);

  response->insert(response->end(), ptr, ptr + nmemb);

  return nmemb;
}

void genericRequestSettings(
  CURL* curl,
  int iter,
  std::vector<string> certificates,
  std::vector<char>& response)
{
  curl_easy_setopt(
    curl, CURLOPT_URL, (REQ_HOST[iter] + REQ_PATH[iter]).c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
  if (certificates.size() > 0)
  {
    curl_easy_setopt(curl, CURLOPT_SSLCERT, certificates[0].c_str());
    curl_easy_setopt(curl, CURLOPT_SSLKEY, certificates[1].c_str());
    curl_easy_setopt(curl, CURLOPT_CAINFO, certificates[2].c_str());
  }
  curl_easy_setopt(curl, CURLOPT_HEADER, 1L); // add in the response the headers
  struct curl_slist* hs = NULL;
  hs = curl_slist_append(hs, REQ_HEADER[iter].c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hs);

  if (!REQ_DATA[iter].empty())
  { // or compare with not GET
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, REQ_DATA[iter].c_str());
  }

  if (REQ_TYPE[iter] == "HTTP/2")
  {
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
  }
}

std::vector<char> request(
  CURL* curl,
  std::vector<float>& times,
  int iter,
  std::vector<string> certificates)
{
  std::vector<char> response;

  curl_easy_setopt(
    curl, CURLOPT_URL, (REQ_HOST[iter] + REQ_PATH[iter]).c_str());
  genericRequestSettings(curl, iter, certificates, response);

  auto res = curl_easy_perform(curl);

  double total;
  timeval curTime;
  gettimeofday(&curTime, NULL);

  double sendTime = curTime.tv_sec + curTime.tv_usec / 1000000.0;

  curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total);
  times.push_back(total);

  SEND_TIME.push_back(sendTime);
  RESPONSE_TIME.push_back(sendTime + total);

  std::string resp_string(response.begin(), response.end());
  RAW_RESPONSE.push_back(resp_string);

  return response;
}

struct transfer
{
  CURL* easy;
  int num;
};

int main(int argc, char** argv)
{
  ArgumentParser args;
  args.argument_assigner(argc, argv);

  std::vector<string> certificates = {args.cert, args.key, args.rootCa};

  std::vector<float> times;

  readParquetFile(args.generatorFilename);

  if (!args.isMulitplex)
  {
    // REQUEST BY REQUEST
    CURL* curl = curl_easy_init();
    curl_global_init(CURL_GLOBAL_ALL);
    for (int iter = 0; iter < IDS.size(); ++iter)
    {
      std::vector<char> response = request(curl, times, iter, certificates);
      // for (auto& n : response)
      //   std::cout << n;
      // cout << endl;
    }
    curl_easy_cleanup(curl);
    curl_global_cleanup();
  }
  else
  {
    // MULTIPLEX
    CURLM* multi_handle = curl_multi_init();
    int still_running = 0;
    std::vector<char> responsesVec[IDS.size()];
    struct transfer ts[IDS.size()];
    for (int iter = 0; iter < IDS.size(); iter++)
    {
      CURL* curl;
      curl = ts[iter].easy = curl_easy_init();
      genericRequestSettings(curl, iter, certificates, responsesVec[iter]);

      curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);
      curl_multi_add_handle(multi_handle, curl);
    }

    if (REQ_TYPE[iter] == "HTTP/2")
    {
      curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
    }
    else
    {
      curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_HTTP1);
    }

    do
    {
      CURLMcode mc = curl_multi_perform(multi_handle, &still_running);

      if (still_running)
        /* wait for activity, timeout or "nothing" */
        mc = curl_multi_poll(multi_handle, NULL, 0, 1000, NULL);

      if (mc)
      {
        cout << "mc" << endl << "Exiting" << endl;
        exit(0);
      }
    } while (still_running);

    for (int i = 0; i < IDS.size(); i++)
    {
      // long http_code = 0;
      // curl_easy_getinfo(ts[i].easy, CURLINFO_RESPONSE_CODE, &http_code);
      // cout << http_code << endl;
      double total;
      curl_easy_getinfo(ts[i].easy, CURLINFO_TOTAL_TIME, &total);
      times.push_back(total);
      SEND_TIME.push_back(0);
      RESPONSE_TIME.push_back(total);

      if (responsesVec[i].size() > 0)
      {
        std::string resp_string(responsesVec[i].begin(), responsesVec[i].end());
        RAW_RESPONSE.push_back(resp_string);
      }
      else
      {
        long http_code = 0;
        curl_easy_getinfo(ts[i].easy, CURLINFO_RESPONSE_CODE, &http_code);
        RAW_RESPONSE.push_back(to_string(http_code));
      }
      long http_code = 0;
      curl_multi_remove_handle(multi_handle, ts[i].easy);
      curl_easy_cleanup(ts[i].easy);
    }
  }

  writeSendParquetFile(args.sendFilename);
  writeResponseParquetFile(args.responseFilename);
}
