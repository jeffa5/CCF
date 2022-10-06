// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include "handle_arguments.hpp"
#include "parquet_data.hpp"

#include <arrow/api.h>
#include <arrow/array/array_binary.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <bits/stdc++.h>
#include <chrono>
#include <ctime>
#include <curl/curl.h>
#include <iostream>
#include <parquet/arrow/reader.h>
#include <parquet/stream_writer.h>
#include <vector>

#define MAX_CONNECTIONS_IN_MULTI_OR_PIPELINE 1000

using namespace std;

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

void readParquetFile(string generatorFilename, ParquetData& data_handler)
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
    data_handler.IDS.push_back(col1Vals->GetString(row));
    std::vector<string> splitted_req = splitString(
      col2Vals->GetString(row), '$'); // ASSUME om data there are now $, think
                                      // of adding data to new parquet col

    data_handler.REQ_VERB.push_back(splitted_req.at(0));
    data_handler.REQ_HOST.push_back(splitted_req.at(1));
    data_handler.REQ_PATH.push_back(splitted_req.at(2));
    data_handler.REQ_TYPE.push_back(splitted_req.at(3));
    data_handler.REQ_HEADER.push_back(splitted_req.at(4));
    data_handler.REQ_LENGTH.push_back(splitted_req.at(5));
    if (splitted_req.size() > 6)
      data_handler.REQ_DATA.push_back(splitted_req.at(6));
    else
      data_handler.REQ_DATA.push_back("");
  }
}

void writeResponseParquetFile(std::string filename, ParquetData& data_handler)
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

  for (size_t i = 0; i < data_handler.RESPONSE_TIME.size(); i++)
  {
    os << to_string(i) << data_handler.RESPONSE_TIME[i]
       << data_handler.RAW_RESPONSE[i] << parquet::EndRow;
  }
}

void writeSendParquetFile(std::string filename, ParquetData& data_handler)
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

  for (size_t i = 0; i < data_handler.SEND_TIME.size(); i++)
  {
    os << to_string(i) << data_handler.SEND_TIME[i] << parquet::EndRow;
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
  std::vector<char>& response,
  ParquetData& data_handler)
{
  curl_easy_setopt(
    curl,
    CURLOPT_URL,
    (data_handler.REQ_HOST[iter] + data_handler.REQ_PATH[iter]).c_str());
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
  hs = curl_slist_append(hs, data_handler.REQ_HEADER[iter].c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hs);

  if (!data_handler.REQ_DATA[iter].empty())
  { // or compare with not GET
    curl_easy_setopt(
      curl, CURLOPT_POSTFIELDS, data_handler.REQ_DATA[iter].c_str());
  }

  if (data_handler.REQ_TYPE[iter] == "HTTP/2")
  {
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
  }
}

std::vector<char> submitSingleRequest(
  CURL* curl,
  int iter,
  std::vector<string> certificates,
  ParquetData& data_handler)
{
  std::vector<char> response;

  curl_easy_setopt(
    curl,
    CURLOPT_URL,
    (data_handler.REQ_HOST[iter] + data_handler.REQ_PATH[iter]).c_str());
  genericRequestSettings(curl, iter, certificates, response, data_handler);

  auto res = curl_easy_perform(curl);

  double total;
  double start_transfer;
  timeval curTime;
  gettimeofday(&curTime, NULL);

  double sendTime = curTime.tv_sec + curTime.tv_usec / 1000000.0;
  curl_easy_getinfo(curl, CURLINFO_STARTTRANSFER_TIME, &start_transfer);
  curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total);

  data_handler.SEND_TIME.push_back(sendTime);
  data_handler.RESPONSE_TIME.push_back(sendTime + (total - start_transfer));

  std::string resp_string(response.begin(), response.end());
  data_handler.RAW_RESPONSE.push_back(resp_string);

  return response;
}

int main(int argc, char** argv)
{
  ArgumentParser args;
  args.argument_assigner(argc, argv);
  ParquetData data_handler;

  std::vector<string> certificates = {args.cert, args.key, args.rootCa};

  readParquetFile(args.generatorFilename, data_handler);

  if (!args.isMulitplex)
  {
    // REQUEST BY REQUEST
    cout << "Start Submitting requests" << endl;
    CURL* curl = curl_easy_init();

    auto start = chrono::steady_clock::now();
    if (args.duration > 0)
    {
      int requestsSize = data_handler.IDS.size();
      for (int iter = 0;
           chrono::duration<double>(chrono::steady_clock::now() - start)
             .count() < args.duration;
           ++iter)
      {
        std::vector<char> response = submitSingleRequest(
          curl, iter % requestsSize, certificates, data_handler);
      }
    }
    else
    {
      for (int iter = 0; iter < data_handler.IDS.size(); ++iter)
      {
        std::vector<char> response =
          submitSingleRequest(curl, iter, certificates, data_handler);
      }
    }

    curl_easy_cleanup(curl);
    cout << "Finished Submitting requests" << endl;
  }
  else
  {
    // MULTIPLEX
    int request_packs =
      data_handler.IDS.size() / MAX_CONNECTIONS_IN_MULTI_OR_PIPELINE + 1;
    CURLM* multi_handle[request_packs];
    int still_running[request_packs];
    timeval curTime[request_packs]; // Store timestamp of multiple send
    std::vector<char> responsesVec[data_handler.IDS.size()];
    CURL* ts[data_handler.IDS.size()];

    for (int pack = 0; pack < request_packs; pack++)
    {
      multi_handle[pack] = curl_multi_init();
      still_running[pack] = 0;
      curl_multi_setopt(multi_handle[pack], CURLMOPT_MAX_HOST_CONNECTIONS, 1L);
    }
    for (int iter = 0; iter < data_handler.IDS.size(); iter++)
    {
      int general_iter = iter / MAX_CONNECTIONS_IN_MULTI_OR_PIPELINE;
      ts[iter] = curl_easy_init();
      genericRequestSettings(
        ts[iter], iter, certificates, responsesVec[iter], data_handler);

      curl_easy_setopt(ts[iter], CURLOPT_PIPEWAIT, 1L);
      curl_multi_add_handle(multi_handle[general_iter], ts[iter]);
    }

    if (data_handler.REQ_TYPE[0] == "HTTP/2") // Assuming all the requests
                                              // have the same type
    {
      for (int pack = 0; pack < request_packs; pack++)
      {
        curl_multi_setopt(
          multi_handle[pack], CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
      }
    }
    else
    {
      for (int pack = 0; pack < request_packs; pack++)
      {
        curl_multi_setopt(
          multi_handle[pack], CURLMOPT_PIPELINING, CURLPIPE_HTTP1);
      }
    }

    cout << "Start submitting requests" << endl;

    for (int pack = 0; pack < request_packs; pack++)
    {
      // Get timestamp of multiplex/pipelining multi-send
      gettimeofday(&curTime[pack], NULL);
      do
      {
        CURLMcode mc =
          curl_multi_perform(multi_handle[pack], &still_running[pack]);

        if (still_running[pack])
          /* wait for activity, timeout or "nothing" */
          mc = curl_multi_poll(multi_handle[pack], NULL, 0, 1000, NULL);

        if (mc)
        {
          cout << "mc status: " << mc << endl << "Exiting" << endl;
          exit(0);
        }
      } while (still_running[pack]);
    }
    cout << "Finished submitting requests" << endl;

    for (int i = 0; i < data_handler.IDS.size(); i++)
    {
      int pack_item = i / MAX_CONNECTIONS_IN_MULTI_OR_PIPELINE;
      double start_transfer;
      double total;
      curl_easy_getinfo(ts[i], CURLINFO_TOTAL_TIME, &total);
      curl_easy_getinfo(ts[i], CURLINFO_STARTTRANSFER_TIME, &start_transfer);
      double sendTime =
        curTime[pack_item].tv_sec + curTime[pack_item].tv_usec / 1000000.0;
      data_handler.SEND_TIME.push_back(sendTime);
      data_handler.RESPONSE_TIME.push_back(sendTime + (total - start_transfer));
      if (responsesVec[i].size() > 0)
      {
        std::string resp_string(responsesVec[i].begin(), responsesVec[i].end());
        data_handler.RAW_RESPONSE.push_back(resp_string);
      }
      else
      { // Remember you may need to close the connections earlier to prevent
        // max_open_sessions_soft error
        long http_code = 0;
        curl_easy_getinfo(ts[i], CURLINFO_RESPONSE_CODE, &http_code);
        data_handler.RAW_RESPONSE.push_back(to_string(http_code));
      }
      curl_multi_remove_handle(multi_handle[pack_item], ts[i]);
      curl_easy_cleanup(ts[i]);
    }
  }

  cout << "Start storing results" << endl;
  writeSendParquetFile(args.sendFilename, data_handler);
  writeResponseParquetFile(args.responseFilename, data_handler);
  cout << "Finished storing results" << endl;
}
