# Generic Performance Testing System

A Performance Testing System which consists from three different components:
_Generator_
_Submitter_
_Analysis_

In order to run the components you need python3 and run each component selectively. The folder contatining each these components is the **CCF/perf-system**

The required packages are included inside the the requirements.txt and can be installed running the following command from the **CCF/perf-system** directory

```sh
pip install -r requirements.txt
```

After that, run your CCF system.

## Generator

Inside the **CCF/perf-system/generator** exists the generator component and you can execute it from this directory with the following command:

```sh
python3 generator.py
```

By default, the generator will create a .parquet file, which is necessary for the following component, using the configurations provided. To provide another configuration file please use the following options:

- `-hs, --host`: The main host to submit the request. Default `http://localhost:8000`
- `-p, --path`: The realtive path to submit the request. Default `app/log/private`
- `-t, --type`: The type of the HTTP request (Only HTTP/1.1 which is the default is supported for now)
- `-vr, --verb`: The request action. Default `POST` (Only `POST` and `GET` are supported for now)
- `-r, --rows`: The number of requests to send. Default `16`
- `-pf, --parquet_filename`: Name of the parquet file to store the generated requests. Default file `./requests.parquet`
- `-d, --data`: A string with the data to be sent in a POST or DELETE request

This component consists of 3 different files. The **CCF/perf-system/Generator/loggin_generator.py** is an alternative of the command line options run providing more flexibility to the user in order to create his own more complex requests. There exist some samples, calling `create_post()` and `create_get()` functions to initiate rows of requests. All requests in the end should be followed by the `create_parquet()` function in order to generate the parquet file.

## Submitter

In the **CCF/perf-system/submitter** there are two submitter components one written in C++ language and a simpler in Python.

### C++

For the **C++** submitter you first need to download and build the arrow and parquet libraries from the following repository `https://github.com/apache/arrow`.
For a quick guide to build the above repo please use the following commands

```sh
git clone https://github.com/apache/arrow.git
cd arrow/cpp
mkdir release
cd release
cmake .. -DCMAKE_INSTALL_PREFIX=/path/to/install -DARROW_FILESYSTEM=ON -DARROW_PARQUET=ON
make
make install
```

Take extra care with the options given to cmake command. `-DARROW_FILESYSTEM=ON` and `-DARROW_PARQUET=ON` will install the necessary libraries in addition to arrow, needed for the submitter execution.

The `/path/to/install` provided for your system is necessary for the CMake file of our project. Replace inside the **CCF/perf-system/CmakeLists.txt** in lines 7, 9 and 10 the absolute paths `/home/fotisk/include/` and `/home/fotisk/lib/` to `/path/to/install/include/` and `/path/to/install/lib/` respectively.

After installing arrow and changing CMakeLists.txt accordingly follow the commands below to run your program

```sh
mkdir build
cd build
cmake ..
make
cd ..
./build/submit -manual_configurations
```

You can provide certification files or configure import/export files by replacing `-manual_configurations` in the latest command with one or more of the following options, providing after each option the corresponsing argument (where necessary).

- `-c`: Followed by the path to the certificate file
- `-k`: Followed by the path to the private key file
- `-ca`: Followed by the path to the specified certificate file to verify the peer
- `-gf`: Followed by the path to the file that contains the requests to be submitted. Default file `../generator/requests.parquet`
- `-sf`: Followed by the path to the parquet file to store the requests that have been submitted. Default file `./cpp_sends.parquet`.
- `-rf`: Followed by the path to the parquet file to store the responses from the requests that have been submitted. Default file `./cpp_responses.parquet`.
- `-multiplex`: The existence of this option will force the submitter to send http2 requests using multiplex.

### Python

To run the submitter writtern in **Python** you need to run from the current directory the following command

```sh
python3 submitter.py
```

When running the submitter you have the following options:

- `-ca, --cacert`: Use the specified certificate file to verify the peer
- `-c, --cert`: Use the specified client certificate file
- `-k, --key`: Private key file
- `-d, --duration`: Set the duration time for the submitter to run over the data from generator file
- `-gf, --generator_file`: Name of the parquet file that contains the requests to be submitted. Default file `../generator/requests.parquet`
- `-sf, --send_file`: Name of the parquet file to store the requests that have been submitted. Default file `./sends.parquet`.
- `-rf, --response_file`: Name of the parquet file to store the responses from the requests that have been submitted. Default file `./responses.parquet`.

When the submitter is done, there will be two .parquet files generated in this directory

## Analyzer

The **CCF/perf-system/analyzer** directory, contatins the last component which is used to produce some metrics based on the submitted data of the previous component. For this component you need to run the following command from the current directory:

```sh
python3 analysis.py
```

When running the analyzer you have the following options to specify the exported files:

- `-sf, --send_file`: Name of the parquet file for the requests that have been submitted. Default file `../submitter/cpp_send.parquet`.
- `-rf, --response_file`: Name of the parquet file for the responses from the requests that have been submitted. Default file `../submitter/cpp_respond.parquet`.

After the execution, in the command prompt will be written two tables with some metrics and in the current directory there will be exported an image plotting the latency of the requests.