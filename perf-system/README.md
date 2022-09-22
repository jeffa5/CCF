# Generic Performance Testing System

 A Performance Testing System which consists from three different components:
*Generator*
*Submitter* 
*Analysis*

In order to run the components you need python3 and run each component selectively. The folder contatining each these components is the __CCF/perf-system__ 

The required packages are included inside the the requirements.txt and can be installed running the following command from the __CCF/perf-system__ directory
```sh
pip install -r requirements.txt
```
After that, run your CCF system.

## Generator
Inside the __CCF/perf-system/Generator__ exists the generator component and you can execute it from this directory with the following command:
```sh
python3 generator.py
```
By default, the generator will create a .parquet file, which is necessary for the following component, using the configurations provided. To provide another configuration file please use the following options:

- ```-hs, --host```: The main host to submit the request. Default `http://localhost:8000`
- ```-p, --path```: The realtive path to submit the request. Default `app/log/private`
- ```-t, --type```: The type of the HTTP request (Only HTTP/1.1 which is the default is supported for now)
- ```-vr, --verb```: The request action. Default `POST` (Only `POST` and `GET` are supported for now)
- ```-r, --rows```: The number of requests to send. Default `16` 

## Submitter
In the __CCF/perf-system/Submitter__ there is the submitter component and you can execute it from this directory with the following command
```sh
python3 submitter.py -ca /path/to/ca/cert -c /path/to/certificate -k /path/to/private/key
```
In order to run the submitter you need to provide the TLS configuration files:
- ```-ca, --cacert```: Use the specified certificate file to verify the peer
- ```-c, --cert```: Use the specified client certificate file
- ```-k, --key```: Private key file

When the submitter is done, there will be two .parquet files generated in this directory

## Analyzer
The __CCF/perf-system/Analyzer__ directory, contatins the last component which is used to produce some metrics based on the submitted data of the previous component. For this component you need to run the following command from the current directory:
```sh
python3 analysis.py
```
After the execution, there will be printed in the terminal some metrics of the data that were sumbitted.