import sys
import argparse
import pandas as pd
import fastparquet as fp
import yaml

REQUEST_CONTENT_TYPE = "Content-Type: application/json"
REQUEST_LENGTH_TEXT = "Content-Length: "
PARQUET_FILE_NAME = "requests.parquet"

class Generator():
    def __init__(self):
        self.df = pd.DataFrame(columns=["messageID", "request"])

    def create_df(self, req_path, req_type, req_verb, req_iters, conf_file):
        """
        Creates a dataframe with the data 
        required for the requests
        """
        # entering the private file paths as metadata in the start of parquet

        if req_verb == "POST":

            for iter in range(req_iters):
                request_message = '{"id": ' + str(iter) + ', "msg": "Send message with id ' + str(iter) + '"}'

                self.df.loc[iter] = [str(iter), req_verb + "$" + req_path + "$" + req_type + "$" \
                                    + REQUEST_CONTENT_TYPE + "$" + REQUEST_LENGTH_TEXT + \
                                    str(len(request_message)) + "$" + request_message]
        
        elif req_verb == "GET":
            for iter in range(req_iters):
                self.df.loc[iter] = [str(iter), req_verb + "$" + req_path + "$" + req_type + "$" \
                    + REQUEST_CONTENT_TYPE]
        


    def create_parquet(self):
        """
        Takes the dataframe data and stores them 
        in a parquet file in the current directory
        """
        fp.write(PARQUET_FILE_NAME, self.df)
        


def main(argv):
    arg_path = "/app/log/private" #default path to request
    arg_type = "HTTP/1.1" #default type
    arg_verb = "POST" #default verb
    arg_iterations = 16

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", help="the path for the request", type=str)
    parser.add_argument("-t", "--type", help="the type of the request", type=str)
    parser.add_argument("-vr", "--verb", help="the verb that specifies the action to the server")
    parser.add_argument("-r", "--rows", help="the number of request to be created", type=int)
    parser.add_argument("-f", "--file", help="the path to a .yaml configuration file")
    

    args = parser.parse_args()

    yaml_input = yaml.safe_load(open(args.file or "./config.yaml"))

    gen = Generator()

    #yaml has highest priority, over command line, over provided args

    gen.create_df(yaml_input["configure"]["path"] or args.path or arg_path, args.type or arg_type,\
                yaml_input["configure"]["verb"] or args.verb or arg_verb,\
                yaml_input["configure"]["rows"] or args.rows or arg_iterations, args.file or "",)
    gen.create_parquet()

if __name__ == "__main__":
    main(sys.argv)
