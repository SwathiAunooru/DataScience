import sys
import os
import spc
import json
import logging
import pandas as pd
import argparse
from datetime import datetime

# Libraries needed to Run the Function
from google.cloud import storage
from report import upload_blob_text_to_gcs, upload_text_to_Local_file
from gcsbucket import gcsBucket
from writetobigquery import write_to_bigquery
from spc_file_converter import convertSpcFiles

logging.basicConfig()
logging.root.setLevel(logging.NOTSET)
logging.info('Program Execution Started')

# Default config
# Default config should run for local to local file conversion only
# Add local SPC file directory in config property with name Input_Environment_Location
# Add destination directory for json files in config property with name Output_Environment_Location
# If main () routine does not have config in args, run in local mode.

config = {
    "current_env": "default-local",
    "Directory_Type": "folder",
    "Input_Environment_Type": "gcs",
    "Input_Environment_Location": "raw-raman-spc",
    "Required": "yes",
    "Output_Environment_Type": "bigquery",
    "Output_Environment_Location": "nova_ds",
    "report_location": "test_bigquery_sample"
}


def main():
    global config
    parser = argparse.ArgumentParser(description='Run a file conversion based on external config file.')
    parser.add_argument('--config', dest='config', action='store', required=False, type=argparse.FileType('r'),
                        help='File containing config information to override defaults')
    args = parser.parse_args()

    currentTimestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    stepZero = "Step0  " + currentTimestamp + "Report  started \n"

    # If config not present, use default config and run the whole flow from local file to local file.
    # step1 Parsing command line arguments
    if args.config:
        try:
            config = args.config
            # check if file exists if not exit
            config = json.load(config)
            logging.info('Current Environment: ' + config["current_env"])
            stepOne = "Step 1 :  " + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + " Reading custom config values \n"
        except:
            logging.info("Unable to load config.json file")
            stepOne = "Step 1 : " + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + " Unable to load config.json file \n"
            sys.exit()
    else:
        stepOne = "Step 1 :  " + datetime.now().strftime(
            "%Y-%m-%d-%H:%M:%S") + " Reading default config values and exited \n"
        logging.info('Current Environment: ' + config["current_env"])

    # step2 Getting Input_Environment_Type

    if config['Directory_Type'] == 'folder':
        if config['Input_Environment_Type'] == 'local':
            logging.info('Conversion option: Local SPC file read, local json file write')
            stepTwo = "Step2  " + datetime.now().strftime(
                "%Y-%m-%d-%H:%M:%S") + "Conversion option: Local SPC file read, local json file write \n"
            # change to take the config filename from environment variable
            convertSpcFiles(config["Input_Environment_Location"], config["Output_Environment_Location"])
            if config['Output_Environment_Type'] == 'gcs':
                pass
            elif config['Output_Environment_Type'] == 'bigquery':
                pass
            elif config['Output_Environment_Type'] == 'local':
                logging.info("Done!")
            else:
                logging.info("Invalid config value directory_type")
                sys.exit()

        elif config['Input_Environment_Type'] == 'gcs':
            source_bucket = gcsBucket(r"D:\data-ingest\src\config\gcs.json", config['Input_Environment_Location'])
            logging.info('Loading SPC from GCS, write json into GCS')
            stepTwo = "Step2  " + datetime.now().strftime(
                "%Y-%m-%d-%H:%M:%S") + "Loading SPC from GCS, write json into GCS \n"
            for i in source_bucket.bucket_object.list_blobs():
                j = source_bucket.read_from_bucket(i)
                try:
                    cont = spc.Content(j)
                    logging.info(str(cont) + str(type(cont)))
                    df = cont.dataframe_content()
                except:
                    pass
                finally:
                    if config['Output_Environment_Type'] == 'gcs':
                        destination_bucket = gcsBucket(r"D:\data-ingest\src\config\gcs.json", config['Output_Environment_Location'])
                        destination_Blob_Name = i.name[:-4]
                        destination_blob = destination_bucket.bucket_object.blob(destination_Blob_Name)
                        destination_bucket.write_to_bucket(destination_blob, df)
                        logging.info('gcs to gcs was success ')
                        stepTwo = "Step2 " + datetime.now().strftime(
                            "%Y-%m-%d-%H:%M:%S") + " gcs to gcs was successful \n"

                    elif config['Output_Environment_Type'] == 'bigquery':
                        logging.info('gcs to bigquery was successful')
                        stepTwo = "Step2 " + datetime.now().strftime(
                            "%Y-%m-%d-%H:%M:%S") + " gcs to gcs was successful \n"
                        write_to_bigquery(df, config['Output_Environment_Location'], i.name[:-4],r"D:\data-ingest\src\config\bigquery.json")
                    elif config['Output_Environment_Type'] == 'local':
                        logging.info(config['Output_Environment_Type'], "to", config['Output_Environment_Type'],
                                     " was successful")
                    else:
                        logging.info("Invalid config value directory_type")
                        logging.info("gcs to ", config['Output_Environment_Type'], " was failure")
                        stepTwo = "Step 2 : " + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + " gcs to " + str(
                            config['Output_Environment_Type']) + " was failure"" \n"
                        sys.exit()
        else:
            logging.info("Invalid config value:  Input_Environment_Type")
            sys.exit()

    elif config['Directory_Type'] == 'file':
        logging.info("work in progress")
        sys.exit()
    else:
        logging.info("Invalid config value directory_type")
        sys.exit()

    Report = ''.join((stepZero, stepOne, stepTwo))

    if config['Output_Environment_Type'] != "local":
        status = upload_blob_text_to_gcs(config['report_location'], Report)
        sys.exit()
    else:
        status = upload_text_to_Local_file(config["report_location"], Report)
        logging.info("Logging report to push to Local")


if __name__ == "__main__":
    main()
