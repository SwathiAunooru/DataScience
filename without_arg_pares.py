import sys
# sys.path.append("""/config.json""")
### In future, JSconfig needed for authentication of credential to GCS
from UploadToGCP import uploadBlob
from uploadReportTexttoGCS import uploadBlob_text
from DownloadFromGCP import downloadBlob
from ramandataingest import spcToJsonConverter
from ramandataingest import directoryContainingSpcFiles
from gcstobigquery import gcstoBigQuery
#Libraries needed to Run the Function
from google.cloud import storage
import spc
import json
import os
import logging
import pandas as pd
from datetime import datetime
import argparse

logging.info('Logging Started')

#Default config
config = {
    #Input Location
    "current_env" :"default",
    'Input_Environment_Type':"gcs",
    'Input_Environment_Location':"raw-raman-spc",
    #InterMediary Location
    'Required':"yes",
    'No_Of_Intermediates':"2",
    'Intermediary_Environment_Type':"local,gcs",
    'Intermediary_Environment_Location':"test_data,sample-json-files",
    #OutPut Location
    'Output_Environment_Type':"bigquery",
    'Output_Environment_Location':"poms_ds"   
}


def main():
    
    parser = argparse.ArgumentParser(description='Run a file conversion based on external config file.')
    parser.add_argument('--config', dest='config', action='store', required=False,
                       help='File containing config information to override defaults')
    args = parser.parse_args()

    if args.config:
        config =  args.config

    timestr = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    step0 = timestr+"Report logging started \n"

    try:
        config = json.load(open("config.json",'r'))
        logging.info(config["current_env"])
        step1 = "Step1  "+timestr+" Reading custom config values \n"
    except:
        step1 = "Step1  "+timestr+" Reading default config values and exited \n"
        logging.info(config["current_env"])
 
    ###Configuration One: Local-->Local
    #spctojsonconverter
    ###Configuration Two: GCS--->GCS
    #downloadtoGCS
    #spctojsonconverter
    #uploadtoGCS
    ####Configuration Three: GCS-->(JSON)GCS-->BigQuery
    #downloadtLocal
    #spctojsonconverter
    #uploadtoGCS
    #json_bucket_to_bigquery
    ### In future, JSconfig needed for authentication of credential to GCS


    if (config['Input_Environment_Type']=='local') & (config['Output_Environment_Type']=='local'):
        logging.info('local to local was successful')
        try:
            step2 = "Step2 "+timestr+" local to local was successful \n"
            directoryContainingSpcFiles(config['Input_Environment_Location'],config['Output_Environment_Location'])
        except Exception:
            logging.info('local to local was failure')
            step2 = "Step2 "+timestr+" local to local was failure \n"
            logging.exception(Exception)

    elif (config['Input_Environment_Type']=='gcs') & (config['Output_Environment_Type']=='gcs'):
        
        if (config['Required']=='yes') & (config["No_Of_Intermediates"]=='1'):
            logging.info('gcs to gcs was successful')
            step2 = "Step2 "+timestr+" gcs to gcs was successful \n"
            try:
                cwd = os.getcwd()
                logging.info('Current Directory: '+cwd)
                exportJsonPath = cwd + '/raman_docker/test_data'
                logging.info('exportJsonPath:' + exportJsonPath)
                jsConfigPath = cwd + '/raman_docker'
                os.chdir(exportJsonPath)
                logging.info('exportJsonPath current directory:' + exportJsonPath)
                downloadBlob(jsConfigPath + '/GCSConfigJson.json',config['Input_Environment_Location'], exportJsonPath)

                directoryContainingSpcFiles(os.chdir(exportJsonPath))
                uploadBlob(jsConfigPath + '/GCSConfigJson.json',config['Output_Environment_Location'], exportJsonPath)
            except Exception:
                step2 = "Step2 "+timestr+" gcs to gcs was failure \n"
                logging.exception(Exception)
        else:
            logging.raiseException("INVALID INTERMEDIARY VALUES")
    ##Process_Control['Input_Environment']['Location'][0]-->Local
    ##Process_Control['Input_Environment']['Location'][1]-->GCS
    elif (config['Input_Environment_Type']=='gcs') & (config['Output_Environment_Type']=='bigquery'):
        if (config['Required']=='yes') & (config["No_Of_Intermediates"]=='2'):
            logging.info('gcs to bigquery was successful')
            step2 = "Step2 "+timestr+" gcs to bigquery was successful \n"
            try:
                cwd = os.getcwd()
                logging.info('Current Directory: '+cwd)
                exportJsonPath = cwd + '/raman_docker/test_data'
                logging.info('exportJsonPath:' + exportJsonPath)
                jsConfigPath = cwd + '/raman_docker'
                os.chdir(exportJsonPath)
                logging.info('exportJsonPath current directory:' + exportJsonPath)
                downloadBlob(jsConfigPath+'/GCSConfigJson.json',config['Input_Environment_Location'],exportJsonPath)
                try:
                    directoryContainingSpcFiles(os.chdir(exportJsonPath))
                except:
                    pass
                uploadBlob(jsConfigPath+'/GCSConfigJson.json' ,config['Intermediary_Environment_Location'][1],exportJsonPath)
                gcstoBigQuery(jsConfigPath+'/GCSConfigJson.json',jsConfigPath+'/bigQueryAPIEnable.json',config['Intermediary_Environment_Location'][1],config['Output_Environment_Location'])
            except Exception:
                logging.info('gcs to bigquery was failure ')
                step2 = " Step2 "+timestr+" gcs to bigquery was failure \n"
                logging.exception(Exception)
        else:
            logging.raiseException("INVALID INTERMEDIARY VALUES")
    Report = ''.join((step0,step1,step2))

    try:
        uploadBlob_text('report_buck',Report,timestr)
    except:
        logging.info("unable to push to bucket")

if __name__ == "__main__":
    main()
