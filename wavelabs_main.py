import sys
#sys.path.append("""/config.json""")
### In future, JSconfig needed for authentication of credential to GCS
from UploadToGCP import uploadBlob
from DownloadFromGCP import downloadBlob
from ramandataingest import spcToJsonConverter
from ramandataingest import directoryContainingSpcFiles
from gcstobigquery import gcstoBigQuery
#Libraries needed to Run the Function
from google.cloud import storage
from google.cloud import storage
import spc
import json
import os
import logging
import pandas as pd
from datetime import datetime
from log import log

log_path = os.getcwd()
datestring = datetime.strftime(datetime.now(), '%Y_%m_%d')
log_file_name = str(log_path)+'\log\MainLogs'+'_'+str(datestring)+'.log'
logging.basicConfig(filename=log_file_name,level=logging.INFO,format="%(asctime)s:%(levelname)s,%(lineno)d:%(name)s.%(funcName)s:%(message)s")
logging.info('Logging Started')
print("...files are getting logged...")

#Default config
config = {
    #Input Location
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

    #Recording logging data
    # log_path = os.getcwd()
    # log(log_path)

#need to comment it from 49-64
    Process_Control= {
    "Input_Environment": {
        'Type': os.getenv("Input_Environment_Type"),    #Possible Values bigquery,gcs,local
        'Location': os.getenv("Input_Environment_Location")
    },
    "Intermediary_Environment":{
        'Required': os.getenv("Required"),# Possible Values Yes/No
        'No_Of_Intermediates': os.getenv("No_Of_Intermediates"), #Possible Values 0,1,2. For now 2 is max limit
        'Type': os.getenv("Intermediary_Environment_Type").split(','), #[###could be array of strings.],
        'Location': os.getenv("Intermediary_Environment_Location").split(',') #[###could be array of strings.]
    },
    "Output_Environment": {
        'Type': os.getenv("Output_Environment_Type"),
        'Location': os.getenv("Output_Environment_Location")
    },
    }

    config['Input_Environment_Type']=  os.getenv("Input_Environment_Type")
    config['Input_Environment_Location'] = os.getenv("Input_Environment_Location")
    #InterMediary Location
    config['Required'] =  os.getenv("Required")
    config['No_Of_Intermediates'] = os.getenv("No_Of_Intermediates")
    config['Intermediary_Environment_Type'] =  os.getenv("Intermediary_Environment_Type").split(',')
    config['Intermediary_Environment_Location'] =  os.getenv("Intermediary_Environment_Location").split(',')
    #OutPut Location
    config['Output_Environment_Type'] =  os.getenv("Output_Environment_Type")
    config['Output_Environment_Location']  = os.getenv("Output_Environment_Location")


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


    # if (Process_Control['Input_Environment']['Type']=='local') & (Process_Control['Output_Environment']['Type']=='local'):
    if (config['Input_Environment_Type']=='local') & (config['Output_Environment_Type']=='local'):
        logging.info('local to local was successful')

        try:

            directoryContainingSpcFiles(config['Input_Environment_Location'],config['Output_Environment_Location'])
        except Exception:
            logging.info('local to local was failure')
            logging.exception(Exception)
    elif (config['Input_Environment_Type']=='gcs') & (config['Output_Environment_Type']=='gcs'):
        
        if (config['Required']=='yes') & (config["No_Of_Intermediates"]=='1'):
            logging.info('gcs to gcs was successful')
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
                logging.info('gcs to gcs was failure')
                logging.exception(Exception)
        else:
            logging.raiseException("INVALID INTERMEDIARY VALUES")
    ##Process_Control['Input_Environment']['Location'][0]-->Local
    ##Process_Control['Input_Environment']['Location'][1]-->GCS
    elif (config['Input_Environment_Type']=='gcs') & (config['Output_Environment_Type']=='bigquery'):
        if (config['Required']=='yes') & (config["No_Of_Intermediates"]=='2'):
            logging.info('gcs to bigquery was successful')
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
                logging.info('gcs to bigquery was failure')
                logging.exception(Exception)
        else:
            logging.raiseException("INVALID INTERMEDIARY VALUES")

if __name__ == "__main__":
    main()
