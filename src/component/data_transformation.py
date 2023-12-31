from datetime import datetime
from os import listdir
import pandas
import sys
from src.logger import logging
from src.exception import CustomException


class dataTransform:
    """
        This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

        Written By: Amin Sharif
        Version: 1.0
        Revisions: None
    """
    def __init__(self, ):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
    


    def replaceMissingWithNull(self):
          """
                Method Name: replaceMissingWithNull
                Description: This method replaces the missing values in columns with "NULL" to
                store in the table. We are using substring in the first column to
                keep only "Integer" data for ease up the loading.
                This column is anyways going to be removed during training.

                Written By: Amin Sharif
                Version: 1.0
                Revisions: None

            """

          try:
               onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    csv = pandas.read_csv(self.goodDataPath+"/" + file)
                    csv.fillna('NULL',inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Unnamed: 0'] = csv['Unnamed: 0'].str[6:]
                    csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    logging.info("File Transformed successfully!!")
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
          except Exception as e:
               logging.info("Data Transformation failed because:")
               raise CustomException(e, sys)