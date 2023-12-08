import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from src.exception import CustomException
from src.logger import logging
import os
import sys

class data_validation():
    """
        this class will be used for call validation for traning data
        written by: amin sharif
        version: 1.0
        Revision: None
    """
    def __init__(self, path):
        self.batch_directory = path
        self.schema_path = "schema_training.json"

        """
            Method_name: valuesFromSchema
            Description: This method extracts all the relevent information form predefine schema_training.json file
            Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            Written: Amin sharif
            Version: 1.0
            Revision: None
        """
    def valuesFromSchema(self):
            try:
                with open(self.schema_path, 'r') as f:
                    dic = json.load(f)
                    f.close()

                    pattern = dic['SampleFileName']
                    LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
                    LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
                    column_names = dic['ColName']
                    NumberofColumns = dic['NumberofColumns']

                    message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
                    logging.log(message)

            except ValueError as e:
                logging.info("ValueError:Value not found inside schema_training.json")
                raise CustomException(e, sys) 

            except KeyError as e:
                logging.info("KeyError:Key value error incorrect key passed")
                raise CustomException(e, sys) 

            except Exception as e:
                logging.info(e)
                raise CustomException(e, sys)   


            return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns 
    
    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None

                                 Written By: Amin Sharif
                                Version: 1.0
                                Revisions: None

                                        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex
    
    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: Amin sharif
                    Version: 1.0
                    Revisions: None

                """

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in listdir(self.batch_directory)]
        try:
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            logging.log("Valid File name!! File moved to GoodRaw Folder")

                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            logging.log("Invalid File Name!! File moved to Bad Raw Folder")
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        logging.log("Invalid File Name!! File moved to Bad Raw Folder")
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    logging.log("Invalid File Name!! File moved to Bad Raw Folder")

        except Exception as e:
            logging.log("Error occured while validating FileName ")
            raise CustomException(e, sys)


    def createDirectoryForGoodBadRawData(self):

            """
                                        Method Name: createDirectoryForGoodBadRawData
                                        Description: This method creates directories to store the Good Data and Bad Data
                                                        after validating the training data.

                                        Output: None
                                        On Failure: OSError

                                        Written By: Amin Sharif
                                        Version: 1.0
                                        Revisions: None

            """

            try:
                path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
                if not os.path.isdir(path):
                    os.makedirs(path)
                path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
                if not os.path.isdir(path):
                    os.makedirs(path)

            except OSError as ex:
                logging.log("Error while creating Directory ")
                raise CustomException(ex, sys)




    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: Amin Sharif
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                logging.log("BadRaw directory deleted before starting validation!!!")
        except OSError as s:
            logging.log("Error while Deleting Directory")
            raise CustomException(s, sys)

    
    def deleteExistingGoodDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made  to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: Amin sharif
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            path = 'Training_Raw_files_validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                logging.log("GoodRaw directory deleted successfully!!!")
        except OSError as s:
            logging.log("Error while Deleting Directory")
            raise CustomException(s, sys)
        
    
    def validateColumnLength(self,NumberofColumns):
        """
                          Method Name: validateColumnLength
                          Description: This function validates the number of columns in the csv files.
                                       It is should be same as given in the schema file.
                                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.
                                      The csv file is missing the first column name, this function changes the missing name to "Wafer".
                          Output: None
                          On Failure: Exception

                           Written By: Amin sharif
                          Version: 1.0
                          Revisions: None

                      """
        try:
            logging.log("Column Length Validation Started!!")
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    logging.log("Invalid Column Length for the file!! File moved to Bad Raw Folder")
            logging.log("Column Length Validation Completed!!")
        except OSError as e:
            logging.log("Error Occured while moving the file")
            raise CustomException(e, sys)
        except Exception as e:
            logging.log("Error Occured")
            raise Exception(e, sys)
