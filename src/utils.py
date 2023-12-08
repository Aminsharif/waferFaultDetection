import os
import sys

from src.validation.train_validation import row_data_validation
from src.exception import CustomException
from src.logger import logging
from src.component import data_transformation
from src.component import data_ingestion

class train_val():
    def __init__(self, path):
        self.raw_data = row_data_validation.data_validation(path)
        self.dataTransform = data_transformation.dataTransform()
        self.dBOperation = data_ingestion.dBOperation()

    def validation(self):
        try:
            logging.info("Started file validation")
             # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()

            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()

             # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)

             # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            logging.log(self.file_object, "Raw Data Validation Complete!!")

            logging.log(self.file_object, "Starting Data Transforamtion!!")

             # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            logging.log("Data transformation completed")
            
            logging.log("Creating Training_Database and tables on the basis of given schema!!!")

            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Training', column_names)
            logging.log("Table creation Completed!!")
            logging.log("Insertion of Data into Table started!!!!")

            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Training')
            logging.log("Insertion in Table completed!!!")
            logging.log("Deleting Good Data Folder!!!")

            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            logging.log("Good_Data folder deleted!!!")
            logging.log("Moving bad files to Archive and deleting Bad_Data folder!!!")

            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            logging.log("Bad files moved to archive!! Bad folder Deleted!!")
            logging.log("Validation Operation completed!!")
            logging.log("Extracting csv file from table")

            # export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv('Training')

        except Exception as e:
            logging.info("Exceception occure in in validation method")
            raise CustomException(e, sys)
        
    
            
