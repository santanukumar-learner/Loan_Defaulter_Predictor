import os
import sys
import json
import certifi
from dotenv import load_dotenv
import pandas as pd
import pymongo
from src.Exception.exception import CustomException
from src.logging.logger import logging

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

ca = certifi.where()

class DataExtractor:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise CustomException(e, sys)

    def csv_to_json(self, file_path: str):
        try:
            df = pd.read_csv(file_path)
            df.reset_index(drop=True, inplace=True)
            records = df.to_dict(orient="records")  
            return records
        except Exception as e:
            raise CustomException(e, sys)

    def insert_to_mongo(self, records, database, collection, batch_size=500):
        try:
            if not isinstance(records, list):
                raise ValueError("Records must be a list of dictionaries.")

            records = [dict(record) if not isinstance(record, dict) else record for record in records]

            self.mongo_client = pymongo.MongoClient(
                    MONGO_DB_URL,
                    tlsCAFile=ca,
                    serverSelectionTimeoutMS=60000,  
                    socketTimeoutMS=60000,           
                    connectTimeoutMS=60000          
                )

            self.database = self.mongo_client[database]
            self.collection = self.database[collection]

      
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                self.collection.insert_many(batch)
                print(f"Inserted batch {i // batch_size + 1}")

            return len(records)
        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    FILE_PATH = "data/lending_club_loan_two.csv" 
    DATABASE = "lending_club"
    COLLECTION = "loan_two"
    
    dataobj = DataExtractor()
    
    records = dataobj.csv_to_json(FILE_PATH)
    print("First 2 records:", records[:2])  
    no_of_records = dataobj.insert_to_mongo(records, DATABASE, COLLECTION)
    print(f"Inserted {no_of_records} records into MongoDB")
