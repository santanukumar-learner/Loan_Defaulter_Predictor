import tensorflow as tf
import pandas as pd


class DataTransformation:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def encode_features(self):
        # Map verification_status
        verif_mapping = {"Not Verified": 0, "Source Verified": 1, "Verified": 2}
        if 'verification_status' in self.dataframe.columns:
            self.dataframe['verification_status'] = self.dataframe['verification_status'].map(verif_mapping)

        # Encode sub_grade
        sub_grade_order = [
            'A1', 'A2', 'A3', 'A4', 'A5',
            'B1', 'B2', 'B3', 'B4', 'B5',
            'C1', 'C2', 'C3', 'C4', 'C5',
            'D1', 'D2', 'D3', 'D4', 'D5',
            'E1', 'E2', 'E3', 'E4', 'E5',
            'F1', 'F2', 'F3', 'F4', 'F5',
            'G1', 'G2', 'G3', 'G4', 'G5'
        ]

        sub_grade_mapping = {grade: idx for idx, grade in enumerate(sub_grade_order)}

        # Map the sub_grade value to its corresponding ordinal value
        if 'sub_grade' in self.dataframe.columns:
            self.dataframe['sub_grade'] = self.dataframe['sub_grade'].map(sub_grade_mapping)

        one_hot_columns = {
            'homeOwnership': ['home_ownership_MORTGAGE', 'home_ownership_OWN', 'home_ownership_RENT'],
            'loanTerm': ['term_ 36 months', 'term_ 60 months'],
            'loanPurpose': ['purpose_car', 'purpose_credit_card', 'purpose_debt_consolidation',
                            'purpose_educational', 'purpose_home_improvement', 'purpose_medical',
                            'purpose_other', 'purpose_small_business'],
            'applicationType': ['application_type_DIRECT_PAY', 'application_type_INDIVIDUAL', 'application_type_JOINT']
        }

        for col, encoded_cols in one_hot_columns.items():
            if col in self.dataframe.columns:
                for enc_col in encoded_cols:
                    self.dataframe[enc_col] = (self.dataframe[col] == enc_col.split('_')[-1]).astype(int)
                self.dataframe.drop(columns=[col], inplace=True)

        rename_map = {
            'loanAmount': 'loan_amnt',
            'annualIncome': 'annual_inc',
            'monthlyInstallment': 'installment',
            'interestRate': 'int_rate',
            'creditUtilization': 'revol_util',
            'openCreditLines': 'open_acc',
            'derogatoryRecords': 'pub_rec',
            'revolvingCreditBalance': 'revol_bal',
            'totalCreditAccounts': 'total_acc',
            'publicBankruptcies': 'pub_rec_bankruptcies',
            'dtiRatio': 'debt_to_income_ratio',
            'creditHistoryLength': 'credit_history_length'
        }
        self.dataframe.rename(columns=rename_map, inplace=True)

        # Ensure proper column order for the NN model
        column_order = ['loan_amnt', 'int_rate', 'installment', 'sub_grade', 'annual_inc',
                        'verification_status', 'open_acc', 'pub_rec', 'revol_bal', 'revol_util',
                        'total_acc', 'pub_rec_bankruptcies',
                        'home_ownership_MORTGAGE', 'home_ownership_OWN', 'home_ownership_RENT',
                        'term_ 36 months', 'term_ 60 months',
                        'purpose_car', 'purpose_credit_card', 'purpose_debt_consolidation',
                        'purpose_educational', 'purpose_home_improvement', 'purpose_medical',
                        'purpose_other', 'purpose_small_business',
                        'application_type_DIRECT_PAY', 'application_type_INDIVIDUAL', 'application_type_JOINT',
                        'debt_to_income_ratio', 'credit_history_length']

        # Fill NaN (null) values with 0
        self.dataframe.fillna(0, inplace=True)

        # Reorder columns
        self.dataframe = self.dataframe[column_order]

        return self.dataframe


data = pd.read_csv('shuffled_loan_data.csv')
print(data.columns)
data_class = DataTransformation(data)
array = data_class.encode_features()

model = tf.keras.models.load_model('best_model40.keras')

predictions = model.predict(data_class.encode_features())
print(predictions)

# import pymongo
# from pymongo import MongoClient
# import pandas as pd
#
# MONGO_DB_URL = 'mongodb://localhost:27017/Usersf'
#
# # Establish connection
# class MongoReader:
#     def __init__(self, mongo_db_url: str):
#         self.MONGO_DB_URL = mongo_db_url
#
#     def df_creator_from_mongo(self):
#         client = MongoClient(self.MONGO_DB_URL)
#         db = client['Usersf']
#         collection = db['admindatas']
#         cursor = collection.find({})
#         df = pd.DataFrame(list(cursor))
#         if '_id' in df.columns:
#             df = df.drop('_id', axis=1)
#         client.close()
#
#         return df



# Drop unnecessary columns if needed

