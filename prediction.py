import pandas as pd
import numpy as np
from datetime import datetime
import joblib
import requests
import json

# URL de l'API
# url = "https://real-time-payments-api.herokuapp.com/current-transactions"
# response = requests.get(url)

# data = pd.read_json(response.json(),orient="split")

def prediction(data):

    consumer = data
    # #Data transform
    # consumer = pd.read_json(data,orient="split")
    consumer['current_time'] = pd.to_datetime(consumer['current_time'])
     #finding age
    

    # deriving additonal columns from 'trans_date_trans_time'
    # consumer = record_value
    #deriving hour
    consumer['trans_hour'] = consumer['current_time'].dt.hour
    #deriving 'year'
    consumer['trans_year'] = consumer['current_time'].dt.year
    #deriving 'month'
    consumer['trans_month'] = consumer['current_time'].dt.month
    #deriving 'day of the week'
    consumer['trans_day_of_week'] = consumer['current_time'].dt.weekday

    #converting 'dob' column to datetime
    consumer['dob'] = pd.to_datetime(consumer['dob'])
    consumer['age'] = np.round((consumer['current_time'] - 
                        consumer['dob'])/np.timedelta64(1, 'Y'))
    
    
    #dropping unnecessary columns 
    consumer.drop(['current_time','first', 'last', 'dob'],axis=1,inplace=True)

    df = consumer

    df["gender"] = df["gender"].apply(lambda x : 1 if x == "F" else 0)

    df.drop(['cc_num', 'trans_num', 'merchant','street','city','state','zip','lat','job', 'long','merch_lat','merch_long',
          'category', 'city_pop'],axis=1, inplace=True)
    
    #innput-output split
    X = df.drop(['is_fraud'],axis=1)
    # Reindex the columns of X to match the training data
    # X = X.reindex(columns=X.columns)
    # charger le scaler depuis le fichier
    # with open('scaler1.pkl', 'rb') as f:
        # scaler = joblib.load(f)
    # Load the scaler from the .joblib file
    scaler = joblib.load('scaler1.joblib')
    
    # utiliser le scaler pour effectuer une prédiction
    X = scaler.transform(X)
    # Load the model from the .joblib file
    model_RFS = joblib.load('https://storage.googleapis.com/fraud-detector-dataset/RF_SMOTE1.joblib')
    # with open('RF_SMOTE1.pkl', 'rb') as f_model:
    #     model_RFS = joblib.load(f_model)
    y_test_pred = model_RFS.predict(X)

    return y_test_pred


# print(data, prediction(data))

    
