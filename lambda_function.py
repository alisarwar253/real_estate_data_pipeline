import boto3
import pandas as pd
import numpy as np
import json
import re
from io import StringIO
from slugify import slugify
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from elasticsearch import Elasticsearch, helpers
import os

# Rename map
RENAME_MAP = {
    "propertyStatus": "status",
    "numberOfBeds": "bedrooms",
    "numberOfBaths": "bathrooms",
    "sqft": "square_feet",
    "addr1": "address_line_1",
    "addr2": "address_line_2",
    "streetNumber": "street_number",
    "streetName": "street_name",
    "streetType": "street_type",
    "preDirection": "pre_direction",
    "unitType": "unit_type",
    "unitNumber": "unit_number",
    "zipcode": "zip_code",
    "propertyType": "property_type",
    "yearBuilt": "year_built",
    "presentedBy": "presented_by",
    "brokeredBy": "brokered_by",
    "realtorMobile": "presented_by_mobile",
    "sourcePropertyId": "mls",
    "openHouse": "open_house",
    "compassPropertyId": "compass_property_id",
    "pageLink": "page_link"
}

STATUS_MAPPING = {
    "Active Under Contract": "Pending",
    "New": "Active",
    "Closed": "Sold"
}

s3 = boto3.client('s3')

def transform(df):
    # renaming the columns
    df = df.rename(columns=RENAME_MAP)

    # status standardization
    df['status'] = df['status'].replace(STATUS_MAPPING)
    df['status'] = df['status'].where(pd.notnull(df['status']), None)

    # present_by name parsing
    df['presented_by'] = df['presented_by'].replace(['nan', 'NaN'], '')
    df[['presented_by_first_name','presented_by_middle_name','presented_by_last_name']] = (
        df['presented_by'].fillna('').apply(lambda x: pd.Series(x.split(' ', 2)))
    )

    # open_house JSON extraction
    def parse_open_house(val):
        try:
            if pd.isna(val) or val in ['nan', 'NaN', None, '']:
                return pd.Series([None, None, None])
            data = json.loads(val)
            if isinstance(data, list) and len(data) > 0:
                first = data[0]
                start_time = first.get('startTimeMillis')
                contact = first.get('contact', {})
                company = contact.get('company')
                contact_name = contact.get('contactName')
                return pd.Series([start_time, company, contact_name])
        except Exception:
            pass
        return pd.Series([None, None, None])

    df[['oh_startTime', 'oh_company', 'oh_contactName']] = df['open_house'].apply(parse_open_house)
    df.drop(columns=['open_house'], inplace=True)

    # mobile number limitation to 10 digits
    df['presented_by_mobile'] = df['presented_by_mobile'].astype(str).replace(['nan', 'NaN', 'None'], '').str.replace(r'\D','',regex=True).str[-10:]
    
    # split email into multiple columns
    def split_email(email):
        if pd.isna(email) or email.strip() == '':
            return pd.Series([None, None], index=['email_1','email_2'])
        parts = email.split(',', 1)
        if len(parts) == 1:
            parts.append(None)
        return pd.Series(parts, index=['email_1','email_2'])

    df[['email_1','email_2']] = df['email'].apply(split_email)

    # Complete address generation
    df['full_address'] = df[['address_line_1','address_line_2','city','state','zip_code']].replace({np.nan: '', 'nan': ''}).fillna('').astype(str).agg(', '.join, axis=1)
    
    # Id generation
    df['id'] = df.apply(
        lambda x: slugify(
            f"{x.get('mls', '')}-{x.get('address_line_1', '')}-{x.get('city', '')}-{x.get('state', '')}-{x.get('zip_code', '')}"
        ) if pd.notnull(x.get('mls')) else None,
        axis=1
    )    
    df = df.replace({np.nan: None, 'nan': None, 'NaN': None, pd.NaT: None})
    return df

# lambda event handler
def lambda_handler(event, context):
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    # converting to pd dataframe
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')), skip_blank_lines=True, engine="python").replace(r'^\s*$', pd.NA, regex=True).dropna(how='all')

    df = transform(df)
    df = df.dropna(how = 'all')

    sf_columns = [
    'id','mls','compass_property_id',
    'status','price','bedrooms','bathrooms','square_feet','property_type','year_built',
    'address_line_1','address_line_2','street_number','street_name','street_type','pre_direction',
    'unit_type','unit_number','city','state','zip_code','latitude','longitude','full_address',
    'presented_by','presented_by_first_name','presented_by_middle_name','presented_by_last_name','presented_by_mobile',
    'brokered_by','listing_office_id','listing_agent_id',
    'email','email_1','email_2','list_date','pending_date','scraped_date',
    'oh_startTime','oh_company','oh_contactName','page_link']

    df_sf = df[sf_columns].copy()

    df_sf = df_sf.replace({np.nan: None, pd.NaT: None, 'nan': None, 'NaN': None, 'None': None})
    df_sf = df_sf.dropna(how='all').reset_index(drop=True)
    
    print(f"Cleaned dataframe shape: {df_sf.shape}")
    print(df_sf.head(3).to_dict())

    # Snowflake connection through environment variables
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse='COMPUTE_WH',
        database='REAL_ESTATE_DB',
        schema='REAL_ESTATE_SCHEMA'
    )

    conn.cursor().execute("TRUNCATE TABLE REAL_ESTATE_SCHEMA.TRANSACTIONS")
    df_sf.columns = [c.upper() for c in df_sf.columns]
    success, nchunks, nrows, _ = write_pandas(conn, df_sf, "TRANSACTIONS")
    print(f"Successfully inserted {nrows} rows into TRANSACTIONS (in {nchunks} chunks).")

    conn.close()

    # Indexing data in Elastic Search
    for col in ['LIST_DATE', 'PENDING_DATE', 'SCRAPED_DATE']:
        if col in df_sf.columns:
            df_sf[col] = df_sf[col].astype(str).replace({'NaT': None, 'nan': None, 'NaN': None, 'None': None})

    df_sf['PROPERTY_TYPE'] = df_sf['PROPERTY_TYPE'].astype(str).replace({'nan': None, 'NaN': None, 'None': None})
    df_sf = df_sf.replace({np.nan: None, pd.NaT: None, 'nan': None, 'NaN': None, 'None': None})

    es = Elasticsearch(cloud_id=os.environ['ELASTIC_CLOUD_ID'],
                       basic_auth=('elastic', os.environ['ELASTIC_PASSWORD']))
    actions = [
        {"_index": "real_estate_index_map", "_id": r['ID'], "_source": r.to_dict()}
        for _, r in df_sf.iterrows()
    ]
    helpers.bulk(es, actions)
