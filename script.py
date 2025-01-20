import pandas as pd
import numpy
import requests
import json
import csv
import psycopg2
import datetime as dt


#extracting bof data

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": "ce3693686amshd8a81ba62ce435ep1113afjsn137d2b9aa172",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

data = response.json()

#saving the data as a file

filename = 'property.json'

# to write the file into the filename
with open (filename, 'w') as file:
    json.dump(data,file, indent = 4)

# to read into a dataframe
property_df = pd.read_json('property.json')


# TRANSFORMING THE DATA 
# Extract taxAssessment2021_Value
# Extract taxAssessment2021_Values
property_df['taxAssessment2021_Values'] = property_df['taxAssessment'].apply(
    lambda x: x.get('2021', {}).get('value') if isinstance(x, dict) else None
)

# Extract propertyTaxes2021_Total
property_df['propertyTaxes2021_Total'] = property_df['propertyTaxes'].apply(
    lambda x: x.get('2021', {}).get('total') if isinstance(x, dict) else None
)

# Extract taxAssessment2022_Values
property_df['taxAssessment2022_Values'] = property_df['taxAssessment'].apply(
    lambda x: x.get('2022', {}).get('value') if isinstance(x, dict) else None
)

# Extract propertyTaxes2022_Total
property_df['propertyTaxes2022_Total'] = property_df['propertyTaxes'].apply(
    lambda x: x.get('2022', {}).get('total') if isinstance(x, dict) else None
)

# Extract taxAssessment2023_Values
property_df['taxAssessment2023_Values'] = property_df['taxAssessment'].apply(
    lambda x: x.get('2023', {}).get('value') if isinstance(x, dict) else None
)

# Extract propertyTaxes2023_Total
property_df['propertyTaxes2023_Total'] = property_df['propertyTaxes'].apply(
    lambda x: x.get('2023', {}).get('total') if isinstance(x, dict) else None
)

# extract ownerName from owner dictionary and create a new column 
property_df['ownerName'] = property_df['owner'].apply(lambda x: x['names'][0] if isinstance(x, dict) and isinstance(x['names'], list) and x['names'] else None)


property_df['features'] = property_df['features'].apply(json.dumps)

# second approach to find and replace the nulls
property_df.fillna({
        'bedrooms': 0,
        'ownerNames':  'Unknown',
        'addressLine2': 'Not available', 
        'squareFootage': 0,
        'yearBuilt': 0,
        'features': 'None',
        'assessorID': 'Unknown',
        'legalDescription': 'Not available',
        'subdivision': 'Not available', 
         'zoning': 'Unknown', 
         'bathrooms': 0, 
         'lotSize': 0,
         'propertyType': 'Unknown', 
         'taxAssessment': 'Not available',
        'propertyTaxes':  'Not available', 
         'lastSalePrice': 0,
        'lastSaleDate': 0,
        'ownerOccupied': 0,
        'addressLine2': 'Unknown',
        'county': 'Not available'}, inplace =True)

# to extract the id from the address
property_df['id'] = property_df['id'].apply(lambda x: id(x))

#Transforming the lastSaleDate to year, month, monthName and quarter
property_df['lastSaleDate'] = pd.to_datetime(property_df['lastSaleDate'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
property_df= property_df.dropna(subset=['lastSaleDate'])
property_df['year'] = property_df['lastSaleDate'].dt.year
property_df['month'] = property_df['lastSaleDate'].dt.month
property_df['monthName'] = property_df['lastSaleDate'].dt.month_name()
property_df['quarter'] = property_df['lastSaleDate'].dt.quarter

#Transforming features dataset and creating an id for the table
features_dim = property_df[['features', 'propertyType', 'zoning']].drop_duplicates().reset_index(drop=True)
features_dim['features_id'] = features_dim.index +1
property_df = property_df.merge(
    features_dim[['features_id','features', 'propertyType', 'zoning']],  # Bring sales_id into propertyrecords_df
    on=['features', 'propertyType', 'zoning'],  # Match on shared columns
    how='left'
)


#Transforming legal dataset and creating an id for the table
legal_dim = property_df[['legalDescription', 'subdivision']].drop_duplicates().reset_index(drop=True)
legal_dim['legal_id'] = legal_dim.index +1
property_df = property_df.merge(
    legal_dim[['legal_id','legalDescription', 'subdivision']],  # Bring sales_id into propertyrecords_df
    on=['legalDescription', 'subdivision'],  # Match on shared columns
    how='left'
)


#Transforming location dataset and creating an id for the table
location_dim = property_df[['county','zipCode','formattedAddress','state','city']].drop_duplicates().reset_index(drop=True)
location_dim['location_id'] = location_dim.index +1
property_df = property_df.merge(
    location_dim[['location_id','county','zipCode','formattedAddress','state','city']],  # Bring sales_id into propertyrecords_df
    on=['county','zipCode','formattedAddress','state','city'],  # Match on shared columns
    how='left'
)


#Transforming features dataset and creating an id for the table
date_dim = property_df[['lastSaleDate', 'year', 'month',	'monthName', 'quarter']].drop_duplicates().reset_index(drop=True)
date_dim['date_id'] = date_dim.index +1

property_df = property_df.merge(
    date_dim[['date_id','lastSaleDate', 'year', 'month',	'monthName', 'quarter']],  # Bring sales_id into propertyrecords_df
    on=['lastSaleDate', 'year', 'month',	'monthName', 'quarter'],  # Match on shared columns
    how='left'
)

#Transforming owner dataset and creating an id for the table
owner_dim = property_df[['ownerName','ownerOccupied']].drop_duplicates().reset_index(drop=True)
owner_dim['owner_id'] = owner_dim.index +1
# Merge features_dim into propertyrecords_df
property_df = property_df.merge(
    owner_dim[['owner_id','ownerName','ownerOccupied']],  # Use 'feature_id' consistently
    on=['ownerName','ownerOccupied'],  # Match on shared columns
    how='left'
)

# creating the fact table 
fact_table = property_df[['id', 'date_id','owner_id', 'features_id', 'location_id', 'bedrooms', 'squareFootage', 'taxAssessment2021_Values','propertyTaxes2021_Total',
                'taxAssessment2022_Values','propertyTaxes2022_Total', 'taxAssessment2023_Values', 'propertyTaxes2023_Total','bathrooms', 'lotSize', 'lastSalePrice', 'longitude', 'latitude']]




# converting list to a DataFrame
fact_table = pd.DataFrame(fact_table)
owner_dim = pd.DataFrame(owner_dim)
legal_dim = pd.DataFrame(legal_dim)
location_dim = pd.DataFrame(location_dim)
date_dim = pd.DataFrame(date_dim)
features_dim = pd.DataFrame(features_dim)

# saving dataset as csv
owner_dim.to_csv('data/owners_dimension.csv', index=False)
legal_dim.to_csv('data/legal_dimension.csv', index=False)
location_dim.to_csv('data/location_dimension.csv', index = False)
date_dim.to_csv('data/date_dimension.csv', index = False)
features_dim.to_csv('data/features_dimension.csv', index = False)
fact_table.to_csv('data/property_fact.csv', index=False)

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database 'Zapco_db' on localhost:5432.

    Returns:
        connection: A connection object to the database.

    Raises:
        OperationalError: Unable to connect to the database.
        Exception: An unexpected error occurred.
    """
    try:
        connection = psycopg2.connect(
            host='localhost',
            port=5432,
            user='postgres',
            password='your password',
            database='zipco_db'
        )
        print("Database connection successful!")
        return connection
    except psycopg2.OperationalError as e:
        print("OperationalError: Unable to connect to the database.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None
    
get_db_connection()

def create_tables():
    """
    Creates the necessary tables in the database if they do not already exist.

    The tables are:

    - zipco.location_dim: contains information about the location of a property
    - zipco.date_dim: contains information about the date a property was sold
    - zipco.features_dim: contains information about the features of a property
    - zipco.legal_dim: contains information about the legal description of a property
    - zipco.owners_dim: contains information about the owner of a property
    - zipco.fact_table: contains the fact data for the properties

    The function first creates the schema if it does not already exist, then creates the tables.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Separate the schema creation
    create_schema_query = '''
        CREATE SCHEMA IF NOT EXISTS zipco;
    '''

    # Create tables query
    create_table_query = '''
        DROP TABLE IF EXISTS zipco.fact_table CASCADE;
        DROP TABLE IF EXISTS zipco.location_dim CASCADE;
        DROP TABLE IF EXISTS zipco.owners_dim CASCADE;
        DROP TABLE IF EXISTS zipco.legal_dim CASCADE;
        DROP TABLE IF EXISTS zipco.date_dim CASCADE;
        DROP TABLE IF EXISTS zipco.features_dim CASCADE;

        CREATE TABLE zipco.location_dim (
            county VARCHAR(300),  
            zipCode INTEGER,     
            formattedAddress TEXT,  
            state VARCHAR(200),    
            city VARCHAR(200),     
            location_id INT PRIMARY KEY  
        );

        CREATE TABLE zipco.date_dim (
            lastSaleDate DATE,     
            year INTEGER,         
            month INTEGER,        
            monthName TEXT,       
            quarter INTEGER,      
            date_id INT PRIMARY KEY  
        );

        CREATE TABLE zipco.features_dim (
            features TEXT,        
            propertyType TEXT,    
            zoning TEXT,         
            features_id INT PRIMARY KEY  
        );

        CREATE TABLE zipco.legal_dim (
            legalDescription TEXT,  
            subdivision TEXT,      
            legal_id INT PRIMARY KEY  
        );

        CREATE TABLE zipco.owners_dim (
            ownerName TEXT,       
            ownerOccupied FLOAT,  
            owner_id INT PRIMARY KEY  
        );

        CREATE TABLE zipco.fact_table (
            id BIGINT PRIMARY KEY,  
            date_id INT REFERENCES zipco.date_dim(date_id) ON DELETE CASCADE,
            features_id INT REFERENCES zipco.features_dim(features_id) ON DELETE CASCADE,
            location_id INT REFERENCES zipco.location_dim(location_id) ON DELETE CASCADE,
            owner_id INT REFERENCES zipco.owners_dim(owner_id) ON DELETE CASCADE,
            legal_id INT REFERENCES zipco.legal_dim(legal_id) ON DELETE CASCADE,
            bedrooms FLOAT,        
            squareFootage FLOAT,   
            bathrooms FLOAT,       
            lotSize FLOAT,         
            lastSalePrice FLOAT,  
            taxAssessment2021_Values FLOAT,  
            propertyTaxes2021_Total FLOAT,  
            taxAssessment2022_Values FLOAT,  
            propertyTaxes2022_Total FLOAT,  
            taxAssessment2023_Values FLOAT,  
            propertyTaxes2023_Total FLOAT,  
            longitude FLOAT,       
            latitude FLOAT         
        );
    '''

    try:
        print("Creating schema...")
        cursor.execute(create_schema_query)
        
        print("Creating tables...")
        cursor.execute(create_table_query)
        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error occurred during table creation: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")

# Run the function
create_tables()

def load_data(csv_path, table_name, column_name):
    """
    Load the data from a given CSV file into a table in the database.

    Args:
        csv_path (str): The path to the CSV file containing the data.
        table_name (str): The name of the table to load the data into.
        column_name (list[str]): The names of the columns in the table, in the order they appear in the CSV file.

    Returns:
        None
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Replace 'Not available' or empty string with None for null values
            row = [None if cell in ('', 'Not available', ' ') else cell for cell in row]
            
            # Prepare placeholders for the insert statement
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} ({", ".join(column_name)}) VALUES ({placeholders});'
            
            # Execute the query with the row data
            cursor.execute(query, row)

    # Commit the changes to the database
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data loaded successfully into {table_name}.")

#  Load data for the owner table
owner_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/owners_dimension.csv'
load_data(owner_csv_path, 'zipco.owners_dim', ['ownerName', 'ownerOccupied','owner_id'])

# # Load date into the legal table
legal_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/legal_dimension.csv'
load_data(legal_csv_path, 'zipco.legal_dim',['legalDescription','subdivision', 'legal_id'])

##  Load data for the features table
features_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/features_dimension.csv'
load_data(features_csv_path, 'zipco.features_dim',['features', 'propertyType', 'zoning','features_id'])


# Load data for the location table
location_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/location_dimension.csv'
load_data(location_csv_path, 'zipco.location_dim', ['county', 'zipCode', 'formattedAddress', 'state', 'city','location_id'])

# -- Load data for the date table
date_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/date_dimension.csv'
load_data(date_csv_path, 'zipco.date_dim',['lastSaleDate','year','month','monthName','quarter','date_id'])

# Load data for the fact table
fact_columns = ['id', 'date_id','owner_id', 'features_id', 'location_id', 'bedrooms', 'squareFootage', 'taxAssessment2021_Values','propertyTaxes2021_Total',
                'taxAssessment2022_Values','propertyTaxes2022_Total', 'taxAssessment2023_Values', 'propertyTaxes2023_Total','bathrooms', 'lotSize', 'lastSalePrice', 'longitude', 'latitude']

fact_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/property_fact.csv'
load_data(fact_csv_path, 'zipco.fact_table', fact_columns)

print('congratulations Zipco api successfully extracted and loaded')