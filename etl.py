# etl.py
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def extract():
    logging.info("Extracting data from source...")
    # Create the variables to define the path for source and raw data
    source_path = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'datasource', 'data_source.csv')
    raw_path = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'raw', 'data_raw.csv')
    
    data = pd.read_csv(source_path)
    data.to_csv(raw_path, index=False)
    logging.info("Extraction completed.")
    return raw_path

def transform(raw_path):
    logging.info("Transforming and Cleaning data...")
    # Create the variables to define the path for raw and raw curated and stats
    raw_path = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'raw', 'data_raw.csv')
    curated_path = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'curated', 'data_curated.csv')
    stats_path = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'curated', 'stats.csv')
    
    # read raw data
    data = pd.read_csv(raw_path)
    
    # Remove rows with all null values
    data = data.dropna(how='all')
    
    # Create age_group column
    bins = [0, 20, 40, 59, float('inf')]
    labels = ['younger_than_20', 'btw_21_and_40', 'btw_41_and_59', 'older_than_60']
    data['age_group'] = pd.cut(data['AGE'], bins=bins, labels=labels, right=False)
    
    # Save transformed data
    data.to_csv(curated_path, index=False)
    
    # Calculate statistics for specified columns
    stats = data[['YRBLD', 'AGE', 'CENS_POP_DENSITY']].describe().transpose()
    stats.to_csv(stats_path)
    logging.info("Transformation completed.")

    return curated_path, stats_path

def load(curated_path, stats_path):
    logging.info("Loading data...")
    # Create the variables to define the path for curated source and for presentations folders
    data_presentation_directory = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'presentation', 'data_presentation.json')
    stat_presentation_directory = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'presentation', 'stats_presentation.json')
    curated_path = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'curated', 'data_curated.csv')
    stats_path = os.path.join(os.environ['AIRFLOW_HOME'], 'data', 'curated', 'stats.csv')
    
    # Read both curated and stats files 
    data = pd.read_csv(curated_path)
    stats = pd.read_csv(stats_path)

    # Convert stats and data files to json and save in Presentation layer
    data.to_json(data_presentation_directory, orient='records', lines=True)
    stats.to_json(stat_presentation_directory, orient='records', lines=True)
    logging.info("Load completed.")
