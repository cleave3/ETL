import os
import argparse
from dotenv import load_dotenv, find_dotenv
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.mongodbio import MongoClient
from apache_beam.options.pipeline_options import PipelineOptions
from src.load_users import get_user_data
from src.load_user_goals import get_user_goals_data
from src.load_user_modules import get_user_module_data
from src.load_user_activities import get_user_activities_data

# Load from .env file
load_dotenv(find_dotenv())


# Load environment variables
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
PROJECT_ID = os.environ.get("PROJECT_ID")
DATA_SET_ID = os.environ.get("DATA_SET_ID")
TEMP_LOCATION = os.environ.get("TEMP_LOCATION")

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
args, beam_args = parser.parse_known_args()

# Create and set your PipelineOptions.
# For Cloud execution, specify DataflowRunner and set the Cloud Platform
# project, job name, temporary files location, and region.

def beam_options(job_sufix: str) -> PipelineOptions:
    return PipelineOptions(
        beam_args,
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name=f'company-analytics-{job_sufix}',
        temp_location=TEMP_LOCATION,
        region='europe-central2')


def getDB():
    connection_uri = f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}?retryWrites=true&w=majority"
    client = MongoClient(connection_uri)
    db = client[DB_NAME]
    return db


def handleUser():
    user = get_user_data(getDB(), PROJECT_ID, DATA_SET_ID)
    data = user["data"]
    schema = user["schema"]
    spec = user["spec"]

    with beam.Pipeline(options=beam_options("user")) as pipeline:
        # user pipeline
        user_records = pipeline | beam.Create(data)
        user_records | beam.io.WriteToBigQuery(
            spec,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


def handleUserGoals():
    user_goals = get_user_goals_data(getDB(), PROJECT_ID, DATA_SET_ID)
    data = user_goals["data"]
    schema = user_goals["schema"]
    table_spec = user_goals["spec"]

    with beam.Pipeline(options=beam_options("user-goals")) as pipeline:
        # user goals pipeline
        user_goals_records = pipeline | beam.Create(data)
        user_goals_records | beam.io.WriteToBigQuery(
            table_spec,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


def handleUserModules():
    user_modules = get_user_module_data(getDB(), PROJECT_ID, DATA_SET_ID)
    data = user_modules["data"]
    schema = user_modules["schema"]
    table_spec = user_modules["spec"]

    with beam.Pipeline(options=beam_options("user-modules")) as pipeline:
        # user module pipeline
        user_modules_records = pipeline | beam.Create(data)
        user_modules_records | beam.io.WriteToBigQuery(
            table_spec,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

def handleUserActivities():
    user_activities = get_user_activities_data(getDB(), PROJECT_ID, DATA_SET_ID)
    data = user_activities["data"]
    schema = user_activities["schema"]
    table_spec = user_activities["spec"]

    with beam.Pipeline(options=beam_options("user-activities")) as pipeline:
        # user module pipeline
        user_activities_records = pipeline | beam.Create(data)
        user_activities_records | beam.io.WriteToBigQuery(
            table_spec,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


if __name__ == "__main__":
    # handleUser()
    # handleUserGoals()
    # handleUserModules()
    # handleUserActivities()
    pass
