#don't forget to pass the service principle variables 
import argparse
import logging
import re


import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



# Define column names as constants
START_STATION_COL = "start_station_name"
END_STATION_COL = "end_station_name"

def format_output(element):
    """
    Formats the output for each element in the PCollection.

    :param element: A tuple containing the key (start and end station names) and the count.
    :return: A dictionary with formatted output.
    """
    start_station, end_station, count = element
    return {
        START_STATION_COL: start_station,
        END_STATION_COL: end_station,
        "amount_of_rides": count
    }

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', dest='project',required=True, help='GCP Project ID')
    parser.add_argument('--region', dest='region',required=True, help='GCP Region')
    parser.add_argument('--bucket', dest= 'bucket',required=True, help= "bucket to store the data")
    parser.add_argument('--bq_table', dest='bq_table',required=True, help='BigQuery Table')
    parser.add_argument('--output', dest='output',default= 'output', help='Output file to write results to.')
    parser.add_argument('--top_n', dest="top_n",default= 100000, help='N top trips to select')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        f'--runner=DataflowRunner',
        f'--project={known_args.project}',
        f'--region={known_args.region}',
        f'--temp_location=gs://{known_args.bucket}/temp',
        f'--staging_location=gs://{known_args.bucket}/staging',
         f'--setup_file=/home/ron/Documents/projects/ml6_challange/pipelines/setup.py'#tried to set-up on gcp stroage
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    table_query= f"""
        SELECT {START_STATION_COL}, {END_STATION_COL}
        FROM {known_args.bq_table}"""

    with beam.Pipeline(options=pipeline_options) as pipeline:
        rides = pipeline | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
        query = table_query,
        use_standard_sql = True
        )

        total_rides = (
            rides
            | 'Get key column' >> beam.Map(lambda x: (x[START_STATION_COL], x[END_STATION_COL]))
            | 'Count elements per trip' >> beam.combiners.Count.PerElement()
            | 'Format Output' >> beam.Map(format_output)
            | 'Top N Trips' >> beam.transforms.combiners.Top.Of(known_args.top_n, key=lambda x: x["amount_of_rides"]) # this is not a very scalable solution.
            | "Flatten to dicts" >> beam.FlatMap(lambda x: x) \
            | "Rename Columns" >> beam.Map(lambda x: {START_STATION_COL: x[0][1],END_STATION_COL: x[0][0], "amount_of_rides" : x[1]})
        )

        total_rides | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}", file_name_suffix=".txt")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()