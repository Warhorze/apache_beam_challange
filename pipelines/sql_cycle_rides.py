
#don't forget to pass the service principle variables 
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

    known_args, pipeline_args = parser.parse_known_args(argv)
    # Add runner, project, and region options directly to pipeline_args
    pipeline_args.extend([
        f'--runner=DataflowRunner',
        f'--project={known_args.project}',
        f'--region={known_args.region}',
        f'--temp_location=gs://{known_args.bucket}/temp',
        f'--staging_location=gs://{known_args.bucket}/staging',
        f'--setup_file=/home/ron/Documents/projects/ml6_challange/src/setup.py'
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    count_query= f"""
        SELECT start_station_id, end_station_id, COUNT(*) as amount_of_rides
        FROM {known_args.bq_table}
        GROUP BY start_station_id, end_station_id
        ORDER BY amount_of_rides DESC
        LIMIT 100; 
        """
    with beam.Pipeline(options=pipeline_options) as p:
        rows = p | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
        query = count_query,
        use_standard_sql = True
        )
        rows | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/{known_args.output}/easy.txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
