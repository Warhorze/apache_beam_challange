#don't forget to pass the service principle variables toe the global env q
import argparse
import logging
import re


import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# Define column names as constants
START_STATION_COL = "start_station_id"
END_STATION_COL = "end_station_id"

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
         f'--setup_file=/home/ron/Documents/projects/ml6_challange/pipelines/setup.py'#tried to set-up on gcp stroage and I know should not be hard coded
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
            | 'Get stations column' >> beam.Map(lambda x: (x[START_STATION_COL], x[END_STATION_COL])) \
            | 'Count elements per ride' >> beam.combiners.Count.PerElement() \
            | 'map key-value pairs' >> beam.Map(lambda x: (x[0], x[1]))  \
            | "Sort by count" >> beam.transforms.combiners.Top.Of(known_args.top_n, key=lambda x: x[1]) \
            | "Flatten to dicts" >> beam.FlatMap(lambda x: x) \
            | "Rename Columns" >> beam.Map(lambda x:(x[0][1], x[0][0],x[1]))

        )
        
<<<<<<< HEAD
<<<<<<< HEAD
        total_rides | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}",
                                                   file_name_suffix=".csv",
                                                   header=f"{START_STATION_COL},{END_STATION_COL}, amount_of_rides")
=======
        total_rides | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}", file_name_suffix=".txt")
>>>>>>> 60a70f6ca2783fda5cd13fbc0748c89d8cbfd956
=======
        total_rides | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}",
                                                   file_name_suffix=".csv",
                                                   header=f"{START_STATION_COL},{END_STATION_COL}, amount_of_rides")
>>>>>>> cf6babe (feat: finished the hard example)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()