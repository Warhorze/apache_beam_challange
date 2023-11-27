
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# Set your GCP project and region
PROJECT = 'codingchallence'
REGION = 'europe-central2'
BUCKET = 'ml6wra-bucket'

BQ_PROJECT ="bigquery-public-data"
BQ_DATASET = "london_bicycles"
BQ_TABLE = "cycle_hire"



def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        default=output_path,
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Add runner, project, and region options directly to pipeline_args
    pipeline_args.extend([
        f'--runner=DataflowRunner',
        f'--project={PROJECT}',
        f'--region={REGION}',
        f'--temp_location=gs://{BUCKET}/temp',
        f'--staging_location=gs://{BUCKET}/staging'
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    table_spec = bigquery.TableReference(
        projectId=BQ_PROJECT,
        datasetId=BQ_DATASET,
        tableId=BQ_TABLE
    )
    with beam.Pipeline(options=pipeline_options) as p:
        # Read data from BigQuery into a PCollection.
        rows = p | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(table=table_spec)

        row_count = (
            rows
            | 'CountRows' >> beam.combiners.Count.Globally()
            | 'FormatCount' >> beam.Map(lambda count: f'Row count: {count}')
        )

        # Write the output to GCS
        row_count | 'WriteToGCS' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
