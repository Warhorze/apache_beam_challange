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

class CalculateTotalDistance(beam.DoFn):
    def process(self, element):
        key, (rides, distances) = element
        total_distance = 0
        
        ride_count = rides[0] if isinstance(rides, list) and rides else None
        distance_value = distances[0] if isinstance(distances, list) and distances else None

        if isinstance(ride_count, (int, float)) and isinstance(distance_value, (int, float)):
            total_distance = ride_count * distance_value
        
        
        yield (key, total_distance)



def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', dest='project',required=True, help='GCP Project ID')
    parser.add_argument('--region', dest='region',required=True, help='GCP Region')
    parser.add_argument('--bucket', dest= 'bucket',required=True, help= "bucket to store the data")
    parser.add_argument('--dist_file', dest='distance',required=True, help='table with')
    parser.add_argument('--rides_file', dest='rides',required=True, help='table with')
    parser.add_argument('--output', dest='output',default= 'output', help='Output file to write results to.')
    parser.add_argument('--top_n', dest="top_n",default= 100000, help='N top trips to select')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        f'--runner=DataflowRunner',
        f'--project={known_args.project}',
        f'--region={known_args.region}',
        f'--temp_location=gs://{known_args.bucket}/temp',
        f'--staging_location=gs://{known_args.bucket}/staging',
        f'--setup_file=/home/ron/Documents/projects/ml6_challange/pipelines/setup.py'#tried to set-up on gcp stroage and i know should not be hard coded
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        rides =  pipeline | "Get unqiue stations from BigQuery" >> beam.io.ReadFromText(file_pattern= f'gs://{known_args.bucket}/output/{known_args.rides}.txt') \
        | "Create Shared Key for  Rides" >> beam.Map(lambda x: ((x[START_STATION_COL], x[END_STATION_COL]), x["amount_of_rides"]))
        distance =  pipeline | "Get unqiue stations from GCP" >> beam.io.ReadFromText(file_pattern= f'gs://{known_args.bucket}/output/{known_args.distance}.txt')
        
        result = (
        ( rides,  distance)
        | "Group by Key" >> beam.CoGroupByKey()
        | "Explode column" >> beam.Map(lambda x: (x[0],( x[1][0], x[1][1])))
        | "Calculate Total Distance" >> beam.ParDo(CalculateTotalDistance())
        | "Sort by total distances" >> beam.transforms.combiners.Top.Of(100, key=lambda x: x[1]) 
        | "Flatten to dicts" >> beam.FlatMap(lambda x: x) \
        | "Rename Columns" >> beam.Map(lambda x: {START_STATION_COL: x[0][1],END_STATION_COL: x[0][0], "amount_of_rides" : x[1]})
        )
        result | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}", file_name_suffix=".txt")
        
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()