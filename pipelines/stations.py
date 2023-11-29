#don't forget to pass the service principle variables 
import argparse
import logging
import re


import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from geopy.distance import geodesic 


# Define column names as constants
START_STATION_COL = "start_station_name"
END_STATION_COL = "end_station_name"

def calculate_euclidean_distance(point1, point2):
    coord1 = (point1['latitude'], point1['longitude'])
    coord2 = (point2['latitude'], point2['longitude'])
    distance = geodesic(coord1,coord2).kilometers
    return distance, point1['name'], point2['name']

class CalculateDist(beam.DoFn):
    def process(self, element):
        _, values = element
        for v1 in values['pc1']:
            for v2 in values['pc2']:
                distance, name1, name2 = calculate_euclidean_distance(v1, v2)
                yield {
                       START_STATION_COL: name1, 
                       END_STATION_COL :  name2,
                      'distance': distance}

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
        f'--setup_file=/home/ron/Documents/projects/ml6_challange/pipelines/setup.py'#tried to set-up on gcp stroage and i know should not be hard coded
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    table_query= f"""
        SELECT latitude, longitude, name 
        FROM {known_args.bq_table};"""
    
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        stations = pipeline | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
        query = table_query,
        use_standard_sql = True
        )

        pc1_with_key = stations | 'AddKey1' >> beam.Map(lambda x: ('key', x ))
        pc2_with_key = stations | 'AddKey2' >> beam.Map(lambda x: ('key', x ))

        cross_join = {'pc1': pc1_with_key, 'pc2': pc2_with_key} | beam.CoGroupByKey()

        distance = (
            cross_join | 'Calculate distance' >> beam.ParDo(CalculateDist()) 
                              | "Create shared key for distance" >> beam.Map(lambda x: ((x[START_STATION_COL], x[END_STATION_COL]),x['distance']))
        )
        distance | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}", file_name_suffix=".txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()