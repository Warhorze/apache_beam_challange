import argparse
import logging
import re


import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from geopy.distance import geodesic

# Define column names as constants becasue the ids contain nan's and they might ask me to run it with the acutal id's
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
        # Count and sort the total number of rides to each station
        cycle_query= f"""
            SELECT {START_STATION_COL}, {END_STATION_COL}
            FROM bigquery-public-data.london_bicycles.cycle_hire;
            """
        
        rides =   pipeline | 'Read cycle_hire From BigQuery' >> beam.io.ReadFromBigQuery(
                        query = cycle_query,
                        use_standard_sql = True
            )
        total_rides = ( 
            rides      
            | 'Get trips column' >> beam.Map(lambda x: (x[START_STATION_COL], x[END_STATION_COL])) 
            | 'Count elements per trip' >> beam.combiners.Count.PerElement() 
            | 'map key-value pairs' >> beam.Map(lambda x: (x[0], x[1]))  
            | "Sort by count" >> beam.transforms.combiners.Top.Of(known_args.top_n, key=lambda x: x[1]) 
            | "Flatten to dicts" >> beam.FlatMap(lambda x: x) 
            | "Reformat output" >> beam.Map(lambda x: {START_STATION_COL: x[0][1],END_STATION_COL: x[0][0], "amount_of_rides" : x[1]})
            | "Create Share Key for Total Rides" >> beam.Map(  lambda x: ((x[START_STATION_COL], x[END_STATION_COL]), x["amount_of_rides"]))
        )
        station_query= """
            SELECT latitude, longitude, name 
            FROM bigquery-public-data.london_bicycles.cycle_stations;
            """
        # Calcualte the distance between each station
        stations = pipeline | 'Read cycle_staiion From BigQuery' >> beam.io.ReadFromBigQuery( 
                            query = station_query,
                            use_standard_sql = True
        )

        pc1_with_key = stations | 'AddKey1' >> beam.Map(lambda x: ('key', x ))
        pc2_with_key = stations | 'AddKey2' >> beam.Map(lambda x: ('key', x ))

        cross_join = {'pc1': pc1_with_key, 'pc2': pc2_with_key} | beam.CoGroupByKey()

        distance = (
            cross_join | 'Calculate distance' >> beam.ParDo(CalculateDist()) 
                       | "Create shared key for distance" >> beam.Map(lambda x: ((x[START_STATION_COL], x[END_STATION_COL]),x['distance']))
        )
        
        # Multiply the total number rides by the distance between the stations.
        result = (
        ( total_rides,  distance)
        | "Group by Key" >> beam.CoGroupByKey()
        | "Explode column" >> beam.Map(lambda x: (x[0],( x[1][0], x[1][1])))
        | "Calculate Total Distance" >> beam.ParDo(CalculateTotalDistance())
        | "Sort by total distances" >> beam.transforms.combiners.Top.Of(100, key=lambda x: x[1]) 
        | "Transpose" >> beam.FlatMap(lambda x: x) \
        | "Format" >> beam.Map(lambda x: {START_STATION_COL: x[0][1],END_STATION_COL: x[0][0], "total_distance" : x[1]})# this needs to be 
        )
        result | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}", file_name_suffix=".txt")
        
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
