from typing import Tuple, Dict
import argparse
import logging
import re


import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from geopy.distance import geodesic



class CalculateDistanceAllRides(beam.DoFn):
    """ Calculate the total distance traveled for all rides """
    def process(self, element: Tuple):
        key, (rides, distances) = element
        total_distance = 0
        total_rides = 0        
        ride_count = rides[0] if isinstance(rides, list) and rides else None
        distance_value = distances[0] if isinstance(distances, list) and distances else None

        if isinstance(ride_count, (int, float)) and isinstance(distance_value, (int, float)):
            total_distance = ride_count * distance_value
            total_rides =ride_count if isinstance(ride_count, (int, float)) else 0
       
        
        
        yield (key, total_rides, total_distance)

class CalculateDistanceBetweenStations(beam.DoFn):
    """Calculate the distance between stations"""
    def process(self, element :Tuple):
        _, values = element
        for v1 in values['pc1']:
            for v2 in values['pc2']:
                distance, name1, name2 = calculate_euclidean_distance(v1, v2)
                yield ((name1, name2), distance)


def calculate_euclidean_distance(point1 : Dict, point2: Dict):
    """Calculate the distance between two coordinate tuples """
    #Hard coded 'id' anoys me should use functtools.partial to prepack the 'name' col
    coord1 = (point1['latitude'], point1['longitude'])
    coord2 = (point2['latitude'], point2['longitude'])
    distance = geodesic(coord1,coord2).kilometers
    return distance, point1['id'], point2['id']
    

def format_csv(elements : Tuple):
    """Format as csv"""
    return ','.join([str(x) for x in elements])  


def remove_none(elements : Tuple):
    """Returns True if containing None values"""
    return all([y is not None for y in elements])  

def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', dest='project',required=True, help='GCP Project ID')
    parser.add_argument('--region', dest='region',required=True, help='GCP Region')
    parser.add_argument('--bucket', dest= 'bucket',required=True, help= "bucket to store the data")
    parser.add_argument('--output', dest='output',default= 'output', help='Output file to write results to.')
    parser.add_argument('--input_col', dest="col",default= 'id', help='N top trips to select')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        f'--runner=DataflowRunner',
        f'--project={known_args.project}',
        f'--region={known_args.region}',
        f'--temp_location=gs://{known_args.bucket}/temp',
        f'--staging_location=gs://{known_args.bucket}/staging',
        f'--setup_file=pipelines/setup.py'#tried to set-up on gcp stroage and i know should not be hard coded
    ])

 

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    START_STATION_COL = f"start_station_{known_args.col}"
    END_STATION_COL = f"end_station_{known_args.col}"
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Count all rides to each station
        cycle_query= f"""
            SELECT {START_STATION_COL}, {END_STATION_COL}
            FROM bigquery-public-data.london_bicycles.cycle_hire;
            """
        
        rides =   pipeline | 'Read cycle_hire From BigQuery' >> beam.io.ReadFromBigQuery(
                        query = cycle_query,
                        use_standard_sql = True
            )
        total_rides = (
                        rides   | 'Get trips column' >> beam.Map(lambda x: (x[START_STATION_COL], x[END_STATION_COL]))
                                | 'Remove none elements' >> beam.Filter(lambda x: remove_none(x)) 
                                | 'Count elements per trip' >> beam.combiners.Count.PerElement() 
            )

        
        station_query= """
            SELECT latitude, longitude, id 
            FROM bigquery-public-data.london_bicycles.cycle_stations;
            """
        
        # Calcualte the distance between each station
        stations = pipeline | 'Read cycle_staiion From BigQuery' >> beam.io.ReadFromBigQuery( 
                            query = station_query,
                            use_standard_sql = True
        )

        pc_with_key =( stations
                            | 'Remove empty elements' >> beam.Filter(lambda x: remove_none(x)) 
                            | 'Add Key' >> beam.Map(lambda x: ('key', x ) )
                    )
        distance = {'pc1': pc_with_key, 'pc2': pc_with_key} \
                                | beam.CoGroupByKey() \
                                | 'Calculate distance' >> beam.ParDo(CalculateDistanceBetweenStations())
        result = (
             (total_rides,  distance)
                | "Group by Key" >> beam.CoGroupByKey()
                | "Calculate Total Distance" >> beam.ParDo(CalculateDistanceAllRides())
            #   | "Sort by total distances" >> beam.transforms.combiners.Top.Of(100, key=lambda x: x[1]) 
            #   | "Flatten to dicts" >> beam.FlatMap(lambda x: x) 
                | "Reformat Output" >> beam.Map(lambda x: format_csv([x[0][0],x[0][1], x[1],x[2]])) 
        )

        result | 'WriteToGCS' >> WriteToText(f"gs://{known_args.bucket}/output/{known_args.output}",
                                              file_name_suffix=".txt",
                                              num_shards=0,
                                              shard_name_template='')
                                            #  header=f"{START_STATION_COL},{END_STATION_COL},amount_of_rides,total_distance_between_stations")
        
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()