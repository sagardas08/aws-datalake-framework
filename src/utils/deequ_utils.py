import json
from io import StringIO

from pyspark.sql import SparkSession, Row
import boto3


# utility function that returns spark object
def get_spark():
    spark = (SparkSession
        .builder
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate())
    return spark

# utility function to store Pandas DF to S3
def store_to_s3(type, bucket, key, data):
    if type == 'json':
        s3 = boto3.client('s3')
        json_object = data
        s3.put_object(
             Body=json.dumps(json_object),
             Bucket=bucket,
             Key=key
        )
    elif type == 'dataframe':
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        try:
            s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
            print("stored DF to bucket -> {0} with key -> {1}".format(bucket, key))
        except Exception as e:
            print(e)

def run_profiler(spark, df):
    print("Running Profiler")
    result = ColumnProfilerRunner(spark) \
        .onData(df) \
        .run()
    for col, profile in result.profiles.items():
        print(profile)

def run_constraint_suggestion(spark, df):
    print("Running constraint suggestions")
    suggestionResult = ConstraintSuggestionRunner(spark) \
                 .onData(df) \
                 .addConstraintRule(DEFAULT()) \
                 .run()
     # Json format
    return json.dumps(suggestionResult, indent=2)
