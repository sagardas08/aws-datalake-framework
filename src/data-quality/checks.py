import json
from io import StringIO

import boto3
from pyspark.sql import SparkSession, Row

import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.repository import *


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

# utility function that returns spark object
def get_spark():
    spark = (SparkSession
        .builder
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate())
    return spark

def run_analyzer(spark, df, return_type='pandas'):
    print("Running Analyzer")
    analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("review_id")) \
                    .addAnalyzer(ApproxCountDistinct("review_id")) \
                    .addAnalyzer(Mean("star_rating")) \
                    .addAnalyzer(Compliance("top star_rating", "star_rating >= 4.0")) \
                    .addAnalyzer(Correlation("total_votes", "star_rating")) \
                    .addAnalyzer(Correlation("total_votes", "helpful_votes")) \
                    .run()
    if return_type.lower() == 'spark':
        result = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    else:
        result = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult, pandas=True)
    return result

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

def run_constraint_checks(spark, df, return_type='pandas'):
    print("Running constraint checks")
    check = Check(spark, CheckLevel.Warning, "Amazon Electronic Products Reviews")
    checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasSize(lambda x: x >= 3000000) \
        .hasMin("star_rating", lambda x: x == 1.0) \
        .hasMax("star_rating", lambda x: x == 5.0)  \
        .isComplete("review_id")  \
        .isUnique("review_id")  \
        .isComplete("marketplace")  \
        .isContainedIn("marketplace", ["US", "UK", "DE", "JP", "FR"]) \
        .isNonNegative("year")) \
    .run()
    print(f"Verification Run Status: {checkResult.status}")
    if return_type.lower() == 'spark':
        result = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    else:
        result = VerificationResult.checkResultsAsDataFrame(spark, checkResult, pandas=True)
    return result

def main():
    spark = get_spark()
    df = spark.read.parquet("s3a://amazon-reviews-pds/parquet/product_category=Electronics/")
    df.printSchema()
    run_profiler(spark, df)
    analyzer_df = run_analyzer(spark, df)
    constraints_suggestion = run_constraint_suggestion(spark, df)
    check_df = run_constraint_checks(spark, df)
    store_to_s3('dataframe', '2181-datastore', 'pydeequ-output/analyzer.csv', analyzer_df)
    store_to_s3('dataframe', '2181-datastore', 'pydeequ-output/constraint_check.csv', check_df)
    store_to_s3('json', '2181-datastore', 'pydeequ-output/constraing_suggestion.json', constraints_suggestion)
    # SparkSession and Java Gateway teardown
    spark.sparkContext._gateway.close()
    spark.stop()

if __name__ == '__main__':
    main()
