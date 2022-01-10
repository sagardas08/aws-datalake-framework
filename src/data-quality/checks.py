from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.repository import *

from src.utils.deequ_utils import (get_spark, run_profiler,
                                run_constraint_suggestion, store_to_s3)

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
