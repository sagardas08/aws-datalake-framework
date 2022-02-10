import cape_privacy as cape
from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import ColumnRedact, DatePerturbation, NumericPerturbation, NumericRounding, \
    Tokenizer
from cape_privacy.spark.transformations.tokenizer import ReversibleTokenizer, TokenReverser
from pyspark import sql
from pyspark.sql import functions
from .logger import log


@log
def get_spark_for_masking(logger=None):
    """
    Utility method to return a spark Session object configured for cape privacy
    :return: Spark Session Object
    """
    sess = sql.SparkSession.builder \
        .getOrCreate()
    sess = cape.spark.configure_session(sess)
    return sess


@log
def run_data_masking(source_df, metadata, key, logger=None):
    """
    Utility method to mask sensitive data
    :return: masked spark dataframe
    """
    tokenize = Tokenizer(key=key)
    perturb_numeric = NumericPerturbation(dtype=dtypes.Integer, min=-10, max=10)
    perturb_date = DatePerturbation(frequency=("YEAR", "MONTH", "DAY"), min=(-10, -5, -5), max=(10, 5, 5))
    round_numeric = NumericRounding(dtype=dtypes.Float, precision=-3)
    for i in metadata:
        for a, b in i.items():
            if a == "req_tokenization" and (str(b) == "True" or str(b) == "true"):
                col_name = i.get("col_nm")
                source_df = source_df.withColumn(col_name, tokenize(functions.col(col_name)))
            if a == "req_redaction" and (str(b) == "True" or str(b) == "true"):
                col_name = i.get("col_nm")
                redact_list = []
                redact_list.append(col_name)
                redact = ColumnRedact(columns=redact_list)
                df = redact(df)
            if a == "req_dateperturbation" and (str(b) == "True" or str(b) == "true"):
                col_name = i.get("col_nm")
                source_df = source_df.withColumn(col_name, perturb_date(functions.col(col_name)))
            if a == "req_numericperturbation" and (str(b) == "True" or str(b) == "true"):
                col_name = i.get("col_nm")
                source_df = source_df.withColumn(col_name, perturb_numeric(functions.col(col_name)))
            if a == "req_numericrounding" and (str(b) == "True" or str(b) == "true"):
                col_name = i.get("col_nm")
                source_df = source_df.withColumn(col_name, round_numeric(functions.col(col_name)))
    logger.write(message="Data masking done successfully")
    return source_df