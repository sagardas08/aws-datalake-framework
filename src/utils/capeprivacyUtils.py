import cape_privacy as cape
from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import ColumnRedact,DatePerturbation,NumericPerturbation,NumericRounding,Tokenizer
from cape_privacy.spark.transformations.tokenizer import ReversibleTokenizer,TokenReverser
from pyspark import sql
from pyspark.sql import functions

def get_spark_for_masking():
    sess = sql.SparkSession.builder \
        .getOrCreate()
    sess = cape.spark.configure_session(sess)
    return sess
    
def run_data_masking(spark,source_df,metadata):
  #key=get_secret()
  tokenize = Tokenizer()
  for i in metadata:
    for a,b in i.items():
        if a=="req_tokenization" and b==False:
            col_name=i.get("col_nm")
            source_df = source_df.withColumn(col_name, tokenize(functions.col(col_name)))
  return source_df
    
