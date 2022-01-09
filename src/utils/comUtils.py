import json, decimal

def create_spark_df(
  spark, source_file_path, asset_file_type,
  asset_file_delim, asset_file_header
):
  if asset_file_type == 'csv' and asset_file_header == True:
    source_df = spark.read.csv(
      path=source_file_path, sep = asset_file_delim,
      header=True,  inferSchema=True
    )
  elif asset_file_type == 'csv' and asset_file_header == False:
    source_df = spark.read.csv(
      path=source_file_path, 
      sep = asset_file_delim
    )
  elif asset_file_type == "parquet":
    source_df = spark.read.parquet(source_file_path)
  elif asset_file_type == "json":
    source_df = spark.read.json(source_file_path)
  elif asset_file_type == "orc":
    source_df = spark.read.orc(source_file_path)
  try:
    assert source_df != None
    return source_df
  except AssertionError:
    print("Unable to read the data from the source")
    
# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, decimal.Decimal):
      return str(o)
    if isinstance(o, set):  #<---resolving sets as lists
      return list(o)
    return super(DecimalEncoder, self).default(o)

def dynamodbJsonToDict(dynamodbJson):
  for i in dynamodbJson['Items']:
    items = json.dumps(i, cls=DecimalEncoder)
  return json.loads(items)
