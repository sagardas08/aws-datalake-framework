from pyspark.sql.types import *
from pydeequ.checks import ConstrainableDataTypes


# Spark to DQ Mapping
SPARK_DQ_MAP = {
    BooleanType:  ConstrainableDataTypes.Boolean,
    IntegerType:  ConstrainableDataTypes.Integral,
    ShortType:    ConstrainableDataTypes.Integral,
    LongType:     ConstrainableDataTypes.Integral,
    FloatType:    ConstrainableDataTypes.Numeric,
    DoubleType:   ConstrainableDataTypes.Fractional,
    DecimalType:  ConstrainableDataTypes.Fractional,
    StringType:   ConstrainableDataTypes.String,
}

# Spark to UI Mapping
SPARK_UI_MAP = {
    BooleanType:  "Boolean",
    IntegerType:  "Integer",
    ShortType:    "Integer",
    LongType:     "Integer",
    FloatType:    "Double",
    DoubleType:   "Double",
    DecimalType:  "Double",
    StringType:   "String",
    TimestampType: "Datetime"
}

# UI to Spark Mapping
UI_SPARK_MAP = {
    "Integer": IntegerType(),
    "String":  StringType(),
    "Long":    LongType(),
    "Boolean": BooleanType(),
    "Double":  DoubleType(),
    "Datetime": TimestampType()
}

# UI to DQ Map
UI_DQ_MAP = {
    "Integer": ConstrainableDataTypes.Integral,
    "String":  ConstrainableDataTypes.String,
    "Long":    ConstrainableDataTypes.Integral,
    "Boolean": ConstrainableDataTypes.Boolean,
    "Double":  ConstrainableDataTypes.Numeric,
    "Datetime": None
}





