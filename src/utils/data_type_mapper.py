from pyspark.sql.types import *
from pydeequ.checks import ConstrainableDataTypes


# Spark to Deequ Mapping
SPARK_DEEQU_MAP = {
    BooleanType:  ConstrainableDataTypes.Boolean,
    IntegerType:  ConstrainableDataTypes.Integral,
    ShortType:    ConstrainableDataTypes.Integral,
    LongType:     ConstrainableDataTypes.Integral,
    FloatType:    ConstrainableDataTypes.Numeric,
    DoubleType:   ConstrainableDataTypes.Fractional,
    DecimalType:  ConstrainableDataTypes.Fractional,
    NumericType:  ConstrainableDataTypes.Numeric,
    StringType:   ConstrainableDataTypes.String,
}

SPARK_UI_MAP = {
    BooleanType:  "Boolean",
    IntegerType:  "Integer",
    ShortType:    "ShortInt",
    LongType:     "Integer",
    FloatType:    "Double",
    DoubleType:   "Double",
    DecimalType:  "Decimal",
    NumericType:  "Numeric",
    StringType:   "String",
}

UI_SPARK_MAP = {
    "Integer": IntegerType(),
    "Decimal": DoubleType(),
    "String":  StringType(),
    "Long":    LongType(),
    "Boolean": BooleanType(),
    "Double":  DoubleType(),
    "Float":   FloatType(),
    "Datetime": TimestampType()
}

UI_DQ_MAP = {
    "Integer": ConstrainableDataTypes.Integral,
    "Decimal": ConstrainableDataTypes.Fractional,
    "String":  ConstrainableDataTypes.String,
    "Long":    ConstrainableDataTypes.Integral,
    "Boolean": ConstrainableDataTypes.Boolean,
    "Double":  ConstrainableDataTypes.Numeric,
    "Float":   ConstrainableDataTypes.Numeric,
    "Datetime": None
}





