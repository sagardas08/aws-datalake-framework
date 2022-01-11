import json, decimal
import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.repository import *

def run_constraint_checks(spark, df, return_type='pandas'):
  print("Running constraint checks")
  check = Check(spark, CheckLevel.Warning, "Execute Data Quality Checks")
  checkResult = VerificationSuite(spark) \
  .onData(df) \
  .addCheck(
    check.hasSize(lambda x: x >= 3000000) \
    .hasMin("star_rating", lambda x: x == 1.0) \
    .hasMax("star_rating", lambda x: x == 5.0)  \
    .isComplete("review_id")  \
    .isUnique("review_id")  \
    .isComplete("marketplace")  \
    .isNonNegative("year")) \
  .run()
  print(f"Verification Run Status: {checkResult.status}")
  if return_type.lower() == 'spark':
    result = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
  else:
    result = VerificationResult.checkResultsAsDataFrame(spark, checkResult, pandas=True)
  return result
