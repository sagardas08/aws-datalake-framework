import pydeequ
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.suggestions import *
from pydeequ.repository import *

from .logger import log


PRIMARY_CHECKS = {
    "null": ".isComplete",
    "pk": ".isUnique",
    "data_type": ".hasDataType",
    "max_length": ".hasMaxLength",
}


def run_constraint_suggestion(spark, df):
    print("Running constraint suggestions")
    suggestionResult = (
        ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()
    )
    # Json format
    return json.dumps(suggestionResult, indent=2)


def deequ_equivalent(config, check):
    """
    Utility method for finding the corresponding Pydeequ function for a check in O(1) time
    :param config: A dict of configs
    :param check: Kind of check to be performed which will act as the key of the dict
    :return: the corresponding Pydeequ function
    """
    try:
        dq_check = config[check]
        return dq_check
    except KeyError:
        return None


def build_constraint(column, assertion, check):
    """
    utility method to build a string constraint check based on the column
    :param column: The name of the column
    :param assertion: An optional lambda function
    :param check: The kind of check to be performed -> min / max / primary key etc
    :return: String Deequ constraint object
    """
    dq_func = PRIMARY_CHECKS[check]
    if assertion is not None:
        dq_constraint = f"{dq_func}('{column}', {assertion})"
    else:
        dq_constraint = f"{dq_func}('{column}')"
    return dq_constraint


def dq_dtype(dtype):
    """
    utility method to get the data types as defined in Pydeequ
    :param dtype: Incoming data type
    :return: String
    """
    dtype = dtype.lower().capitalize()
    dq_data_type = f"ConstrainableDataTypes.{dtype}"
    return dq_data_type


def generate_assertion(constraint, value=None):
    """
    utility method to generate assertion function
    :param constraint: the kind of check -> Data type check / Assertion check
    :param value: Value for the lambda func
    :return: String object
    """
    if constraint == "lambda" and value is not None:
        return f"lambda x: x < {value}"
    elif constraint == "data_type":
        return dq_dtype(value)


@log
def generate_code(responses, logger=None, adv_dq_info=None):
    """
    utility method to generate Pydeequ code based on asset metadata
    :return: String object
    """
    # check if advance dq is required or not
    # if it is required, then continue with list of adv_dq rules
    if adv_dq_info:
        check_list = adv_dq_info
    else:
        check_list = list()
    for ob in responses:
        column = ob["col_nm"]
        # Null check
        if ob["nullable"]:
            null_check = build_constraint(column, None, "null")
            check_list.append(null_check)
        # Primary key check
        if ob["pk_ind"]:
            pk_check = build_constraint(column, None, "pk")
            check_list.append(pk_check)
        # Max length check
        if int(ob["col_length"]) != 0:
            length = ob["col_length"]
            len_assertion = generate_assertion("lambda", length)
            length_check = build_constraint(column, len_assertion, "max_length")
            check_list.append(length_check)
        # Data type check
        data_assertion = generate_assertion("data_type", ob["data_type"])
        data_type_check = build_constraint(column, data_assertion, "data_type")
        check_list.append(data_type_check)
    # Creating a unified string of Pydeequ checks
    parsed_checks = "".join(check_list)
    parsed_code = "checkOutput = VerificationSuite(spark).onData(source_df).addCheck(check{0}).run()".format(
        parsed_checks
    )
    return parsed_code
