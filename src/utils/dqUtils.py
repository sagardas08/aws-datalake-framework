import pydeequ
from pydeequ.profiles import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.repository import *
from .dqConfig import Config


def run_profiler(spark, df):
    print("Running Profiler")
    result = ColumnProfilerRunner(spark).onData(df).run()
    for col, profile in result.profiles.items():
        print(profile)


def run_constraint_suggestion(spark, df):
    print("Running constraint suggestions")
    suggestionResult = (
        ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()
    )
    # Json format
    return json.dumps(suggestionResult, indent=2)


def get_configs(config_file_path):
    """
    Utility method to get Configuration details from a JSON file path
    Replaced by using a Class Based Approach
    :param config_file_path: The path to configuration file
    :return: Dict of configs
    """
    data = None
    with open(config_file_path, "r") as f:
        data = json.load(f)
    f.close()
    return data


def deequ_equivalent(config, check):
    """
    Utility method for finding the corresponding Pydeequ function for a check in O(1) time
    :param config: A dict of configs
    :param check: Kind of check to be performed which will act as the key of the dict
    :return: the corresponding Pydeequ function
    """
    dq_check = None
    try:
        dq_check = config[check]
    except KeyError:
        return None
    return dq_check


def build_constraint(column, assertion, check):
    """
    utility method to build a constraint check based on the column
    :param column: The name of the column
    :param assertion: An optional lambda function
    :param check: The kind of check to be performed -> min / max / primary key etc
    :return: String Deequ constraint object
    """
    deequ_function = Config.config(check)
    if assertion is not None:
        deequ_constraint = f"{deequ_function}('{column}', {assertion})"
    else:
        deequ_constraint = f"{deequ_function}('{column}')"
    return deequ_constraint


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


def generate_code(responses):
    """
    utility method to generate Pydeequ code based on asset metadata
    :param responses: A list of dictionary objects
    :return: String object
    """
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

