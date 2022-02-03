# imports
import boto3


def get_schema_details(table):
    """

    :param table:
    :return:
    """
    dct = dict()
    response_list = list()
    client = boto3.client('dynamodb', region_name='us-east-1')
    response = client.scan(TableName=table)['Items']
    return response


def order_columns(items):
    """

    :param items:
    :return:
    """
    schema_dict = dict()
    for item in items:
        col_id = item['col_id']['N']
        col_name = item['col_nm']['S']
        schema_dict[col_name] = int(col_id)
    columns = sorted(schema_dict, key=schema_dict.get)
    return columns


def match_in_order(actual, expected):
    """

    :param actual:
    :param expected:
    :return:
    """
    zipped_cols = zip(actual, expected)
    difference = dict()
    match = True
    for item in zipped_cols:
        if len(item[0]) != len(item[1]) or item[0] != item[1]:
            difference[item[0]] = item[1]
            match = False
    return match, difference


def match_without_order(actual, expected):
    """

    :param actual:
    :param expected:
    :return:
    """
    matching = [i for i in actual if i in expected]
    if len(matching) == len(actual) == len(expected):
        return True
    else:
        return False


def match_length(actual, expected):
    """

    :param actual:
    :param expected:
    :return:
    """
    if len(actual) == len(expected):
        return True
    return False


def validate_schema(asset_file_type, asset_file_header, df, metadata_table):
    """
    Target Function to enforce schema validation
    :param asset_file_type:
    :param asset_file_header:
    :param df:
    :param metadata_table:
    :return:
    """
    df_cols = df.columns
    schema_details = get_schema_details(metadata_table)
    columns = order_columns(schema_details)
    actual_cols = [i.lower() for i in df_cols]
    expected_cols = [i.lower() for i in columns]
    result = None
    # If the length of the 2 lists : actual and expected do not match from the start
    if not match_length(actual_cols, expected_cols):
        result = False
    else:
        # For json and parquet files: match the column names of the actual and expected cols
        if asset_file_type == 'json' or asset_file_type == 'parquet':
            result = match_without_order(actual_cols, expected_cols)
        # For CSV files with file header: Match the column name and column order
        elif asset_file_type == 'csv' and asset_file_header:
            result, diff = match_in_order(actual_cols, expected_cols)
            if len(diff) > 0:
                print("The following columns are not matching: ")
                for k, v in diff.items():
                    print(f"Source Col Name: {k} --- Expected: {v}")
    return result
