import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from .comUtils import get_metadata, dynamodbJsonToDict
from .dqUtils import generate_code
from .logger import Logger
from .validateSchema import validate_schema


class DataAsset:
    def __init__(self, args, config, run_identifier):
        """
        Defines a Data Asset and its properties
        :param args: Run time arguments gathered from Step Function
        :param config: The Global config file
        """
        self.asset_metadata = None
        self.asset_id = args["asset_id"]
        self.source_path = args["source_path"]
        self.source_id = args["source_id"]
        self.exec_id = args["exec_id"]
        self.fm_prefix = config["fm_prefix"]
        self.region = boto3.session.Session().region_name
        self.log_type = config["log_type"]
        self.secret_name = config["secret_name"]
        self.source_file_path = self.source_path.replace("s3://", "s3a://")
        self.logger = Logger(
            log_type=self.log_type,
            log_name=self.exec_id,
            src_path=self.source_path,
            asset_id=self.asset_id,
            region=self.region,
            run_identifier=run_identifier,
        )
        self.dynamo_db = boto3.resource("dynamodb", region_name=self.region)
        items = self.get_data_asset_info()
        self.asset_name = items["asset_nm"]
        self.asset_file_type = items["file_type"]
        self.asset_file_delim = items["file_delim"]
        self.asset_file_header = items["file_header"]
        self.target_id = items["target_id"]
        self.encryption = items["req_encryption"]
        self.metadata_table = f"{self.fm_prefix}.data_asset.{self.asset_id}"
        self.data_catalog = f"{self.fm_prefix}.data_catalog.{self.asset_id}"

    def get_data_asset_info(self):
        table = f"{self.fm_prefix}.data_asset"
        self.logger.write(message=f"Getting asset info from {table}")
        asset_info = self.dynamo_db.Table(table)
        asset_info_items = asset_info.query(
            KeyConditionExpression=Key("asset_id").eq(int(self.asset_id))
        )
        items = dynamodbJsonToDict(asset_info_items)
        return items

    def get_results_path(self):
        return (
            self.source_file_path.split(self.asset_id)[0]
            + f"{self.asset_id}/logs/{self.exec_id}/dq_results"
        )

    def get_error_path(self):
        pass

    def get_masking_path(self):
        return (
            self.source_file_path.split(self.asset_id)[0] + f"{self.asset_id}/masked/"
        )

    def get_asset_metadata(self):
        return get_metadata(self.metadata_table, self.region, logger=self.logger)

    def adv_dq_required(self):
        adv_dq_table = f"{self.fm_prefix}.adv_dq.{self.asset_id}"
        table = self.dynamo_db.Table(adv_dq_table)
        required = None
        try:
            if table.table_status in ["CREATING", "UPDATING", "ACTIVE"]:
                required = True
        except ClientError:
            required = False
        return required

    def generate_dq_code(self):
        metadata = self.get_asset_metadata()
        adv_dq = self.adv_dq_required()
        if adv_dq:
            adv_dq_table = f'{self.fm_prefix}.adv_dq.{self.asset_id}'
            table = self.dynamo_db.Table(adv_dq_table)
            response = table.scan()
            if len(response['Items']):
                # The table exists and contains the adv dq
                check_list = ['.' + i['dq_rule'] for i in response['Items']]
                code = generate_code(metadata, logger=self.logger, adv_dq_info=check_list)
            else:
                # The table exists but is empty
                code = generate_code(metadata, logger=self.logger)
        else:
            code = generate_code(metadata, logger=self.logger)
        self.logger.write(message=f"Pydeequ Code Generated: {code}")
        return code

    def update_data_catalog(
        self, dq_validation=None, data_masking=None, data_standardization=None
    ):
        """
        Updates the data catalog in DynamoDB
        :param dq_validation:
        :param data_masking:
        :param data_standardization:
        :return:
        """
        table = self.dynamo_db.Table(self.data_catalog)
        response = table.get_item(Key={"exec_id": self.exec_id})
        item = response["Item"]
        if dq_validation:
            self.logger.write(
                message=f"updating data catalog entry dq_validation with {dq_validation}"
            )
            item["dq_validation"] = dq_validation
        elif data_masking:
            self.logger.write(
                message=f"updating data catalog entry data_masking with {data_masking}"
            )
            item["data_masking"] = data_masking
        elif data_standardization:
            self.logger.write(
                message=f"updating data catalog entry data_standardization with {data_standardization}"
            )
            item["data_standardization"] = data_standardization
        table.put_item(Item=item)

    def validate_schema(self, source_df):
        """
        Method to return if an asset's schema is validated
        :param source_df:
        :return:
        """
        schema_validation = validate_schema(
            self.asset_file_type,
            self.asset_file_header,
            source_df,
            self.metadata_table,
            self.region,
            logger=self.logger,
        )
        self.logger.write(message=f"Schema Validation = {schema_validation}")
        return schema_validation
