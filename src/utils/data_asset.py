import boto3
from .comUtils import get_metadata, get_timestamp
from .dqUtils import generate_code
from .logger import Logger
from .validateSchema import validate_schema


class DataAsset:
    def __init__(self, args, config, run_identifier, conn):
        """
        Defines a Data Asset and its properties
        :param args: Run time arguments gathered from Step Function
        :param config: The Global config file
        :param run_identifier: To identify which job is running at present
        :param conn : DB connection object
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
        items = self.get_data_asset_info(conn)
        self.asset_name = items["asset_nm"]
        self.asset_file_type = items["file_type"]
        self.asset_file_delim = items["file_delim"]
        self.asset_file_header = items["file_header"]
        self.target_id = items["target_id"]
        self.encryption = items["file_encryption_ind"]

    def get_data_asset_info(self, conn):
        """
        :param conn: DB connection object
        :return: Data asset info as dictionary
        """
        asset_dict = conn.retrieve_dict(
            "data_asset", cols="all", where=("asset_id=%s", [self.asset_id])
        )
        return asset_dict[0]

    def get_results_path(self):
        """
        Path for storing the DQ results
        :return: s3 uri
        """
        return (
                self.source_file_path.split(self.asset_id)[0]
                + f"{self.asset_id}/logs/{self.exec_id}/dq_results"
        )

    def get_error_path(self):
        pass

    def get_masking_path(self):
        """
        Path for storing the results of Data Masking
        :return: s3 uri
        """
        return (
                self.source_file_path.split(self.asset_id)[0] + f"{self.asset_id}/masked/{get_timestamp(self.source_file_path)}/"
        )

    def get_asset_metadata(self, conn):
        """
        Gets the required metadata for an asset
        :return:
        """
        return get_metadata(conn, self.asset_id, logger=self.logger)

    def adv_dq_required(self, conn):
        """
        The function checks if advanced DQ is required for a particular asset or not
        :param: DB connection object
        :return: Boolean
        """
        dq_rules = conn.retrieve(
            "adv_dq_rules", cols="all", where=("asset_id=%s", [self.asset_id])
        )
        if len(dq_rules) == 0:
            return False
        else:
            return True

    def generate_dq_code(self, conn):
        """
        Generate the Dynamic DQ code
        :param :DB connection object
        :return: Generated code
        """
        metadata = self.get_asset_metadata(conn)
        adv_dq = self.adv_dq_required(conn)
        if adv_dq:
            dq_rules = conn.retrieve_dict(
                "adv_dq_rules", cols="dq_rule", where=("asset_id=%s", [self.asset_id])
            )
            if len(dq_rules):
                # The table exists and contains the adv dq
                check_list = ["." + i["dq_rule"] for i in dq_rules]
                code = generate_code(
                    metadata, logger=self.logger, adv_dq_info=check_list
                )
            else:
                # The table exists but is empty
                code = generate_code(metadata, logger=self.logger)
        else:
            code = generate_code(metadata, logger=self.logger)
        self.logger.write(message=f"Pydeequ Code Generated: {code}")
        return code

    def update_data_catalog(
            self,
            conn,
            dq_validation=None,
            data_masking=None,
            data_standardization=None,
            tgt_file_path=None,
            data_masking_exec_id=None,
            data_standardization_exec_id=None
    ):
        """
        Updates the data catalog in DynamoDB
        """
        table_name = "data_asset_catalogs"
        where_clause = ("exec_id=%s", [self.exec_id])
        item = {}
        if dq_validation:
            self.logger.write(
                message=f"updating data catalog entry dq_validation with {dq_validation}"
            )
            item["dq_validation"] = dq_validation
        if data_masking:
            self.logger.write(
                message=f"updating data catalog entry data_masking with {data_masking}"
            )
            item["data_masking"] = data_masking
        if data_standardization:
            self.logger.write(
                message=f"updating data catalog entry data_standardization with {data_standardization}"
            )
            item["data_standardization"] = data_standardization
        if tgt_file_path:
            self.logger.write(
                message=f"updating data catalog entry tgt_file_path with {tgt_file_path}"
            )
            item["tgt_file_path"] = tgt_file_path
        if data_masking_exec_id:
            self.logger.write(
                message=f"updating data catalog entry data_masking_exec_id with {data_masking_exec_id}"
            )
            item["data_masking_exec_id"] = data_masking_exec_id
        if data_standardization_exec_id:
            self.logger.write(
                message=f"updating data catalog entry data_standardization_exec_id with {data_standardization_exec_id}"
            )
            item["data_standardization_exec_id"] = data_standardization_exec_id

        conn.update(table=table_name, data=item, where=where_clause)

    def validate_schema(self, conn, source_df):
        """
        Method to return if an asset's schema is validated
        :return: Bool
        """
        schema_validation = validate_schema(self, source_df, conn, logger=self.logger)
        self.logger.write(message=f"Schema Validation = {schema_validation}")
        return schema_validation
