import json
import os
import sys
import time

import boto3

from deploy_s3 import fetch_latest_code, deploy_to_s3, remove_clone_dir
from create_jobs import create_glue_jobs
from deploy_lambda import create_lambda, lambda_function_exists
from create_sfn import create_step_function
from logger import Logger

deploy_logger = Logger()
ROLLBACK_STATES = {
    1: "Fetching Latest Code from Github",
    2: "Deploying the latest code to S3",
    3: "Creating the lambda function",
    4: "Initiating Glue Job creation",
    5: "Creating Step Function",
}


class DeployPipeline:
    def __init__(self, config, path, region):
        self.config = config
        self.fm_prefix = config["fm_prefix"]
        self.project_name = config["project_name"]
        self.region = region
        self.code_bucket_s3 = f"{self.fm_prefix}-code-{self.region}"
        self.clone_path = path
        self.state = 0
        self.rollback = False

    def update_state(self):
        self.state += 1

    def clone_github(self):
        self.update_state()
        clone_status = fetch_latest_code(self.clone_path, self.config)
        if clone_status:
            deploy_logger.write(message="Fetched the latest code")
        else:
            self.initiate_rollback()

    def deploy_to_s3(self):
        """
        Class method to clone git repo and upload the contents to s3
        :return:
        """
        # Status = True if a method executes without any errors, False otherwise
        # Initiate rollback in case the method raise any errors
        if not self.rollback:
            self.update_state()
            deploy_status = deploy_to_s3(
                self.clone_path, self.config, self.region
            )
            if deploy_status:
                deploy_logger.write(
                    message=f"Deployed the latest code to S3"
                )
            else:
                self.initiate_rollback()
        else:
            pass

    def create_lambda_function(self):
        """
        Class method to create the lambda function
        :return:
        """
        # Status = True if a method executes without any errors, False otherwise
        # Initiate rollback in case the method raise any errors
        if not self.rollback:
            self.update_state()
            lambda_status = create_lambda(self.config, self.region)
            if not lambda_status:
                self.initiate_rollback()
            else:
                deploy_logger.write(
                    message=f"Deployed the Lambda function"
                )
        else:
            pass

    def create_glue_jobs(self):
        """
        Class method to create glue jobs
        :return:
        """
        # Status = True if a method executes without any errors, False otherwise
        # Initiate rollback in case the method raise any errors
        if not self.rollback:
            self.update_state()
            create_status = create_glue_jobs(self.config, self.region)
            if not create_status:
                self.initiate_rollback()
            else:
                deploy_logger.write(message="Deployed the Glue jobs")
        else:
            pass

    def create_step_function(self):
        if not self.rollback:
            self.update_state()
            create_status = create_step_function(
                self.config, self.region
            )
            if not create_status:
                self.initiate_rollback()
            else:
                deploy_logger.write(
                    message="Deployed the Step Function"
                )
        else:
            pass

    def _rollback_code_fetch(self):
        """
        Helper method for the main rollback method
        :return:
        """
        deploy_logger.write(
            message=f"Rolling back Cloning of github repo"
        )
        remove_clone_dir(self.clone_path)

    def _rollback_s3_upload(self):
        """
        Helper method for the main rollback method
        :return:
        """
        self._rollback_code_fetch()
        rm_command = f"aws s3 rm s3://{self.code_bucket_s3} --recursive"
        os.system(rm_command)
        deploy_logger.write(
            message=f"Rolling back the code upload to s3://{self.code_bucket_s3}"
        )

    def _rollback_lambda_creation(self):
        """
        Helper method for the main rollback method
        :return:
        """
        self._rollback_s3_upload()
        lambda_client = boto3.client("lambda", region_name=self.region)
        func_name = self.config["lambda_function_name"]
        if lambda_function_exists(lambda_client, func_name):
            deploy_logger.write(
                message=f"Rolling back the creation of lambda function: {func_name}"
            )
            lambda_client.delete_function(FunctionName=func_name)
        else:
            deploy_logger.write(
                message=f"The lambda function: {func_name} does not exist"
            )

    def _rollback_glue_job_creation(self):
        jobs = (
            f"{self.fm_prefix}-data-quality-checks",
            f"{self.fm_prefix}-data-masking",
            f"{self.fm_prefix}-data-standardization",
        )
        client = boto3.client("glue", region_name=self.region)
        for job in jobs:
            client.delete_job(JobName=job)
            status = f"Currently deleting {job}"
            deploy_logger.write(message=status)
            time.sleep(secs=5)

    def initiate_rollback(self):
        """
        Method to initiate rollback in case an error arises
        The rollback is dependent on the level of the state and
        uses a cascading call to helper methods
        :return:
        """
        rollback_issue = ROLLBACK_STATES[self.state]
        deploy_logger.write(
            message=f"Initiating rollback due to issue in {rollback_issue}"
        )
        self.rollback = True
        if self.state == 2:
            self._rollback_code_fetch()
        elif self.state == 3:
            self._rollback_s3_upload()
        elif self.state == 4:
            self._rollback_lambda_creation()
        elif self.state == 5:
            self._rollback_glue_job_creation()
        else:
            pass


def deploy(config, clone_path, region, multi_region=False, iteration=0):
    # Create an object of the DeployPipeline class
    deploy_ob = DeployPipeline(config, clone_path, region)
    # While deploying in multiple regions care is taken not to re clone the GitHub repo
    if (multi_region and iteration == 0) or (not multi_region):
        deploy_ob.clone_github()
    deploy_ob.deploy_to_s3()
    deploy_ob.create_lambda_function()
    deploy_ob.create_glue_jobs()
    deploy_ob.create_step_function()
    # While deploying in multiple region do not clean the clone dir
    # since it will be used again and again for multiple regions
    if not multi_region:
        remove_clone_dir(clone_path)


def deploy_region_wise(config, clone_path, deploy_region=None):
    if deploy_region is None:
        # Deploy the pipeline in primary region only
        primary_region = config["primary_region"]
        deploy(config, clone_path, primary_region)
    elif deploy_region == "all":
        # Deploy in all the regions specified in the config file
        region_vars = [i for i in config.keys() if "region" in i]
        for idx, val in enumerate(region_vars):
            region = config[val]
            deploy(
                config,
                clone_path,
                region,
                multi_region=True,
                iteration=idx,
            )
        # Removing the clone path after the final deployment
        remove_clone_dir(clone_path)
    else:
        # Deploy in the region as specified in the argument
        deploy(config, clone_path, deploy_region)


def main():
    """
    Main method of the DeployPipeline class
    :return:
    """
    # Get the global configs
    config_file_path = "config/globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    clone_path = os.getcwd() + "/github"
    # Get the arguments that are being passed while executing the script
    arguments = sys.argv
    # If region will be passed then the arguments list will have 2 elements
    if len(arguments) > 1:
        # arg region can be None, All, any AWS zone specific region
        region = arguments[1].lower()
        deploy_logger.write(message=f"Deploying in region = {region}")
        if region == "all":
            # Deploying in all the regions specified in the config file
            deploy_logger.write(
                message=f"Deploying in all the regions specified in config"
            )
            deploy_region_wise(config, clone_path, deploy_region="all")
        else:
            # Deploying in the region specified
            deploy_logger.write(
                message=f"Deploying in region = {region}"
            )
            deploy_region_wise(config, clone_path, deploy_region=region)
    else:
        # No region arguments passed hence deploy only in the primary region
        deploy_logger.write(message=f"Deploying in primary region")
        deploy_region_wise(config, clone_path)


if __name__ == "__main__":
    main()
