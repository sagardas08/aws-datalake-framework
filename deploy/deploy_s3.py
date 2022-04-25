import os
import shutil
import getpass


def create_or_remove_dir(path):
    """
    Create a temporary directory to clone the git repository
    """
    try:
        print("Creating a new dir")
        os.mkdir(path=path)
    except FileExistsError:
        print("The dir already exists, cleaning in progress")
        shutil.rmtree(path)
        os.mkdir(path=path)
    return path


def fetch_latest_code(path, config):
    """
    method to fetch the latest code from github
    """
    # the path on which the repo is cloned
    local_path = create_or_remove_dir(path)
    # git username and pass, required for first time usage and is cached later
    username = getpass.getpass("Enter username - ")
    project_name = config["project_name"]
    branch = config["git_branch"]
    git_cmd = f"cd github && git clone -b {branch} https://github.com/{username}/{project_name}.git"
    try:
        os.system(git_cmd)
        return True
    except Exception as e:
        print(e)
        return False


def remove_clone_dir(path):
    """
    method to remove the cloned github repo using in-built library shutil
    """
    if os.path.exists(path):
        shutil.rmtree(path)


def zip_utils(source_path, target_zip_path, base_dir=None):
    """
    method to zip a file in the source_path to a target_path
    """
    if base_dir:
        shutil.make_archive(
            base_name=target_zip_path,
            format="zip",
            root_dir=source_path,
            base_dir="utils",
        )
    else:
        shutil.make_archive(
            base_name=target_zip_path,
            format="zip",
            root_dir=source_path,
        )


def deploy_to_s3(root_dir, config, region=None):
    """
    Method to deploy the framework code to specified AWS S3 code bucket.
    """
    fm_prefix = config["fm_prefix"]
    region = config["primary_region"] if region is None else region
    project_name = config["project_name"]
    bucket_name = f"{fm_prefix}-code-{region}"
    # project root
    source_path = root_dir + f"/{project_name}"
    # Zip file source and target paths
    utils_zip_src_path = root_dir + f"/{project_name}/src"
    utils_zip_target_path = (
        root_dir + f"/{project_name}/dependencies/utils"
    )
    lambda_zip_src_path = root_dir + f"/{project_name}/src/lambda"
    lambda_zip_target_path = (
        root_dir + f"/{project_name}/lambda/lambda_function"
    )
    try:
        zip_utils(
            utils_zip_src_path, utils_zip_target_path, base_dir="utils"
        )
        zip_utils(lambda_zip_src_path, lambda_zip_target_path)
        # Cleans the code bucket prior to uploading of the latest code
        cleanup_cmd = f"aws s3 rm s3://{bucket_name} --recursive"
        copy_cmd = f"aws s3 cp {source_path} s3://{bucket_name}/{project_name} --recursive"
        os.system(cleanup_cmd)
        os.system(copy_cmd)
        return True
    except Exception as e:
        print(e)
        return False
