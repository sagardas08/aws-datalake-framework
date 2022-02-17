import os
import shutil
import getpass


def create_or_remove_dir(path):
    """
    Create a temporary directory to clone the git repository
    :param path:
    :return:
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

    :param path:
    :param config:
    :return:
    """
    local_path = create_or_remove_dir(path)
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
    if os.path.exists(path):
        shutil.rmtree(path)


def zip_utils(zip_path, utils_path):
    shutil.make_archive(utils_path, "zip", zip_path)


def deploy_to_s3(root_dir, config, region=None):
    """

    :param root_dir:
    :param config:
    :param region:
    :return:
    """
    fm_prefix = config["fm_prefix"]
    region = config["primary_region"] if region is None else region
    project_name = config["project_name"]
    bucket_name = f"{fm_prefix}-code-{region}"
    source_path = root_dir + f"/{project_name}"
    utils_zip_src_path = root_dir + f"/{project_name}/src/utils"
    utils_zip_target_path = root_dir + f"/{project_name}/dependencies/utils"
    lambda_zip_src_path = root_dir + f"/{project_name}/src/lambda"
    lambda_zip_target_path = root_dir + f"/{project_name}/lambda/lambda_function"
    try:
        zip_utils(utils_zip_src_path, utils_zip_target_path)
        zip_utils(lambda_zip_src_path, lambda_zip_target_path)
        cleanup_cmd = f"aws s3 rm s3://{bucket_name} --recursive"
        copy_cmd = (
            f"aws s3 cp {source_path} s3://{bucket_name}/{project_name} --recursive"
        )
        os.system(cleanup_cmd)
        os.system(copy_cmd)
        return True
    except Exception as e:
        print(e)
        return False
