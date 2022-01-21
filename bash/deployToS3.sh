#!/bin/sh

if [ $# -ne 1 ]; then
  echo "Error: Missing region argument"
  exit 1
fi

export region=$1
export script_path=$(readlink -f "$0")
export project_name="aws-datalake-framework"
export project_parent_dir=$(echo ${script_path} | awk -F "${project_name}" '{print $1}')

cd ${project_parent_dir}
rm -rf ${project_name}-temp
cp -r ${project_name} ${project_name}-temp

cd ${project_name}-temp/src
zip -r utils.zip utils
mv utils.zip ${project_parent_dir}/${project_name}-temp/dependencies/
cd ${project_parent_dir}

aws s3 rm s3://dl-fmwrk-code-${region} --recursive
aws s3 cp ${project_name}-temp s3://dl-fmwrk-code-${region}/${project_name} --recursive
rm -rf ${project_parent_dir}/${project_name}-temp

aws glue delete-job --job-name dl-fmwrk-data-quality-checks --region $region
aws glue create-job \
  --name dl-fmwrk-data-quality-checks \
  --role 2482-misc-service-role \
  --command "{ \
    \"Name\": \"glueetl\", \
    \"ScriptLocation\": \"s3://dl-fmwrk-code-$region/aws-datalake-framework/src/genericDqChecks.py\" \
    }" \
  --default-arguments "{ \
    \"--extra-py-files\": \"s3://dl-fmwrk-code-$region/aws-datalake-framework/dependencies/pydeequ.zip,s3://dl-fmwrk-code-$region/aws-datalake-framework/dependencies/utils.zip\", \
    \"--extra-jars\": \"s3://dl-fmwrk-code-$region/aws-datalake-framework/dependencies/deequ-1.0.3.jar\", \
    \"--TempDir\": \"s3://dl-fmwrk-10000-$region/temporary/\" \
    }"\
  --glue-version 2.0 \
  --number-of-workers 10 \
  --worker-type G.2X \
  --region $region

exit 0