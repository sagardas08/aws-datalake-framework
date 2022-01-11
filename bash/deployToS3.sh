#!/bin/sh

export script_path=$(readlink -f "$0")
export project_name="aws-datalake-framework"
export project_parent_dir=$(echo ${script_path} | awk -F "${project_name}" '{print $1}')

cd ${project_parent_dir}
cp -r ${project_name} ${project_name}-temp

cd ${project_name}-temp/src
zip -r utils.zip utils
mv utils.zip ${project_parent_dir}/${project_name}-temp/dependencies/
cd ${project_parent_dir}

aws s3 rm s3://dl-fmwrk-code --recursive
aws s3 cp ${project_name}-temp s3://dl-fmwrk-code/${project_name} --recursive
rm -rf ${project_parent_dir}/${project_name}-temp
#aws glue create-job \
#  --name "2482-demo" \
#  --role "2482-misc-service-role" \
#  --command '{ \
#    "Name": "glueetl", \
#    "ScriptLocation": "s3://dl-fmwrk-code/aws-datalake-framework/src/genericDqChecks.py" \
#    }' \
#  --extra-py-files "s3://dl-fmwrk-code/aws-datalake-framework/dependencies/pydeequ.zip, \
#    s3://dl-fmwrk-code/aws-datalake-framework/dependencies/utils.zip" \
#  --extra-jars "s3://dl-fmwrk-code/aws-datalake-framework/dependencies/deequ-1.0.3.jar" \
#  --TempDir "s3://dl-fmwrk-10000/temporary/" \
#  --glue-version "2.0" \
#  --number-of-workers 10 \
#  --worker-type "G.2X" \
#  --enable-metrics \
#  --enable-continuous-cloudwatch-log \
#  --region us-east-2

exit 0
