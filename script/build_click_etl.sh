
zip -r ../data_etl_lambda.zip *

aws lambda update-function-code \
--region ap-northeast-1 \
--function-name clicktracketl \
--zip-file fileb://../data_etl_lambda.zip