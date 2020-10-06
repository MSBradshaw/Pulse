rm lambda.zip
# prepare the new zip files of code
zip -r9 lambda.zip package/pymysql/*
zip -g lambda.zip databaseaccess.py
zip -g lambda.zip lambda_function.py
zip -g lambda.zip not_important_info.txt
# send the updated zip to AWS
aws lambda update-function-code --function-name pulse --zip-file fileb://lambda.zip 
