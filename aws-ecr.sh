aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 992382530041.dkr.ecr.us-east-2.amazonaws.com
docker build -t analytics .
docker tag analytics:latest 992382530041.dkr.ecr.us-east-2.amazonaws.com/analytics:latest
docker push 992382530041.dkr.ecr.us-east-2.amazonaws.com/analytics:latest





aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 156554752754.dkr.ecr.us-east-1.amazonaws.com
docker build -t aws_az_analytics_operations_prod .
docker tag aws_az_analytics_operations_prod:latest 156554752754.dkr.ecr.us-east-1.amazonaws.com/aws_az_analytics_operations_prod:latest
docker push 156554752754.dkr.ecr.us-east-1.amazonaws.com/aws_az_analytics_operations_prod:latest