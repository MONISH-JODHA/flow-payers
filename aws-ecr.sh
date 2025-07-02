aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 992382530041.dkr.ecr.us-east-2.amazonaws.com
docker build -t analytics .
docker tag analytics:latest 992382530041.dkr.ecr.us-east-2.amazonaws.com/analytics:latest
docker push 992382530041.dkr.ecr.us-east-2.amazonaws.com/analytics:latest