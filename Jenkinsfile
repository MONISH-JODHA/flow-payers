// Jenkinsfile

pipeline {
    // 1. Define an agent. 'any' means Jenkins can use any available agent.
    //    For best practice, you might specify a label for an agent with Docker installed.
    agent any

    // 2. Define environment variables to make the pipeline reusable and easy to read.
    environment {
        AWS_ACCOUNT_ID      = '992382530041'
        AWS_REGION          = 'us-east-2'
        IMAGE_NAME          = 'analytics'
        // Construct the full ECR repository URL from the variables above
        ECR_REPOSITORY_URL  = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
        // The ID of the AWS credentials stored in Jenkins Credentials Manager
        AWS_CREDENTIALS_ID  = 'aws-ecr-credentials' 
    }

    // 3. Define the stages of the pipeline.
    stages {
        stage('Checkout Code') {
            steps {
                // This step checks out the code from the Git repository configured in the Jenkins job.
                echo 'Checking out code from GitHub...'
                checkout scm
            }
        }

        stage('Build Docker Image') {
            steps {
                echo "Building Docker image: ${IMAGE_NAME}..."
                // Build the image using the Dockerfile in the current directory.
                // We'll tag it with 'latest' and the unique Git commit hash for traceability.
                sh "docker build -t ${IMAGE_NAME}:latest -t ${IMAGE_NAME}:${GIT_COMMIT.substring(0, 8)} ."
            }
        }

        stage('Push Image to AWS ECR') {
            steps {
                echo "Pushing Docker image to ECR repository: ${ECR_REPOSITORY_URL}/${IMAGE_NAME}"
                // The withAWS block securely handles AWS credentials.
                // It uses the 'Pipeline: AWS Steps' plugin.
                withAWS(region: AWS_REGION, credentials: AWS_CREDENTIALS_ID) {
                    
                    // 1. Get the ECR login command and execute it.
                    echo "Logging in to AWS ECR..."
                    sh "aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REPOSITORY_URL}"

                    // 2. Tag the image for ECR.
                    echo "Tagging image for ECR..."
                    sh "docker tag ${IMAGE_NAME}:latest ${ECR_REPOSITORY_URL}/${IMAGE_NAME}:latest"
                    sh "docker tag ${IMAGE_NAME}:${GIT_COMMIT.substring(0, 8)} ${ECR_REPOSITORY_URL}/${IMAGE_NAME}:${GIT_COMMIT.substring(0, 8)}"

                    // 3. Push both the 'latest' and the commit-specific tag to ECR.
                    echo "Pushing tags to ECR..."
                    sh "docker push ${ECR_REPOSITORY_URL}/${IMAGE_NAME}:latest"
                    sh "docker push ${ECR_REPOSITORY_URL}/${IMAGE_NAME}:${GIT_COMMIT.substring(0, 8)}"
                }
            }
        }
    }
    
    // 4. Post-build actions that run regardless of the pipeline's success or failure.
    post {
        always {
            echo 'Pipeline finished. Cleaning up workspace...'
            // Optional: Clean up the Docker image from the Jenkins agent to save space.
            sh "docker rmi ${IMAGE_NAME}:latest ${IMAGE_NAME}:${GIT_COMMIT.substring(0, 8)} ${ECR_REPOSITORY_URL}/${IMAGE_NAME}:latest ${ECR_REPOSITORY_URL}/${IMAGE_NAME}:${GIT_COMMIT.substring(0, 8)} || true"
        }
    }
}