pipeline {
    agent any
    stages {
        stage('Slack') {
            steps {
                script {
                    slackSend color: "good", message: "Job: ${env.JOB_NAME} with #${env.BUILD_NUMBER} started ${env.RUN_DISPLAY_URL}"
                }
            }
        }
        stage('Deploy') {
            when { branch 'master' }
            steps {
                sh 'go build'
            }
        }
    }
}