pipeline {
    agent {
        node {
            label 'master'
        }
    }
    stages {
        stage('Slack') {
            steps {
                script {
                    slackSend color: "good", message: "Job: ${env.JOB_NAME} with #${env.BUILD_NUMBER} started ${env.RUN_DISPLAY_URL}"
                }
            }
        }

        stage('build all') {
            steps {
                sh 'make clean'
                sh 'make all'
                dir("build") {
                sh """
                    fpm -s dir -t deb -n nsq -v 0.0.${env.BUILD_NUMBER} -C ./ -p nsq_0.0.${env.BUILD_NUMBER}_amd64.deb --description "Forked NSQ" \
                    --after-install ./../deploy/systemd/after-install.sh \
                    ./=/opt/nsq-latest/bin \
                    ./../deploy/systemd=/etc/system/systemd
                    """
                }
            }
        }

        stage('add master to repo') {
            when { branch 'master' }
            steps {
                dir("build") {
                    sh "aptly repo add testing nsq_0.0.${env.BUILD_NUMBER}_amd64.deb"
                    sh "aptly snapshot create release-testing-nsq_${env.BUILD_NUMBER} from repo testing"
                    withCredentials([string(credentialsId: 'jenkins_gpg_passphrase', variable: 'PASSPHRASE')]) {
                        sh "aptly -passphrase=$PASSPHRASE publish switch buster-testing release-testing-nsq_${env.BUILD_NUMBER}"
                    }

                    sh "aptly repo add stable nsq_0.0.${env.BUILD_NUMBER}_amd64.deb"
                    sh "aptly snapshot create release-nsq_${env.BUILD_NUMBER} from repo stable"
                    withCredentials([string(credentialsId: 'jenkins_gpg_passphrase', variable: 'PASSPHRASE')]) {
                        sh "aptly -passphrase=$PASSPHRASE publish switch buster release-nsq_${env.BUILD_NUMBER}"
                    }
                }
                sh 'syncrepo'
            }
        }
    }
}