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

        stage('build golang version') {
            steps {
                sh 'make clean'
                sh 'make all'
                dir("build") {

                    // Copy prod systemd service file to build dir
                    sh """
                        cp ../deploy/systemd/prod/* ./
                    """

                    sh """
                        fpm -s dir -t deb -n nsq -v 0.0.${env.BUILD_NUMBER} -C ./ -p nsq_0.0.${env.BUILD_NUMBER}_amd64.deb --description "Forked NSQ" \
                        --after-install ./../deploy/systemd/after-install.sh \
                        --before-install ./../deploy/systemd/before-install.sh \
                        ./=/opt/nsq-latest/bin \
                        ./../deploy/systemd/nsqadmin.service=/etc/systemd/system/nsqadmin.service \
                        ./../deploy/systemd/nsqlookupd.service=/etc/systemd/system/nsqlookupd.service \
                       ./../deploy/systemd/nsqd.service=/etc/systemd/system/nsqd.service
                    """


                    // Copy staging systemd service file to build dir
                    sh """
                        cp ./../deploy/systemd/staging/* ./
                    """
                    sh """
                        fpm -s dir -t deb -n nsq-staging -v 0.0.${env.BUILD_NUMBER} -C ./ -p nsq-staging_0.0.${env.BUILD_NUMBER}_amd64.deb --description "Forked NSQ" \
                        --after-install ./../deploy/systemd/after-install.sh \
                        --before-install ./../deploy/systemd/before-install.sh \
                        ./=/opt/nsq-latest/bin \
                        ./../deploy/systemd/nsqadmin.service=/etc/systemd/system/nsqadmin.service \
                        ./../deploy/systemd/nsqlookupd.service=/etc/systemd/system/nsqlookupd.service \
                       ./../deploy/systemd/nsqd-staging.service=/etc/systemd/system/nsqd.service
                    """
                    /*
                    sh """
                        fpm -s dir -t deb -n nsq -v 0.0.${env.BUILD_NUMBER} -C ./ -p nsq_node_0.0.${env.BUILD_NUMBER}_amd64.deb --description "Forked NSQ" \
                        --after-install ./../deploy/systemd/after-install.sh \
                        --before-install ./../deploy/systemd/before-install.sh \
                        ./=/opt/nsq-latest/bin \
                        ./../deploy/systemd/nsqadmin.service=/etc/systemd/system/nsqadmin.service \
                        ./../deploy/systemd/nsqlookupd.service=/etc/systemd/system/nsqlookupd.service \
                        ./../deploy/systemd/nsqd.service=/etc/systemd/system/nsqd.service
                        """
                    */
                }
            }
        }

        stage('add master to repo') {
            when { branch 'master' }
            steps {
                dir("build") {
                    /*
                    sh "aptly repo add testing nsq_0.0.${env.BUILD_NUMBER}_amd64.deb"
                    sh "aptly snapshot create release-testing-nsq_${env.BUILD_NUMBER} from repo testing"
                    withCredentials([string(credentialsId: 'jenkins_gpg_passphrase', variable: 'PASSPHRASE')]) {
                        sh "aptly -passphrase=$PASSPHRASE publish switch buster-testing release-testing-nsq_${env.BUILD_NUMBER}"
                    }
                    */

                    sh "aptly repo add stable nsq_0.0.${env.BUILD_NUMBER}_amd64.deb"
                    sh "aptly repo add stable nsq-staging_0.0.${env.BUILD_NUMBER}_amd64.deb"
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
