def isManualTrigger() {
    return currentBuild.getBuildCauses().any { cause -> cause._class == 'hudson.model.Cause$UserIdCause' }
}

@Library('shared-library') _

pipeline {

    agent {
        docker {
            label 'memphis-jenkins-big-fleet,'
            image 'maven:3.8.4-openjdk-17'
            args '-u root'
        }
    } 

    environment {
            HOME = '/tmp'
            GPG_PASSPHRASE = credentials('gpg-key-passphrase')
    }

    stages {
        stage('Read Version from pom.xml') {  
            when {
                anyOf {
                    allOf {
                        branch 'master'
                        triggeredBy 'UserIdCause' // Manual trigger on master
                    }
                    allOf {
                        branch 'latest'
                    }
                }
            }            
            steps {
                dir('superstream-clients'){
                    script {
                        // Read the version from pom.xml
                        def pomVersion = sh(
                            script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout",
                            returnStdout: true
                        ).trim()
                        echo "Original version from pom.xml: ${pomVersion}"
                        env.BASE_VERSION = pomVersion
                        def branchName = env.BRANCH_NAME ?: ''
                    }                   
                }

            }
        } 

        stage('Build and Deploy Beta') {
            when {
                expression {
                    env.BRANCH_NAME == 'master' && isManualTrigger()
                }
            }           
            steps {
                script {
                    dir('superstream-clients'){
                        withCredentials([file(credentialsId: 'gpg-key', variable: 'GPG_KEY')]) {
                        //   gpg --batch --import $GPG_KEY
                            sh '''
                            echo '${env.GPG_PASSPHRASE}' | gpg --batch --yes --passphrase-fd 0 --import $GPG_KEY
                            echo "allow-loopback-pinentry" > /tmp/.gnupg/gpg-agent.conf
                            echo "D64C041FB68170463BE78AD7C4E3F1A8A5F0A659:6:" | gpg --import-ownertrust                      
                            '''
                        }
                        withCredentials([file(credentialsId: 'settings-xml-superstream', variable: 'MAVEN_SETTINGS')]) {
                            sh "mvn -B package --file pom.xml"
                            def betaVersion = "${env.BASE_VERSION}-beta"
                            echo "Setting beta version: ${betaVersion}"                                
                            sh "mvn versions:set -DnewVersion=${betaVersion}"
                            sh "mvn -s $MAVEN_SETTINGS deploy -DautoPublish=true"
                        }
                    }                    
                }
            }
        }

        stage('Build and Deploy Production') {
            when {
                    expression { env.BRANCH_NAME == 'latest' }
                }            
            steps {
                script {
                    dir('superstream-clients'){
                        withCredentials([file(credentialsId: 'gpg-key', variable: 'GPG_KEY')]) {
                        //   gpg --batch --import $GPG_KEY
                            sh '''
                            echo '${env.GPG_PASSPHRASE}' | gpg --batch --yes --passphrase-fd 0 --import $GPG_KEY
                            echo "allow-loopback-pinentry" > /tmp/.gnupg/gpg-agent.conf
                            echo "D64C041FB68170463BE78AD7C4E3F1A8A5F0A659:6:" | gpg --import-ownertrust                      
                            '''
                        }
                        withCredentials([file(credentialsId: 'settings-xml-superstream', variable: 'MAVEN_SETTINGS')]) {
                            sh "mvn -B package --file pom.xml"
                            sh "mvn -s $MAVEN_SETTINGS deploy -DautoPublish=true"
                        }
                    }                    
                }
            }
        }

        stage('Checkout to version branch'){
            when {
                    expression { env.BRANCH_NAME == 'latest' }
                }        
            steps {
                sh """
                    curl -L https://github.com/cli/cli/releases/download/v2.40.0/gh_2.40.0_linux_amd64.tar.gz -o gh.tar.gz 
                    tar -xvf gh.tar.gz
                    mv gh_2.40.0_linux_amd64/bin/gh /usr/local/bin 
                    rm -rf gh_2.40.0_linux_amd64 gh.tar.gz
                """
                withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
                sh """
                GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git checkout -b ${env.BASE_VERSION}
                GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git push --set-upstream origin ${env.BASE_VERSION}
                """
                }
                withCredentials([string(credentialsId: 'gh_token', variable: 'GH_TOKEN')]) {
                    dir('superstream-clients'){    
                    sh """
                    gh release create ${env.BASE_VERSION} target/superstream-clients-${env.BASE_VERSION}.jar --generate-notes
                    """
                    }
                }
            }
        } 
    }
    post {
        always {
            cleanWs()
        }
        success {
            script {
                if (env.GIT_BRANCH == 'latest') {   
                    sendSlackNotification('SUCCESS')         
                    notifySuccessful()
                }
            }
        }
        
        failure {
            script {
                if (env.GIT_BRANCH == 'latest') { 
                    sendSlackNotification('FAILURE')              
                    notifyFailed()
                }
            }            
        }
        aborted {
            script {
                if (env.BRANCH_NAME == 'latest') {
                    sendSlackNotification('ABORTED')
                }
                // Get the build log to check for the specific exception and retry job
                AgentOfflineException()
            }          
        }        
    }    
}

def notifySuccessful() {
    emailext (
        subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output and connection attributes at ${env.BUILD_URL}""",
        to: 'tech-leads@superstream.ai'
    )
}
def notifyFailed() {
    emailext (
        subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output at ${env.BUILD_URL}""",
        to: 'tech-leads@superstream.ai'
    )
}
