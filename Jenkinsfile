pipeline {
  agent {
    kubernetes {
      label 'my-agent-pod'
      yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: x86-64-gnu
    image: adlinktech/zenoh-dev-manylinux2010-x86_64-gnu
    command:
    - cat
    tty: true
"""
    }
  }

  options { skipDefaultCheckout() }
  parameters {
    gitParameter(name: 'GIT_TAG',
                 type: 'PT_BRANCH_TAG',
                 description: 'The Git tag to checkout. If not specified "master" will be checkout.',
                 defaultValue: 'master')
    booleanParam(name: 'BUILD_MACOSX',
                 description: 'Build macosx target.',
                 defaultValue: true)
    booleanParam(name: 'BUILD_DOCKER',
                 description: 'Build for zenoh in Docker (Alpine x86_64-unknown-linux-musl target).',
                 defaultValue: true)
    booleanParam(name: 'BUILD_LINUX64',
                 description: 'Build x86_64-unknown-linux-gnu target.',
                 defaultValue: true)
    booleanParam(name: 'BUILD_LINUX32',
                 description: 'Build i686-unknown-linux-gnu target.',
                 defaultValue: true)
    booleanParam(name: 'BUILD_AARCH64',
                 description: 'Build aarch64-unknown-linux-gnu target.',
                 defaultValue: true)
    booleanParam(name: 'BUILD_WIN64',
                 description: 'Build x86_64-pc-windows-gnu target.',
                 defaultValue: true)
    booleanParam(name: 'BUILD_WIN32',
                 description: 'Build i686-pc-windows-gnu target.',
                 defaultValue: true)
    booleanParam(name: 'PUBLISH_ECLIPSE_DOWNLOAD',
                 description: 'Publish the resulting artifacts to Eclipse download.',
                 defaultValue: false)
  }
  environment {
      LABEL = get_label()
      MACOSX_DEPLOYMENT_TARGET=10.7
  }

  stages {
    stage('Test container') {
      steps {
        container('x86-64-gnu') {
          sh '''
          uname -a
          env
          pwd
          ls -al
          id
          echo "HOME=$HOME"
          '''
        }
      }
    }

    stage('Checkout Git TAG') {
      steps {
        container('x86-64-gnu') {
          checkout([$class: 'GitSCM',
                    branches: [[name: "${params.GIT_TAG}"]],
                    doGenerateSubmoduleConfigurations: false,
                    extensions: [],
                    gitTool: 'Default',
                    submoduleCfg: [],
                    userRemoteConfigs: [[url: 'https://github.com/eclipse-zenoh/zenoh-backend-filesystem.git']]
                  ])
          sh '''
          ls -al
          '''
        }
      }
    }


  }
}

def get_label() {
    return env.GIT_TAG.startsWith('origin/') ? env.GIT_TAG.minus('origin/') : env.GIT_TAG
}
