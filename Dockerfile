FROM centos:7.4.1708

MAINTAINER Datatron "team@datatron.io"

ENV APP_DIR=/root/docker-service

ARG GITHUB_ACCESS_TOKEN

COPY . ${APP_DIR}

WORKDIR ${APP_DIR}

ENV PYTHONPATH "${PYTHONPATH}:${APP_DIR}"

ENV DATATRON_ROOT_LOCATION "${APP_DIR}"

RUN yum -y update && yum install -y yum-utils && yum groupinstall -y development \
    && yum install -y  https://centos7.iuscommunity.org/ius-release.rpm \
    && yum install -y python36u python36u-pip python36u-devel \
    && yum install -y java-1.8.0-openjdk-headless \
    && yum install -y git \
    && python3.6 -m pip install -U pip \
    && git clone -b master https://${GITHUB_ACCESS_TOKEN}@github.com/DataTron-io/datatron_common/ \
    && cd ${APP_DIR}/datatron_common/discovery \
    && python3.6 -m pip wheel . \
    && python3.6 -m pip install ${APP_DIR}/datatron_common/discovery/ \
    && python3.6 -m pip install -r ${APP_DIR}/requirements.txt

EXPOSE 80