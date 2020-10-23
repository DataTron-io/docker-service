FROM centos:7.4.1708

MAINTAINER Datatron "team@datatron.io"

ARG GITHUB_ACCESS_TOKEN

ENV APP_DIR=/root/docker-service

COPY . ${APP_DIR}

WORKDIR ${APP_DIR}

ENV PYTHONPATH "${PYTHONPATH}:${APP_DIR}"

ENV DATATRON_ROOT_LOCATION "${APP_DIR}"

RUN yum -y update && yum install -y yum-utils && yum groupinstall -y development \
    && yum install -y  https://repo.ius.io/ius-release-el7.rpm \
    && yum install -y python36u python36u-pip python36u-devel \
    && yum install git \
    && yum install -y java-1.8.0-openjdk-headless \
    && python3.6 -m pip install -r ${APP_DIR}/requirements.txt \
    && git clone -b master https://${GITHUB_ACCESS_TOKEN}@github.com/DataTron-io/datatron_common/ \
    && cd ${APP_DIR}/datatron_common/transfer \
    && python -m pip wheel . \
    && python -m pip install ${APP_DIR}/datatron_common/transfer/ \
    && cd ${APP_DIR}/datatron_common/discovery \
    && python -m pip wheel . \
    && python -m pip install ${APP_DIR}/datatron_common/discovery/ \
    && cd ${APP_DIR}/datatron_common/ml_parser \
    && python -m pip wheel . \
    && python -m pip install ${APP_DIR}/datatron_common/ml_parser/ \
    && rm -rf ${APP_DIR}/datatron_common/ \
    && apt-get autoremove git -y \
    && cd ${APP_DIR}/app \
    && git clone -b sankalp-metrics https://${GITHUB_ACCESS_TOKEN}@github.com/DataTron-io/governor/

EXPOSE 80