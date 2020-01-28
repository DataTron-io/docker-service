FROM centos:7.4.1708

MAINTAINER Datatron "team@datatron.io"

ENV APP_DIR=/root/docker-service

COPY . ${APP_DIR}

WORKDIR ${APP_DIR}

ENV PYTHONPATH "${PYTHONPATH}:${APP_DIR}"

ENV DATATRON_ROOT_LOCATION "${APP_DIR}"

RUN yum -y update && yum install -y yum-utils && yum groupinstall -y development \
    && yum install -y  https://centos7.iuscommunity.org/ius-release.rpm \
    && yum install -y python36u python36u-pip python36u-devel \
    && yum install -y java-1.8.0-openjdk-headless \
    && python -m pip install -U datatron.common.discovery --index-url=http://${DEVPI_SERVER_ACCESS}@${DEVPI_SERVER_IP}:${DEVPI_SERVER_PORT}/datatron/pypi/+simple --trusted-host ${DEVPI_SERVER_IP} \
    && python3.6 -m pip install -r ${APP_DIR}/requirements.txt

EXPOSE 80