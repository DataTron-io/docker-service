FROM datatron/base-py3:latest

MAINTAINER Datatron "team@datatron.io"

ENV APP_DIR=/root/docker-service

ARG GITHUB_ACCESS_TOKEN

COPY . ${APP_DIR}

WORKDIR ${APP_DIR}

ENV PYTHONPATH "${PYTHONPATH}:${APP_DIR}"

ENV DATATRON_ROOT_LOCATION "${APP_DIR}"

RUN apt-get -y update \
    && apt-get install openjdk-8-jre-headless -y \
    && apt-get install git -y \
    && apt-get install libsasl2-dev libldap2-dev libssl-dev libkrb5-dev -y \
    && python -m pip install -U pip \
    && python -m pip install -r ${APP_DIR}/requirements.txt \
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
    && apt-get autoremove git -y

EXPOSE 80