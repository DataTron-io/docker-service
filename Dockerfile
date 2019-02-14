FROM aksyjain/base-py3:latest

MAINTAINER Datatron "team@datatron.io"

ENV APP_DIR=/root/publisher-flask-service

COPY . ${APP_DIR}

WORKDIR ${APP_DIR}

ENV PYTHONPATH "${PYTHONPATH}:${APP_DIR}"

ENV DATATRON_ROOT_LOCATION "${APP_DIR}"

RUN apt-get -y update \
    && apt-get install openjdk-8-jre-headless -y \
    && python -m pip install -U pip \
    && python -m pip install -r ${APP_DIR}/requirements.txt \
    && python -m pip install -U datatron.common.discovery --index-url=http://datatron:rTW0z2NRE22icuyU@13.77.168.70:4039/datatron/pypi/+simple --trusted-host 13.77.168.70 \
    && python -m pip install -U datatron.common.transfer --index-url=http://datatron:rTW0z2NRE22icuyU@13.77.168.70:4039/datatron/pypi/+simple --trusted-host 13.77.168.70 \
    && python -m pip install -U datatron.common.ml_parser --index-url=http://datatron:rTW0z2NRE22icuyU@13.77.168.70:4039/datatron/pypi/+simple --trusted-host 13.77.168.70

EXPOSE 80

CMD [ "gunicorn", "-c", "kubernetes_gunicorn.conf", "wsgi:app", "--bind", ":80" ]
