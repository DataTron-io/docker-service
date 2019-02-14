FROM registry.access.redhat.com/rhscl/python-36-rhel7
MAINTAINER Datatron "team@datatron.io"

ARG DATATRON_PYPI_URL

ARG DATATRON_PYPI_HOST

ENV APP_DIR=/opt/app-root/src

COPY . ${APP_DIR}

WORKDIR ${APP_DIR}

ENV PYTHONPATH "${PYTHONPATH}:${APP_DIR}"

ENV DATATRON_ROOT_LOCATION "${APP_DIR}"

USER root

RUN  yum-config-manager --enable rhel-7-server-rpms && \
     yum install -y --setopt=tsflags=nodocs java-1.8.0-openjdk && \
     yum -y clean all --enablerepo='*' && \
     source scl_source enable rh-python36 && \
     source /opt/app-root/bin/activate && \
     python3.6 -m pip install -U pip && \
     python3.6 -m pip install datatron.common.ml_parser --index-url=$DATATRON_PYPI_URL --trusted-host $DATATRON_PYPI_HOST && \
     python3.6 -m pip install datatron.common.transfer --index-url=$DATATRON_PYPI_URL --trusted-host $DATATRON_PYPI_HOST && \
     python3.6 -m pip install -r ${APP_DIR}/requirements.txt && \
     chown -R 1001:0 /opt/app-root

USER 1001

CMD [ "gunicorn", "-c", "kubernetes_gunicorn.conf", "wsgi:app", "--bind", ":8080" ]
