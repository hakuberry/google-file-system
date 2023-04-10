FROM ubuntu:bionic
FROM python:3.9
LABEL maintainer="hakuberry"

ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt /tmp/requirements.txt
COPY . /app
WORKDIR /app

RUN python -m venv /py && \
    /py/bin/pip install --upgrade pip && \
    /py/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -rf /tmp && \
    grpc_tools.protoc -I. --python_out=. --grpc_python_out=. src/gfs.proto


ENV PATH="/py/bin:$PATH"

CMD ["run.sh"]
