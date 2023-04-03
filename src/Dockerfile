FROM python:3.7-alpine
LABEL maintainer="hakuberry"

ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt /tmp/requirements.txt

RUN apk add --update --no-cache \
    gcc \
    linux-headers \
    make \
    musl-dev \
    g++

RUN python -m venv /py && \
    /py/bin/pip install --upgrade pip && \
    /py/bin/pip install -r /tmp/requirements.txt && \
    rm -rf /tmp && \

ENV PATH="/py/bin:$PATH"

CMD ["run.sh"]
