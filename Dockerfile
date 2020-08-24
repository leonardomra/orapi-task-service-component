#FROM python:3.6-alpine
FROM python:3.7.3-stretch

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/

RUN pip3 install --upgrade pip

RUN pip3 install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.5/en_core_sci_md-0.2.5.tar.gz

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

EXPOSE 80

ENTRYPOINT ["python3"]

CMD ["-m", "task_module"]