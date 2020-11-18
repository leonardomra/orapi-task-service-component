#FROM python:3.6.12-buster
#FROM python:3.7.4-stretch
#FROM python:3.6.12-stretch
#FROM python:3.8.6-buster

FROM ubuntu:18.04
# Upgrade installed packages
RUN apt-get update && apt-get upgrade -y && apt-get clean
# (...)
RUN apt-get install -y curl make wget git gcc g++ lhasa libgmp-dev libmpfr-dev libmpc-dev flex bison gettext texinfo ncurses-dev autoconf rsync
# Python package management and basic dependencies
RUN apt-get install -y curl python3.6 python3.6-dev python3.6-distutils
# Register the version in alternatives
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.6 1
# Set python 3 as the default python
RUN update-alternatives --set python /usr/bin/python3.6
# Upgrade pip to latest version
RUN curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --force-reinstall && \
    rm get-pip.py




RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/

RUN pip install --upgrade pip && pip install wheel

RUN pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.5/en_core_sci_md-0.2.5.tar.gz

RUN pip install --no-cache-dir -r requirements.txt


COPY . /usr/src/app

ENV PYTHONUNBUFFERED=1

EXPOSE 80

#CMD tail -f /dev/null

ENTRYPOINT ["python3"]

CMD ["-u", "-m", "task_module"]
