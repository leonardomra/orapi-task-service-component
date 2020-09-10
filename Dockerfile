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

# Add wait-for-it

#COPY wait-for-it.sh /root/wait-for-it.sh 
#RUN ["chmod", "+x", "/root/wait-for-it.sh"]


ENTRYPOINT ["python3"]
#ENTRYPOINT [ "/bin/bash", "-c" ]


CMD ["-m", "task_module"]
#CMD ["./root/wait-for-it.sh --timeout=0 kibana:5601" , "python3" , "--strict" , "--timeout=300" , "--" , "-m", "task_module"]