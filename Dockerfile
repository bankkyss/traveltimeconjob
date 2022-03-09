FROM python:3.7
#RUN apt-get update && apt-get -y install cron vim
WORKDIR /app
ADD . /app
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
#COPY crontab /etc/cron.d/crontab
#RUN chmod 0644 /etc/cron.d/crontab
#RUN /usr/bin/crontab /etc/cron.d/crontab
RUN echo $PYTHONPATH
# run crond as main process of container
RUN chmod a+x start.sh
CMD ["./start.sh"]
#CMD ["cron", "-f"] & ['uvicorn','apiprocess:app',"--host", "0.0.0.0", "--port", "8000"]


