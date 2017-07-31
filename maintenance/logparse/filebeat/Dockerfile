FROM centos:7
MAINTAINER USGS EROS LSRD http://eros.usgs.gov

ENV FILEBEAT_VER="5.5.1"
ENV FILEBEAT_HOME="/usr/local/filebeat"
RUN groupadd filebeat
RUN useradd -d /home/filebeat -g filebeat -m filebeat
WORKDIR /opt
RUN curl "https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-$FILEBEAT_VER-linux-x86_64.tar.gz" | tar zx && \
    mv "filebeat-$FILEBEAT_VER-linux-x86_64" $FILEBEAT_HOME && \
    chown -R filebeat:filebeat $FILEBEAT_HOME
ENV PATH="${PATH}:$FILEBEAT_HOME"

USER filebeat
WORKDIR /home/filebeat
COPY ./filebeat.yml filebeat.yml

ENTRYPOINT ["filebeat", "-e", "-c", "filebeat.yml", "-d", "publish"]
