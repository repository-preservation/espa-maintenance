FROM centos:7
MAINTAINER USGS EROS LSRD http://eros.usgs.gov

ENV LOGSTASH_VER="5.5.1"
ENV LOGSTASH_HOME="/usr/local/logstash"
RUN groupadd logstash
RUN useradd -d /home/logstash -g logstash -m logstash
WORKDIR /opt
RUN curl "https://artifacts.elastic.co/downloads/logstash/logstash-$LOGSTASH_VER.tar.gz" | tar xz && \
    mv "logstash-$LOGSTASH_VER" $LOGSTASH_HOME && \
    chown -R logstash:logstash $LOGSTASH_HOME
ENV PATH="${PATH}:$LOGSTASH_HOME/bin"

RUN yum install -y java-1.8.0-openjdk-devel
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"
ENV PATH="${PATH}:$JAVA_HOME/bin"

USER logstash
WORKDIR /home/logstash
COPY ./logstash.conf logstash.conf

ENTRYPOINT ["logstash", "-f", "logstash.conf"]
