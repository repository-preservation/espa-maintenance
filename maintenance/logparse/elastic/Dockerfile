FROM centos:7
MAINTAINER USGS EROS LSRD http://eros.usgs.gov

ENV ELASTIC_VER='5.5.1'
ENV ELASTIC_HOME='/usr/local/elastic'
RUN groupadd elastic
RUN useradd -d /home/elastic -g elastic -m elastic
WORKDIR /opt
RUN curl "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-$ELASTIC_VER.tar.gz" | tar xz && \
    mv "elasticsearch-$ELASTIC_VER" $ELASTIC_HOME && \
    chown -R elastic:elastic $ELASTIC_HOME
ENV PATH="${PATH}:$ELASTIC_HOME/bin"

RUN yum install -y java-1.8.0-openjdk-devel
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"
ENV PATH="${PATH}:$JAVA_HOME/bin"

USER elastic
WORKDIR /home/elastic
EXPOSE 9200
EXPOSE 9300
ENV http.host="0.0.0.0"
ENV transport.host="127.0.0.1"

ENTRYPOINT ["elasticsearch"]
