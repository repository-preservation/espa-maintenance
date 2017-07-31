FROM centos:7
MAINTAINER USGS EROS LSRD http://eros.usgs.gov

ENV KIBANA_VER="5.5.1"
ENV KIBANA_HOME="/usr/local/kibana"
RUN groupadd kibana
RUN useradd -d /home/kibana -g kibana -m kibana
WORKDIR /opt
RUN curl "https://artifacts.elastic.co/downloads/kibana/kibana-$KIBANA_VER-linux-x86_64.tar.gz" | tar zx && \
    mv "kibana-$KIBANA_VER-linux-x86_64" $KIBANA_HOME && \
    chown -R kibana:kibana $KIBANA_HOME
ENV PATH="${PATH}:$KIBANA_HOME/bin"

USER kibana
WORKDIR /home/kibana
EXPOSE 5601

ENTRYPOINT ["kibana"]
