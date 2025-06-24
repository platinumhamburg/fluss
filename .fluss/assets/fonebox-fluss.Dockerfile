FROM eclipse-temurin:17-jre-jammy AS builder

# Install dependencies
RUN set -ex;   apt-get update;   apt-get -y install gpg libsnappy1v5 gettext-base libjemalloc-dev;   rm -rf /var/lib/apt/lists/*

 # Prepare environment
ENV FLUSS_HOME=/opt/fluss
ENV PATH=$FLUSS_HOME/bin:$PATH
RUN groupadd --system --gid=9999 fluss &&      useradd --system --home-dir $FLUSS_HOME --uid=9999 --gid=fluss fluss
WORKDIR $FLUSS_HOME

# Please copy build-target to the docker dictory first, then copy to the image.
COPY --chown=fluss:fluss fluss-dist/target/fluss-0.7-SNAPSHOT-bin/fluss-0.7-SNAPSHOT /opt/fluss/

RUN ["chown", "-R", "fluss:fluss", "."]
COPY docker/docker-entrypoint.sh /
RUN ["chmod", "+x", "/docker-entrypoint.sh"]
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]

