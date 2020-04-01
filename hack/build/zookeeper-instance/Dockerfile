ARG  VERSION=3.5.7

FROM zookeeper:${VERSION}

RUN sed -i 's/echo "standaloneEnabled=$ZOO_STANDALONE_ENABLED"/echo "reconfigEnabled=true"/g' /docker-entrypoint.sh && \
    sed -i 's/echo "admin.enableServer=$ZOO_ADMINSERVER_ENABLED"/echo "skipACL=yes"/g' /docker-entrypoint.sh
