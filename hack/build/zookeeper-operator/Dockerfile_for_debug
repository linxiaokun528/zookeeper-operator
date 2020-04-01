FROM centos:7

ADD _output/bin/zookeeper-operator /usr/local/bin
ADD _output/dlv/dlv /usr/local/bin

EXPOSE 40000

CMD ["/usr/local/bin/dlv", "--listen=:40000", "--headless=true", "--api-version=2", "exec", "/usr/local/bin/zookeeper-operator"]

#CMD ["zookeeper-operator"]
