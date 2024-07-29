FROM ubuntu:22.04

WORKDIR /home
ADD maven.sh /etc/profile.d/

RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt install openjdk-17-jdk wget -y
RUN wget https://dlcdn.apache.org/maven/maven-3/3.9.8/binaries/apache-maven-3.9.8-bin.tar.gz -P /tmp
RUN tar -xf /tmp/apache-maven-3.9.8-bin.tar.gz -C /opt
RUN ln -s /opt/apache-maven-3.9.8 /opt/maven
RUN source /etc/profile.d/maven.sh

CMD []