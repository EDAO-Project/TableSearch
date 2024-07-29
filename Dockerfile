FROM ubuntu:22.04

ADD maven.sh /etc/profile.d/

RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt install openjdk-17-jdk wget maven -y

CMD []
