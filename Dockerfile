FROM ubuntu:16.10

RUN apt-get update && apt-get install -y \
    git \
    nano \
    openjdk-8-jdk \
    postgresql \
    postgresql-contrib \
    python \
    python-pip \
    sudo    

RUN pip install -I pyyaml==3.10

# Instal vmtouch, used for paging graphs into RAM
RUN cd /usr/share/ && \
    git clone https://github.com/hoytech/vmtouch.git && \
    cd vmtouch && \
    make && \
    make install

# Allow local jvm process to connect to postgres


RUN echo 'export PATH="/root/tempest/bin:$PATH"' >> ~/.bashrc

COPY bin /root/tempest/bin
COPY example /root/tempest/example
COPY system /root/tempest/system
COPY README.md /root/tempest/README.md
COPY target/scala-2.11 /root/tempest/target/scala-2.11

RUN chmod a+x /root # allow postgres to read example files from /root/tempest/example
