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

# Instal vmtouch, used for paging graphs into RAM
RUN cd /usr/share/ && \
    git clone https://github.com/hoytech/vmtouch.git && \
    cd vmtouch && \
    make && \
    make install

# Allow local jvm process to connect to postgres
RUN HBA="/etc/postgresql/9.5/main/pg_hba.conf"; \
    sed -i '1i host    all             tempest             127.0.0.1/32            trust' $HBA && \
    sed -i '1i local    all             tempest             trust' $HBA    

RUN service postgresql start && \
    sudo -u postgres createuser --createdb --no-createrole --no-superuser tempest && \
    sudo -u postgres createdb tempest

RUN echo 'export PATH="/root/tempest/bin:$PATH"' >> ~/.bashrc
