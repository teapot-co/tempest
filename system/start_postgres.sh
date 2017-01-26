#!/usr/bin/env bash

if [ ! -d "/data/postgres" ]; then
  # If postgres is running here, it means it is the "default" postgres.
  if pgrep "postgres" > /dev/null; then
    sudo service postgresql stop
  fi

  sudo -u postgres /usr/lib/postgresql/9.5/bin/initdb "/data/postgres"

  HBA="/data/postgres/pg_hba.conf"
  # For connecting to postgres from outside docker (for testing), enable connections from docker subnet 172.16.0.0/12
  sed -i '1i host    all             tempest             172.16.0.0/12           trust' $HBA
  # For connecting to postgres from scala inside docker
  sed -i '1i host    all             tempest             127.0.0.1/32            trust' $HBA
  sed -i '1i local   all             tempest             trust' $HBA

  PGCONF="/data/postgres/postgresql.conf"
  # For connecting to postgres from outside docker (for testing), listen on all ip addresses
  sed -i "1i listen_addresses = '*'" $PGCONF

  sudo -Hiu postgres /usr/lib/postgresql/9.5/bin/pg_ctl -D /data/postgres -l /data/postgres/logfile start
  while ! sudo -Hiu postgres /usr/lib/postgresql/9.5/bin/pg_isready; do
    sleep 1
  done

  sudo -Hiu postgres createuser --createdb --no-createrole --no-superuser tempest && \
  sudo -Hiu postgres createdb tempest

else
  chown postgres:postgres /data/postgres
  sudo -Hiu postgres /usr/lib/postgresql/9.5/bin/pg_ctl -D /data/postgres -l /data/postgres/logfile start
  while ! sudo -Hiu postgres /usr/lib/postgresql/9.5/bin/pg_isready; do
    sleep 1
  done
fi
