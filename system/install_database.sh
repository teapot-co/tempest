# Installs postgres
# Reads environment variable SHARED_BUFFERS to set shared_buffers parameter
# Add postgres repo
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
wget -q https://www.postgresql.org/media/keys/ACCC4CF8.asc -O - | sudo apt-key add -

# Install
sudo apt-get update
yes "Y" | head | sudo apt-get install postgresql postgresql-contrib


sudo su - postgres


service postgresql stop
service postgresql start
