#!/usr/bin/env bash

apt-get update
apt-get install -y apache2
apt-get install -y avahi-daemon libnss-mdns
if ! [ -L /var/www ]; then
  rm -rf /var/www
  ln -fs /vagrant /var/www
fi
