#!/bin/bash

set -o verbose

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    rm -rf /usr/local/var/postgres
    initdb /usr/local/var/postgres
    pg_ctl -D /usr/local/var/postgres start
    sleep 10
    createuser -s postgres
else
    psql --version || { apt-get update && apt-get install -y postgresql postgresql-contrib && service postgresql start && psql --version; }
fi

git clone https://github.com/Kitura/Package-Builder.git
./Package-Builder/build-package.sh -projectDir $(pwd)

