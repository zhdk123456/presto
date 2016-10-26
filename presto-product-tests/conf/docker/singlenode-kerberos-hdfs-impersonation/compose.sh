#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/../common/compose-commons.sh

docker-compose \
-f ${BASH_SOURCE%/*}/../common/base.yml \
-f ${BASH_SOURCE%/*}/../common/hive.yml \
-f ${BASH_SOURCE%/*}/../common/presto.yml \
-f ${BASH_SOURCE%/*}/../common/kerberos.yml \
-f ${BASH_SOURCE%/*}/../common/jdbc_db.yml \
-f ${BASH_SOURCE%/*}/docker-compose.yml \
"$@"
