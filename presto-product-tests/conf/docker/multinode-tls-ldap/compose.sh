#!/usr/bin/env bash

SCRIPT_DIRECTORY=${BASH_SOURCE%/*}

source ${SCRIPT_DIRECTORY}/../common/compose-commons.sh

TEMPTO_CONFIG_YAML_DEFAULT="${PRODUCT_TESTS_ROOT}/conf/tempto/tempto-configuration-for-docker-tls-ldap.yaml"
export TEMPTO_CONFIG_YAML=$(canonical_path ${TEMPTO_CONFIG_YAML:-${TEMPTO_CONFIG_YAML_DEFAULT}})

docker-compose \
-f ${SCRIPT_DIRECTORY}/../common/standard.yml \
-f ${SCRIPT_DIRECTORY}/../common/jdbc_db.yml \
-f ${SCRIPT_DIRECTORY}/docker-compose.yml \
"$@"
