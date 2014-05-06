#!/bin/bash
BASEDIR=$(dirname $0)/..
hadoop jar  ${BASEDIR}/target/camus-etl-1.0-SNAPSHOT-shaded.jar com.linkedin.camus.etl.kafka.CamusJob -P ${BASEDIR}/src/main/resources/camus-json-to-avro.properties
