#!/bin/bash

CURRENT=`pwd`

export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:///$CURRENT/log4j.properties"

java $JAVA_OPTS  -cp io-rest-jar-with-dependencies.jar  neu.iot.rest.JettyServerRun  $CURRENT/jettyserver.props 