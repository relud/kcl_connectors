#!/bin/bash

# create a properties file so that all configs are accepted as env vars
cat > kcl.properties << EOF
AWSCredentialsProvider = DefaultAWSCredentialsProviderChain
applicationName = $APPLICATION_NAME
executableName = python -m kcl_connectors.$CONNECTOR
processingLanguage = python/2.7
regionName = $REGION_NAME
streamName = $STREAM_NAME
EOF

# ask kclpy helper to tell us what command to run
$(amazon_kclpy_helper.py --print_command -j java -p kcl.properties)
