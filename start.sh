#!/bin/bash

if [ "$APP_TYPE" = "producer" ]; then
    echo "Starting Producer..."
    exec java -cp kafka-demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.producer.MessageProducer
elif [ "$APP_TYPE" = "consumer" ]; then
    echo "Starting Consumer..."
    exec java -cp kafka-demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.consumer.MessageConsumer
else
    echo "Invalid APP_TYPE. Must be producer or consumer"
    exit 1
fi
