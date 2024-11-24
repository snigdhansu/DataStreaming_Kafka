FROM openjdk:8u151-jdk-alpine3.7

# Install Bash and dos2unix
RUN apk --no-cache add bash libc6-compat dos2unix

# Copy resources
WORKDIR /
COPY wait-for-it.sh wait-for-it.sh
COPY target/flink-kafka2postgres-1.0-SNAPSHOT-jar-with-dependencies.jar flink-processor.jar

# Convert line endings in wait-for-it.sh - Windows configuration
RUN dos2unix wait-for-it.sh
RUN chmod +x wait-for-it.sh

# Wait for Zookeeper and Kafka to be available and run application
CMD ./wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 30 $KAFKA_SERVER -- java -Xmx512m -jar flink-processor.jar
