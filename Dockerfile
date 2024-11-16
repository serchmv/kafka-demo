FROM eclipse-temurin:21-jdk

WORKDIR /app

# Copiar el archivo JAR
COPY target/kafka-demo-1.0-SNAPSHOT-jar-with-dependencies.jar /app/

# Copiar el script de inicio
COPY start.sh /app/
RUN chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]
