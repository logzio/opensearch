FROM eclipse-temurin:21-jre-alpine
COPY build/libs/kafka-message-wrapper.jar /app/wrapper.jar
ENTRYPOINT ["java", "-jar", "/app/wrapper.jar"]
