
# Build stage

FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline

COPY src ./src
RUN mvn package -DskipTests

# Run stage
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

COPY --from=builder /app/target/java-etl-1.0-SNAPSHOT.jar app.jar

ENV JDBC_URL=""
ENV DB_USER=""
ENV DB_PASSWORD=""

CMD ["sh", "-c", "java -jar app.jar"]
