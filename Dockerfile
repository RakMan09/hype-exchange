# Multi-stage build for the two JVM services. Compose selects the runtime stage
# via `target:` so both images share a single dependency-resolving build layer.

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app
COPY gradlew settings.gradle build.gradle gradle.properties ./
COPY gradle gradle
COPY common common
COPY auctioneer auctioneer
COPY streams streams
RUN chmod +x gradlew && ./gradlew --no-daemon :auctioneer:bootJar :streams:installDist

# --- Auctioneer runtime ---
FROM eclipse-temurin:21-jre AS auctioneer
WORKDIR /app
COPY --from=builder /app/auctioneer/build/libs/auctioneer-0.1.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# --- Streams runtime ---
FROM eclipse-temurin:21-jre AS streams
WORKDIR /app
COPY --from=builder /app/streams/build/install/streams /app/streams
ENTRYPOINT ["/app/streams/bin/streams"]
