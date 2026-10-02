# Branch producer image. Built by docker-compose.yml, or: docker build -t branch-sales-producer .

FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
# Dependencies first, so they stay cached while only the code changes
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN ./mvnw -B -q dependency:go-offline
COPY src src
# Tests need Docker (Testcontainers) and the contract/backoffice folders; they run outside the image build
RUN ./mvnw -B -q package -Dmaven.test.skip=true

FROM eclipse-temurin:25-jre
RUN useradd --system --uid 10001 app
USER app
WORKDIR /app
COPY --from=build /src/target/branch-sales-producer-*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
