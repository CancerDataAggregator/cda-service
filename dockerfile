FROM gradle:6.9-jdk11-alpine AS TEMP_BUILD_IMAGE
ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME
COPY . .
RUN set -eux; \
    ./gradlew build
# COPY build.gradle settings.gradle $APP_HOME
#
# COPY gradle $APP_HOME/gradle
# COPY --chown=gradle:gradle . /home/gradle/src
# USER root
# RUN chown -R gradle /home/gradle/src
#
# RUN gradle build || return 0
# COPY . .
# RUN gradle clean build


# actual container

FROM adoptopenjdk/openjdk11:alpine-jre AS FINAL
ENV APP_HOME=/usr/app/

WORKDIR $APP_HOME
COPY --from=TEMP_BUILD_IMAGE $APP_HOME/build/libs/cda-service-2.1.0.jar .

EXPOSE 8080
CMD ["java" , "-jar","cda-service-2.1.0.jar"]

