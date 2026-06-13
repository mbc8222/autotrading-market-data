# syntax=docker/dockerfile:1
#
# autotrading-market-data 컨테이너 이미지 (멀티스테이지)
# 빌드: docker compose build  /  docker build -t autotrading-market-data .
#
# 컨테이너 안에서 ./gradlew bootJar 로 직접 빌드한다 → IntelliJ "Build Artifacts" 함정
# (thin jar / duplicate MANIFEST)을 원천 차단하고 어디서 빌드하든 동일 산출물(재현성)을 보장.

# ---- 빌드 스테이지: JDK 21 + Gradle ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app

# 래퍼/빌드스크립트 먼저 복사 (소스보다 덜 바뀜 → 레이어 캐시 효율)
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
RUN chmod +x ./gradlew

# 소스 복사 후 bootJar. Gradle 의존성/캐시는 BuildKit 캐시 마운트로 재사용(재빌드 가속).
COPY src ./src
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon clean bootJar

# ---- 런타임 스테이지: JRE 21 (이미지 경량) ----
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

# 비루트 실행 계정 + 로그 디렉터리(볼륨 마운트 지점) 소유권.
# log4j2-spring.xml 의 prod 파일 appender 가 /var/log/autotrading 에 기록한다.
RUN useradd -r -u 1001 -m appuser \
 && mkdir -p /var/log/autotrading \
 && chown appuser:appuser /var/log/autotrading
USER appuser

# 빌드 스테이지의 실행가능 bootJar 만 복사(소스/Gradle 캐시는 최종 이미지에 안 남음).
# clean bootJar 만 수행하므로 plain jar 는 생성되지 않아 글롭이 정확히 1개 매칭.
COPY --from=build --chown=appuser:appuser /app/build/libs/*-SNAPSHOT.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
