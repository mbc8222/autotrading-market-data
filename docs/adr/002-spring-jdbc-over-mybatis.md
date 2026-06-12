# ADR-002: DB 접근은 spring-jdbc 표준 도구 (MyBatis 미채택)

- 상태: 채택 (2026-06-11)

## 맥락

모놀리스(AutoTrading)는 MyBatis(@Mapper + XML) 컨벤션을 쓴다. 이 서비스의 DB 접근 기술을 정해야 한다.

## 결정

추가 의존성 없이 **spring-jdbc 표준 도구**를 쓴다:
- 단건/조회: `JdbcClient` (Boot 3.2+의 현대식 fluent API)
- 대량 적재: `NamedParameterJdbcTemplate.batchUpdate` (JdbcClient은 batch 미지원)

## 근거

1. 이 서비스의 SQL은 수집 적재(upsert) + resume 조회 수준 — XML 매퍼 계층이 줄 게 없다.
2. starter-jdbc에 이미 포함 — 의존성 0개 추가.
3. MSA에서 서비스별 기술 선택은 독립 — 모놀리스 컨벤션을 따를 의무 없음.

## 재검토 조건

조회 SQL이 동적 조건으로 복잡해지면(분석용 쿼리 등) MyBatis 도입 재검토. 단, 분석 쿼리는 ② 분석 서비스 소관이므로 가능성 낮음.
