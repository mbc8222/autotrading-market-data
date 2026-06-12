package com.autotrading.autotradingmarketdata.collect;

import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * 수집 공통 설정 — 대상 심볼은 kline/파생/WS/raw가 공유한다.
 * 심볼은 프로젝트 표준 소문자 페어("btcusdt").
 */
@Validated
@ConfigurationProperties(prefix = "collect")
public record CollectProperties(
        @NotEmpty List<String> symbols
) {
}
