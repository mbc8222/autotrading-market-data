package com.autotrading.autotradingmarketdata.kline;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * 캔들 수집 설정. 대상 심볼은 {@code collect.symbols}(공유)를 사용한다.
 */
@Validated
@ConfigurationProperties(prefix = "collect.kline")
public record KlineCollectProperties(
        @NotEmpty List<String> intervals,
        @Min(1) @DefaultValue("7") int backfillDays,
        @Min(100) @Max(1500) @DefaultValue("1500") int pageLimit
) {
}
