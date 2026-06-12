package com.autotrading.autotradingmarketdata.binance;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * 바이낸스 선물(fapi) REST 접속 설정.
 *
 * <p>타임아웃 기본값 근거: 폴링 주기(30s)보다 짧아야 거래소 행(hang) 한 번이
 * 수집 사이클을 통째로 밀어내지 못한다.
 */
@Validated
@ConfigurationProperties(prefix = "binance")
public record BinanceProperties(
        @NotBlank String restBaseUrl,
        @NotNull @DefaultValue("5s") Duration connectTimeout,
        @NotNull @DefaultValue("15s") Duration readTimeout
) {
}
