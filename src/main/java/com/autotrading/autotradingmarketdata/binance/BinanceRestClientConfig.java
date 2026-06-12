package com.autotrading.autotradingmarketdata.binance;

import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class BinanceRestClientConfig {

    @Bean
    public RestClient binanceRestClient(BinanceProperties properties) {
        // Boot 4.0에서 ClientHttpRequestFactorySettings → HttpClientSettings 로 개명됨
        HttpClientSettings settings = HttpClientSettings.defaults()
                .withTimeouts(properties.connectTimeout(), properties.readTimeout());
        return RestClient.builder()
                .baseUrl(properties.restBaseUrl())
                .requestFactory(ClientHttpRequestFactoryBuilder.detect().build(settings))
                .build();
    }
}
