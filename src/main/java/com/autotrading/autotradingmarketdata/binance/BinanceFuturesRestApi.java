package com.autotrading.autotradingmarketdata.binance;

import com.autotrading.autotradingmarketdata.binance.FuturesRows.AggTradeRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.BasisRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.FundingRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.LsRatioRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.OiHistRow;
import com.autotrading.autotradingmarketdata.binance.FuturesRows.TakerRatioRow;
import com.autotrading.autotradingmarketdata.depth.DepthRows;
import com.autotrading.autotradingmarketdata.depth.SymbolUnits;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

@Component
public class BinanceFuturesRestApi {

    private static final ParameterizedTypeReference<List<List<Object>>> KLINE_ROWS =
            new ParameterizedTypeReference<>() {
            };

    private final RestClient restClient;

    public BinanceFuturesRestApi(RestClient binanceRestClient) {
        this.restClient = binanceRestClient;
    }

    /**
     * 캔들 조회 — startTime(epoch ms, inclusive)부터 limit개.
     * symbol은 프로젝트 표준 소문자 페어("btcusdt"), URL에서만 대문자 변환.
     */
    public List<BinanceKline> getKlines(String symbol, String interval, long startTime, int limit) {
        List<List<Object>> rows = restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/fapi/v1/klines")
                        .queryParam("symbol", upper(symbol))
                        .queryParam("interval", interval)
                        .queryParam("startTime", startTime)
                        .queryParam("limit", limit)
                        .build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, BinanceFuturesRestApi::raiseError)
                .body(KLINE_ROWS);
        return rows == null ? List.of() : rows.stream().map(BinanceKline::from).toList();
    }

    // ── /futures/data 통계 + funding + aggTrades (모두 평면 JSON 배열) ──
    //    symbol/pair는 URL에선 대문자(BTCUSDT), 저장 row엔 소문자(btcusdt, 캔들과 키 일관).

    /** Open Interest 통계. period 예: "5m". [startTime, endTime] 범위. */
    public List<OiHistRow> oiHist(String symbol, String period, int limit, long startTime, long endTime) {
        return fetchArray("/futures/data/openInterestHist", symbol, period, limit, startTime, endTime,
                n -> new OiHistRow(symbol,
                        n.path("sumOpenInterest").asDouble(0),
                        n.path("sumOpenInterestValue").asDouble(0),
                        n.path("timestamp").asLong(0)));
    }

    /** Top Trader 포지션 비율. */
    public List<LsRatioRow> topPositionRatio(String symbol, String period, int limit, long startTime, long endTime) {
        return lsRatio("/futures/data/topLongShortPositionRatio", symbol, period, limit, startTime, endTime);
    }

    /** Top Trader 계정 비율. */
    public List<LsRatioRow> topAccountRatio(String symbol, String period, int limit, long startTime, long endTime) {
        return lsRatio("/futures/data/topLongShortAccountRatio", symbol, period, limit, startTime, endTime);
    }

    /** Global 계정 롱/숏 비율. */
    public List<LsRatioRow> globalLsRatio(String symbol, String period, int limit, long startTime, long endTime) {
        return lsRatio("/futures/data/globalLongShortAccountRatio", symbol, period, limit, startTime, endTime);
    }

    private List<LsRatioRow> lsRatio(String path, String symbol, String period, int limit, long startTime, long endTime) {
        return fetchArray(path, symbol, period, limit, startTime, endTime,
                n -> new LsRatioRow(symbol,
                        n.path("longShortRatio").asDouble(0),
                        n.path("longAccount").asDouble(0),
                        n.path("shortAccount").asDouble(0),
                        n.path("timestamp").asLong(0)));
    }

    /** Taker 매수/매도 거래량 비율. */
    public List<TakerRatioRow> takerRatio(String symbol, String period, int limit, long startTime, long endTime) {
        return fetchArray("/futures/data/takerlongshortRatio", symbol, period, limit, startTime, endTime,
                n -> new TakerRatioRow(symbol,
                        n.path("buySellRatio").asDouble(0),
                        n.path("buyVol").asDouble(0),
                        n.path("sellVol").asDouble(0),
                        n.path("timestamp").asLong(0)));
    }

    /** Basis(선물-현물 괴리), contractType=PERPETUAL 고정. */
    public List<BasisRow> basis(String pair, String period, int limit, long startTime, long endTime) {
        JsonNode arr = restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/futures/data/basis")
                        .queryParam("pair", upper(pair))
                        .queryParam("contractType", "PERPETUAL")
                        .queryParam("period", period)
                        .queryParam("limit", limit)
                        .queryParam("startTime", startTime)
                        .queryParam("endTime", endTime)
                        .build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, BinanceFuturesRestApi::raiseError)
                .body(JsonNode.class);
        return parseArray(arr, n -> new BasisRow(pair,
                n.path("futuresPrice").asDouble(0),
                n.path("indexPrice").asDouble(0),
                n.path("basis").asDouble(0),
                n.path("basisRate").asDouble(0),
                n.path("annualizedBasisRate").asDouble(0),
                n.path("timestamp").asLong(0)));
    }

    /** Funding Rate 정산 히스토리 — startTime부터 limit개(오래된→최신). */
    public List<FundingRow> fundingHistory(String symbol, long startTime, int limit) {
        JsonNode arr = restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/fapi/v1/fundingRate")
                        .queryParam("symbol", upper(symbol))
                        .queryParam("startTime", startTime)
                        .queryParam("limit", limit)
                        .build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, BinanceFuturesRestApi::raiseError)
                .body(JsonNode.class);
        return parseArray(arr, n -> new FundingRow(symbol,
                n.path("fundingRate").asDouble(0),
                n.path("fundingTime").asLong(0),
                n.path("markPrice").asDouble(0)));
    }

    /**
     * 집계체결 조회 — fromId(INCLUSIVE)부터 limit개(오래된→최신). WS 갭 보정용.
     * 선물 보존 한도 최근 24시간(그보다 오래된 fromId는 빈 배열).
     */
    public List<AggTradeRow> aggTrades(String symbol, long fromId, int limit) {
        JsonNode arr = restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/fapi/v1/aggTrades")
                        .queryParam("symbol", upper(symbol))
                        .queryParam("fromId", fromId)
                        .queryParam("limit", limit)
                        .build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, BinanceFuturesRestApi::raiseError)
                .body(JsonNode.class);
        return parseArray(arr, n -> new AggTradeRow(symbol,
                n.path("a").asLong(0),
                n.path("p").asDouble(0),
                n.path("q").asDouble(0),
                n.path("f").asLong(0),
                n.path("l").asLong(0),
                n.path("T").asLong(0),
                n.path("m").asBoolean(false)));
    }

    /**
     * 호가창 스냅샷 — 로컬 북 초기화/재동기화용. limit 1000 = weight 20(FAPI 양동이).
     * bids/asks 는 [price, qty] 문자열 그대로(단위 변환은 호출자 — 반올림 금지).
     */
    public DepthRows.Snapshot depthSnapshot(String symbol, int limit) {
        JsonNode n = restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/fapi/v1/depth")
                        .queryParam("symbol", upper(symbol))
                        .queryParam("limit", limit)
                        .build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, BinanceFuturesRestApi::raiseError)
                .body(JsonNode.class);
        if (n == null || !n.has("lastUpdateId")) {
            throw new IllegalStateException("depth 응답 형식 이상: " + n);
        }
        return new DepthRows.Snapshot(n.path("lastUpdateId").asLong(0), n.path("E").asLong(0),
                pairs(n.get("bids")), pairs(n.get("asks")));
    }

    /**
     * exchangeInfo 에서 심볼별 자릿수·필터 — 정수 단위의 근거. 심볼은 소문자 페어로 키를 맞춘다.
     */
    public Map<String, SymbolUnits> symbolUnits(List<String> symbols) {
        JsonNode info = restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/fapi/v1/exchangeInfo").build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, BinanceFuturesRestApi::raiseError)
                .body(JsonNode.class);
        Map<String, SymbolUnits> out = new HashMap<>();
        if (info == null) {
            return out;
        }
        Set<String> want = new HashSet<>();
        for (String s : symbols) {
            want.add(upper(s));
        }
        for (JsonNode s : info.path("symbols")) {
            String sym = s.path("symbol").asString("");
            if (!want.contains(sym)) {
                continue;
            }
            String tick = "";
            String step = "";
            for (JsonNode f : s.path("filters")) {
                String type = f.path("filterType").asString("");
                if ("PRICE_FILTER".equals(type)) {
                    tick = f.path("tickSize").asString("");
                } else if ("LOT_SIZE".equals(type)) {
                    step = f.path("stepSize").asString("");
                }
            }
            out.put(sym.toLowerCase(Locale.ROOT), new SymbolUnits(sym.toLowerCase(Locale.ROOT),
                    s.path("pricePrecision").asInt(), s.path("quantityPrecision").asInt(), tick, step));
        }
        return out;
    }

    private static List<String[]> pairs(JsonNode arr) {
        if (arr == null || !arr.isArray()) {
            return List.of();
        }
        List<String[]> out = new ArrayList<>(arr.size());
        for (JsonNode l : arr) {
            out.add(new String[] {l.path(0).asString("0"), l.path(1).asString("0")});
        }
        return out;
    }

    // ── 공통 ──

    private <T> List<T> fetchArray(String path, String symbol, String period, int limit,
                                   long startTime, long endTime, Function<JsonNode, T> parser) {
        JsonNode arr = restClient.get()
                .uri(uriBuilder -> uriBuilder.path(path)
                        .queryParam("symbol", upper(symbol))
                        .queryParam("period", period)
                        .queryParam("limit", limit)
                        .queryParam("startTime", startTime)
                        .queryParam("endTime", endTime)
                        .build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, BinanceFuturesRestApi::raiseError)
                .body(JsonNode.class);
        return parseArray(arr, parser);
    }

    private static <T> List<T> parseArray(JsonNode arr, Function<JsonNode, T> parser) {
        if (arr == null || !arr.isArray()) {
            return List.of();
        }
        List<T> out = new ArrayList<>(arr.size());
        for (JsonNode n : arr) {
            out.add(parser.apply(n));
        }
        return out;
    }

    private static void raiseError(org.springframework.http.HttpRequest request,
                                   org.springframework.http.client.ClientHttpResponse response) throws java.io.IOException {
        String body = new String(FileCopyUtils.copyToByteArray(response.getBody()), StandardCharsets.UTF_8);
        // 418/429 는 해제 시각을 Retry-After 로 알려준다 — 고정 대기 대신 이 값을 따라야
        // 밴이 안 풀린 상태에서 재개해 밴을 연장하는 되먹임을 끊을 수 있다(2026-08-20).
        String path = request.getURI().getPath();
        long retryAfter = headerLong(response, "Retry-After", 0);
        long usedWeight = headerLong(response, "X-MBX-USED-WEIGHT-1M", -1);
        throw new BinanceRestException(response.getStatusCode().value(),
                "Binance API error: status=" + response.getStatusCode().value()
                        + " uri=" + path
                        + " retryAfter=" + retryAfter + "s usedWeight1m=" + usedWeight
                        + " body=" + body,
                retryAfter, usedWeight, RateBucket.of(path));
    }

    private static long headerLong(org.springframework.http.client.ClientHttpResponse response,
                                   String name, long fallback) {
        String v = response.getHeaders().getFirst(name);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static String upper(String symbol) {
        return symbol.toUpperCase(Locale.ROOT);
    }
}
