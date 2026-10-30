# Alternative data feeds — checked 30 October 2026

## Requirement

Keep one-second capture of NIFTY calls and puts across all required expiries,
including LTP, bid/ask prices and quantities, volume, OI and IV. Use an authenticated
stream to maintain current instrument values, then write a snapshot every second.
This preserves the capture interval without issuing whole-chain REST requests
every second. A captured second is not a promise that every instrument updated
during that second; retain per-leg source times and stale-value markers.

## Candidates

| Source | Cost/access | Fit and outstanding checks |
|---|---|---|
| Upstox | APIs advertised free; account required. Read-only Analytics Token lasts one year, supports market data/WebSocket access without static IP. | First candidate if opening an account is acceptable. Full feed includes prices, depth, OI, IV, traded volume and buy/sell totals. Full-only subscription limit is 2,000 instrument keys; mixed modes have lower limits. Verify every required expiry/strike against its contract master before adopting it. |
| Dhan | Broker account and Data API subscription; published price ₹499/month plus taxes. | Up to 5 connections with 5,000 instruments each. Full packets include price, bid/ask, volume and OI; IV is not listed in that packet and needs a separate plan for parity. Access tokens last 24 hours, with renewal available for eligible active tokens. |
| Zerodha Kite Connect | Broker account; data plan ₹500/month. | 3 WebSocket connections, up to 3,000 instruments each. Good streaming candidate for price/depth/OI; IV is not a direct quote field and requires separate calculation/data. |
| TrueData API | Standalone vendor access, subject to onboarding; API pricing depends on requested exchanges, symbols and data type. | Streaming and option-chain/Greeks products are offered. Request exact NSE F&O coverage, retention permission and cloud/API pricing. Workspace plan prices do **not** include API access. |
| Global Datafeeds API | Standalone vendor subscription; confirm the applicable API plan and instrument allowance directly. | Official documentation advertises one-second updates, derivatives coverage and WebSockets. Verify all expiries, IV/OI fields and the API-specific quote/trial before purchase. |

The user currently has no broker account. No provider has been provisioned and
no subscription has been purchased. I have not verified a permanently free,
unauthenticated alternative meeting the full one-second chain requirement.

## Integration checks before changing the archive

- The first stored snapshots on 29 October contained 722 strike rows and 1,444
  nonempty CE/PE legs across 18 expiries. This is a sample, not a full broker
  contract-universe count; the eventual subscription may be larger.
- Prefer streaming to repeated chain REST calls. Upstox documents standard
  endpoint limits including 500 requests/minute and 2,000/30 minutes; repeatedly
  polling 18 expiries each second is not the proposed design.
- Preserve source identity and upstream timestamps. A broker stream does not
  provide NSE website `records.timestamp` semantics for an entire chain; do not
  silently mix synthetic snapshot times with historical NSE source timestamps.
- Compare OI, traded volume, IV scales, lot sizes and previous-session reference
  values before mapping to the existing CSV columns. Unavailable fields must be
  marked missing, not populated with invented zeroes.
- Test cloud authentication, reconnect behavior, complete subscription coverage
  and an entire session of one-second snapshots before enabling daily jobs.

## Primary sources

- [Upstox Analytics Token](https://upstox.com/developer/api-documentation/analytics-token/)
- [Upstox API pricing FAQ](https://upstox.com/trading-api/)
- [Upstox V3 streaming fields and limits](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/)
- [Upstox rate limits](https://upstox.com/developer/api-documentation/rate-limiting/)
- [Dhan Data API price](https://dhan.co/support/platforms/dhanhq-api/how-can-i-access-live-market-data-through-dhan/)
- [Dhan feed format and subscription limits](https://dhanhq.co/docs/v2/live-market-feed/)
- [Dhan token lifecycle](https://dhanhq.co/docs/v2/authentication/)
- [Kite data API price](https://support.zerodha.com/category/trading-and-markets/general-kite/kite-api/articles/what-are-the-charges-for-kite-apis)
- [Kite streaming limits and fields](https://kite.trade/docs/connect/v3/websocket/)
- [TrueData APIs and quote process](https://www.truedata.in/market-data-apis)
- [TrueData workspace API exclusion](https://www.truedata.in/workspace/pricing)
- [Global Datafeeds API overview](https://globaldatafeeds.in/global-datafeeds-apis/global-datafeeds-apis/introduction/introduction-to-apis/)
