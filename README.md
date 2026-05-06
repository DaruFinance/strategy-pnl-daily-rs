# strategy-pnl-daily-rs

**Fast Rust ETL: `trades.bin` + OHLCV → per-strategy daily PnL Parquet.**

Companion ETL crate to the M-series of reference implementations on
[daru.finance](https://daru.finance). Reads the binary trade
exports produced by
[`quant-research-framework-rs`](https://github.com/DaruFinance/quant-research-framework-rs)
/ `quant-backtester-rs`, joins per-trade candle indices to the asset's
OHLCV timestamp axis, and writes a long-format Parquet at one row per
`(asset, strategy_name, date)`.

The Python prototype of this ETL took ≈ 95 minutes serial across 10
crypto pairs (≈ 500k strategies). The Rust port runs the same workload
in **≈ 6 minutes** with rayon, `Arc`-shared bar-date arrays, and direct
Arrow record-batch construction.

## What it solves

`trades.bin` (see
[`quant-backtester-rs/src/main.rs`](https://github.com/DaruFinance/quant-backtester-rs)
`export_trades_bin`) is a stream of sections. Each section header is
`u16 strat_len, strat, u16 lb_len, lb_label, u16 sec_len, segment,
u32 trade_count`, followed by `trade_count` records of
`u32 entry_idx, u32 exit_idx, i8 side, f64 pnl` (17 bytes per record,
little-endian).

The catch: `entry_idx` / `exit_idx` are **local to each WFO window's
bar slice**, not global indices into the full OHLCV. To recover global
indices, this crate parses each strategy's `.txt` for `OOS_CANDLES` and
`wfo_trigger_val`, then for every section matching `^W(\d+)-OOS$`
adds the offset:

```
oos_global_start = n_total_bars - OOS_CANDLES
offset           = oos_global_start + (window_idx − 1) × wfo_trigger_val
global_idx       = offset + local_idx
```

Without this fix, all 27 windows' worth of OOS trades collapse into the
first ~106 days of the OHLCV (the issue that motivated this rewrite).

## Build and run

```bash
git clone https://github.com/DaruFinance/strategy-pnl-daily-rs
cd strategy-pnl-daily-rs
cargo build --release

./target/release/pnl_daily \
    --assets BTC_30m_27W \
    --assets ETH_30m_28W \
    --out-root /mnt/d/strategies_parquet/pnl_daily
```

Per-asset config is hard-coded in `src/main.rs` (`build_asset_configs`)
to mirror the layout the user has on disk:
- 24 crypto USDT 30m pairs at `/mnt/d/Strategies/<asset>` with OHLCV at
  `/home/daru/golive_pipeline/data/ohlc/`
- BNB 15m + SOL 1h with OHLCV at `/home/daru/data/`
- BCH 30m on the FX root (`/mnt/c/strategies/`)
- 3 forex pairs (AUDUSD, USDCAD, USDCHF) on the FX root with dukascopy
  OHLCV

Adjust the function for your layout.

## Output

```
/mnt/d/strategies_parquet/pnl_daily/asset=<ASSET>/part-00000.parquet
/mnt/d/strategies_parquet/pnl_daily/asset=<ASSET>/_DONE
```

Schema:

| field | type |
|---|---|
| asset | string |
| family | string |
| strategy_name | string |
| date | date32 (UTC) |
| pnl_sum | float64 |
| n_trades | int32 |
| n_long | int32 |
| n_short | int32 |

Compressed with ZSTD. `_DONE` is a sentinel for resume — re-running the
binary skips assets whose sentinel exists.

## Throughput (single 32-core machine, WSL2)

| asset | strategies | aggregate rows | wall clock |
|---|---:|---:|---:|
| BTC_30m_27W | 38,212 | 26,145,617 | 30 s |
| AVAX_30m_17W | 57,318 | 27,606,512 | 102 s |
| BCH_30m_20W | 57,318 | 32,456,607 | 99 s |
| XRP_30M_25W_new | 57,318 | 43,178,856 | 157 s |
| ZEC_30m_22W | 57,318 | 35,304,323 | 120 s |

Throughput is bounded by sequential decompression of `trades.bin` files
on the WSL Windows mount; CPU is comfortably idle.

## License

MIT © Daniel Vieira Gatto.
