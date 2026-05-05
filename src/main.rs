//! pnl_daily ETL — fast Rust port.
//!
//! For each (asset, strategy) folder:
//!   1. Read trades.bin (binary, see format in
//!      quant-backtester-rs/src/main.rs:1922 export_trades_bin).
//!   2. For every section whose tag matches `^W\d+-OOS$`, add the per-window
//!      bar offset (n_total - oos_total + (window-1)*per_window) so that
//!      entry/exit indices become global bar indices.
//!   3. Look up the bar's UTC date in the asset's OHLCV CSV.
//!   4. Aggregate per (strategy, date) -> (sum_pnl, n_trades, n_long, n_short).
//!
//! Writes one Parquet file per asset to
//! /mnt/d/strategies_parquet/pnl_daily/asset=<asset>/part-00000.parquet
//! with a _DONE sentinel.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Date32Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use regex::Regex;

/// Per-asset config.
struct AssetConfig {
    asset: String,
    strategies_root: PathBuf,
    ohlcv_path: PathBuf,
}

/// CLI.
#[derive(Parser, Debug)]
#[command(name = "pnl_daily")]
#[command(about = "Fast ETL: trades.bin -> per-strategy daily PnL Parquet")]
struct Cli {
    /// Output root (one subdir per asset).
    #[arg(long, default_value = "/mnt/d/strategies_parquet/pnl_daily")]
    out_root: PathBuf,

    /// Number of rayon threads (0 = auto).
    #[arg(short = 'j', long, default_value_t = 0)]
    jobs: usize,

    /// Skip assets whose _DONE sentinel exists.
    #[arg(long, default_value_t = true)]
    resume: bool,

    /// Only process these assets (substring match against asset folder name).
    #[arg(long)]
    assets: Vec<String>,
}

/// Hard-coded asset config (mirror of the Python version).
fn build_asset_configs() -> Vec<AssetConfig> {
    let crypto_root = Path::new("/mnt/d/Strategies");
    let fx_root = Path::new("/mnt/c/strategies");
    let ohlc_30m = Path::new("/home/daru/golive_pipeline/data/ohlc");
    let other = Path::new("/home/daru/data");
    let fx = Path::new("/home/daru/golive_pipeline/data/ohlc");

    let mut out = Vec::new();
    let mut add = |asset: &str, root: &Path, ohlc: PathBuf| {
        out.push(AssetConfig {
            asset: asset.into(),
            strategies_root: root.join(asset),
            ohlcv_path: ohlc,
        });
    };

    // Crypto 30m (24 assets, all from golive_pipeline/data/ohlc/)
    let crypto_30m = [
        ("AAVE_30m_17W", "AAVEUSDT_30m_3_9.csv"),
        ("ALGO_30m_6W_1MetaW", "ALGOUSDT_30m_3_9.csv"),
        ("APE_30m_12W", "APEUSDT_30m_3_9.csv"),
        ("APT_30m_6W_1MetaW", "APTUSDT_30m_3_9.csv"),
        ("ARB_30m_6W_1MetaW", "ARBUSDT_30m_3_9.csv"),
        ("ATOM_30m_6W_1MetaW", "ATOMUSDT_30m_3_9.csv"),
        ("AVAX_30m_17W", "AVAXUSDT_30m_3_9.csv"),
        // BCH is intentionally NOT here — its strategy folder lives on the
        // forex root (/mnt/c/strategies/), so it's added separately below.
        ("BTC_30m_27W", "BTCUSDT_30m_3_9.csv"),
        ("DOGE_30m_21W", "DOGEUSDT_30m_3_9.csv"),
        ("DOT_30m_6W_1MetaW", "DOTUSDT_30m_3_9.csv"),
        ("ETC_30m_6W_1MetaW", "ETCUSDT_30m_3_9.csv"),
        ("ETH_30m_28W", "ETHUSDT_30m_3_9.csv"),
        ("HBAR_30m_6W_1MetaW", "HBARUSDT_30m_3_9.csv"),
        ("ICP_30m_6W_1MetaW", "ICPUSDT_30m_3_9.csv"),
        ("LINK_30M_23W_new", "LINKUSDT_30m_3_9.csv"),
        ("LTC_30m_27W", "LTCUSDT_30m_3_9.csv"),
        ("NEAR_30m_17W", "NEARUSDT_30m_3_9.csv"),
        ("SHIB_30m_15W", "SHIBUSDT_30m_3_9.csv"),
        ("SUI_30m_6W_1MetaW", "SUIUSDT_30m_3_9.csv"),
        ("TRX_30m_25W", "TRXUSDT_30m_3_9.csv"),
        ("UNI_30M_17W_new", "UNIUSDT_30m_3_9.csv"),
        ("XLM_30m_6W_1MetaW", "XLMUSDT_30m_3_9.csv"),
        ("XRP_30M_25W_new", "XRPUSDT_30m_3_9.csv"),
        ("ZEC_30m_22W", "ZECUSDT_30m_3_9.csv"),
    ];
    for (asset, csv) in crypto_30m {
        add(asset, crypto_root, ohlc_30m.join(csv));
    }

    // BNB 15m and SOL 1h have non-30m timeframes -> /home/daru/data/
    add("BNB_15m_30W", crypto_root, other.join("BNBUSDT_15m_3_9.csv"));
    add("SOL_1h_7W", crypto_root, other.join("SOLUSDT_1h_3_9.csv"));

    // BCH lives on /mnt/c/strategies/ even though it's spot-crypto data.
    add("BCH_30m_20W", fx_root, ohlc_30m.join("BCHUSDT_30m_3_9.csv"));

    // Forex 1h
    add("AUDUSD_1h_forex", fx_root, fx.join("AUDUSD_1h_3_9.csv"));
    add("USDCAD_1h_forex", fx_root, fx.join("USDCAD_1h_3_9.csv"));
    add("USDCHF_1h_forex", fx_root, fx.join("USDCHF_1h_3_9.csv"));

    out
}

/// Read OHLCV CSV (time,open,high,low,close) and return one UTC date per bar.
fn load_bar_dates(path: &Path) -> std::io::Result<Vec<NaiveDate>> {
    let f = File::open(path)?;
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(f);
    let headers = rdr.headers()?.clone();
    let time_idx = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case("time") || h.eq_ignore_ascii_case("timestamp"))
        .unwrap_or(0);
    let mut out = Vec::with_capacity(1 << 17);
    for rec in rdr.records() {
        let rec = rec?;
        let s = rec.get(time_idx).unwrap_or("");
        // accept either epoch seconds or epoch ms
        let secs: i64 = match s.parse::<i64>() {
            Ok(v) => {
                if v > 10_000_000_000 {
                    v / 1000
                } else {
                    v
                }
            }
            Err(_) => continue,
        };
        let dt = DateTime::<Utc>::from_timestamp(secs, 0).unwrap_or_default();
        out.push(NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day()).unwrap());
    }
    Ok(out)
}

/// Read OOS_CANDLES and wfo_trigger_val from any strategy's .txt.
fn read_wfo_config(strat_dir: &Path) -> Option<(usize, usize)> {
    let stem = strat_dir.file_name()?.to_str()?.to_string();
    let txt = strat_dir.join(format!("{stem}.txt"));
    let content = if txt.is_file() {
        fs::read_to_string(&txt).ok()?
    } else {
        // fallback: any .txt
        let entry = fs::read_dir(strat_dir).ok()?.filter_map(|e| e.ok()).find(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "txt")
                .unwrap_or(false)
        })?;
        fs::read_to_string(entry.path()).ok()?
    };
    let mut oos_total = None;
    let mut per_window = None;
    let re_oos = Regex::new(r"OOS_CANDLES\s*=\s*(\d+)").unwrap();
    let re_pw = Regex::new(r"wfo_trigger_val\(\)\s*=\s*(\d+)").unwrap();
    for line in content.lines() {
        if oos_total.is_none() {
            if let Some(c) = re_oos.captures(line) {
                oos_total = c.get(1).and_then(|m| m.as_str().parse::<usize>().ok());
            }
        }
        if per_window.is_none() {
            if let Some(c) = re_pw.captures(line) {
                per_window = c.get(1).and_then(|m| m.as_str().parse::<usize>().ok());
            }
        }
        if oos_total.is_some() && per_window.is_some() {
            break;
        }
    }
    Some((oos_total?, per_window?))
}

/// Aggregator returned per strategy.
type DateAgg = (f64, i32, i32, i32); // (sum_pnl, n_trades, n_long, n_short)

/// Parse one strategy's trades.bin and aggregate per (date_idx) using the
/// caller-supplied date table. Returns Vec<(date_idx, agg)>. The strategy's
/// own family is given so we don't need to thread it back via the path.
fn parse_strategy(
    strat_dir: &Path,
    bar_dates: &[NaiveDate],
    oos_global_start: i64,
    per_window: i64,
) -> Vec<(i32, DateAgg)> {
    let bin_path = strat_dir.join("trades.bin");
    let mut buf = Vec::new();
    let mut f = match File::open(&bin_path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    if f.read_to_end(&mut buf).is_err() {
        return Vec::new();
    }
    let n_bars = bar_dates.len() as i64;
    let mut pos = 0usize;
    let mut agg: HashMap<i32, DateAgg> = HashMap::new();
    let bytes = &buf[..];
    let n = bytes.len();
    // section header: u16 strat_len, strat_bytes, u16 lb_len, lb_bytes,
    //                 u16 sec_len, sec_bytes, u32 trade_count
    while pos < n {
        if n - pos < 6 {
            break;
        }
        let strat_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2 + strat_len;
        if pos + 2 > n {
            break;
        }
        let lb_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2 + lb_len;
        if pos + 2 > n {
            break;
        }
        let sec_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        if pos + 2 + sec_len > n {
            break;
        }
        let sec = std::str::from_utf8(&bytes[pos + 2..pos + 2 + sec_len]).unwrap_or("");
        pos += 2 + sec_len;
        if pos + 4 > n {
            break;
        }
        let cnt = u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
            as usize;
        pos += 4;

        // Match ^W(\d+)-OOS$
        let window_idx: Option<i64> = (|| -> Option<i64> {
            if !sec.starts_with('W') || !sec.ends_with("-OOS") {
                return None;
            }
            let core = &sec[1..sec.len() - 4];
            core.parse::<i64>().ok()
        })();

        if window_idx.is_none() {
            // skip trades quickly
            let trade_bytes = cnt * 17;
            if pos + trade_bytes > n {
                break;
            }
            pos += trade_bytes;
            continue;
        }
        let w = window_idx.unwrap();
        let offset = oos_global_start + (w - 1) * per_window;

        let trade_bytes = cnt * 17;
        if pos + trade_bytes > n {
            break;
        }
        let chunk = &bytes[pos..pos + trade_bytes];
        pos += trade_bytes;
        for i in 0..cnt {
            let off = i * 17;
            let entry =
                u32::from_le_bytes([chunk[off], chunk[off + 1], chunk[off + 2], chunk[off + 3]])
                    as i64;
            let exit_idx = u32::from_le_bytes([
                chunk[off + 4],
                chunk[off + 5],
                chunk[off + 6],
                chunk[off + 7],
            ]) as i64;
            let _ = entry;
            let side = chunk[off + 8] as i8;
            let pnl = f64::from_le_bytes([
                chunk[off + 9],
                chunk[off + 10],
                chunk[off + 11],
                chunk[off + 12],
                chunk[off + 13],
                chunk[off + 14],
                chunk[off + 15],
                chunk[off + 16],
            ]);
            let global = exit_idx + offset;
            if global < 0 || global >= n_bars {
                continue;
            }
            let date = bar_dates[global as usize];
            let date_idx = epoch_days(date);
            let entry = agg.entry(date_idx).or_insert((0.0, 0, 0, 0));
            entry.0 += pnl;
            entry.1 += 1;
            if side >= 1 {
                entry.2 += 1;
            } else {
                entry.3 += 1;
            }
        }
    }
    let mut out: Vec<(i32, DateAgg)> = agg.into_iter().collect();
    out.sort_by_key(|(k, _)| *k);
    out
}

fn epoch_days(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (d - epoch).num_days() as i32
}

/// Walk strategies_root/{family}/{strategy}/ and return strategy folder paths.
fn list_strategies(asset_root: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(asset_root) {
        Ok(e) => e,
        Err(_) => return out,
    };
    for fam_entry in entries.filter_map(|e| e.ok()) {
        if !fam_entry.path().is_dir() {
            continue;
        }
        let fam_name = fam_entry.file_name().to_string_lossy().to_string();
        let strat_iter = match fs::read_dir(fam_entry.path()) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for sd in strat_iter.filter_map(|e| e.ok()) {
            if sd.path().is_dir() {
                out.push((fam_name.clone(), sd.path()));
            }
        }
    }
    out
}

fn process_asset(cfg: &AssetConfig, out_root: &Path) -> std::io::Result<()> {
    let out_dir = out_root.join(format!("asset={}", cfg.asset));
    fs::create_dir_all(&out_dir)?;
    let done = out_dir.join("_DONE");
    if done.is_file() {
        eprintln!("[pnl] {}: already done, skipping", cfg.asset);
        return Ok(());
    }

    let t0 = Instant::now();
    eprintln!("[pnl] {}: loading {:?}", cfg.asset, cfg.ohlcv_path);
    let bar_dates = load_bar_dates(&cfg.ohlcv_path)?;
    eprintln!(
        "[pnl] {}: {} bars, first={}, last={}",
        cfg.asset,
        bar_dates.len(),
        bar_dates.first().unwrap(),
        bar_dates.last().unwrap()
    );

    let strats = list_strategies(&cfg.strategies_root);
    eprintln!("[pnl] {}: {} strategy folders", cfg.asset, strats.len());
    if strats.is_empty() {
        return Ok(());
    }
    let cfg_pair = strats
        .iter()
        .take(50)
        .find_map(|(_, p)| read_wfo_config(p));
    let (oos_total, per_window) = match cfg_pair {
        Some(x) => x,
        None => {
            eprintln!("[pnl] {}: cannot read WFO config, skipping", cfg.asset);
            return Ok(());
        }
    };
    let n_bars = bar_dates.len();
    let oos_global_start = (n_bars as i64) - (oos_total as i64);
    eprintln!(
        "[pnl] {}: oos_total={}, per_window={}, oos_global_start={}",
        cfg.asset, oos_total, per_window, oos_global_start
    );

    // Parallel parse
    let bar_ref = Arc::new(bar_dates);
    let bar_ref_for_parallel = bar_ref.clone();
    let results: Vec<(String, String, Vec<(i32, DateAgg)>)> = strats
        .par_iter()
        .map(|(fam, sp)| {
            let bd = bar_ref_for_parallel.clone();
            let agg = parse_strategy(sp, &bd, oos_global_start, per_window as i64);
            let strat_name = sp
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            (fam.clone(), strat_name, agg)
        })
        .collect();

    // Build columnar arrays in one go
    let mut col_asset = Vec::<String>::new();
    let mut col_family = Vec::<String>::new();
    let mut col_strat = Vec::<String>::new();
    let mut col_date = Vec::<i32>::new();
    let mut col_pnl = Vec::<f64>::new();
    let mut col_n = Vec::<i32>::new();
    let mut col_l = Vec::<i32>::new();
    let mut col_s = Vec::<i32>::new();
    for (fam, strat, agg) in &results {
        for (date_idx, (sum_pnl, n_trades, n_long, n_short)) in agg {
            col_asset.push(cfg.asset.clone());
            col_family.push(fam.clone());
            col_strat.push(strat.clone());
            col_date.push(*date_idx);
            col_pnl.push(*sum_pnl);
            col_n.push(*n_trades);
            col_l.push(*n_long);
            col_s.push(*n_short);
        }
    }
    let n_rows = col_pnl.len();
    eprintln!(
        "[pnl] {}: {} aggregate rows from {} strategies in {:.1}s",
        cfg.asset,
        n_rows,
        results.len(),
        t0.elapsed().as_secs_f32()
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("asset", DataType::Utf8, false),
        Field::new("family", DataType::Utf8, false),
        Field::new("strategy_name", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
        Field::new("pnl_sum", DataType::Float64, false),
        Field::new("n_trades", DataType::Int32, false),
        Field::new("n_long", DataType::Int32, false),
        Field::new("n_short", DataType::Int32, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(col_asset)) as ArrayRef,
        Arc::new(StringArray::from(col_family)) as ArrayRef,
        Arc::new(StringArray::from(col_strat)) as ArrayRef,
        Arc::new(Date32Array::from(col_date)) as ArrayRef,
        Arc::new(Float64Array::from(col_pnl)) as ArrayRef,
        Arc::new(Int32Array::from(col_n)) as ArrayRef,
        Arc::new(Int32Array::from(col_l)) as ArrayRef,
        Arc::new(Int32Array::from(col_s)) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("recordbatch");
    let path = out_dir.join("part-00000.parquet");
    let f = File::create(&path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut w = ArrowWriter::try_new(f, schema, Some(props)).expect("arrow writer");
    w.write(&batch).expect("write batch");
    w.close().expect("close writer");
    fs::write(done, "ok\n")?;
    eprintln!(
        "[pnl] {}: DONE in {:.1}s, parquet={}",
        cfg.asset,
        t0.elapsed().as_secs_f32(),
        path.display()
    );
    Ok(())
}

fn main() {
    let cli = Cli::parse();
    if cli.jobs > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(cli.jobs)
            .build_global()
            .expect("rayon threadpool");
    }
    fs::create_dir_all(&cli.out_root).expect("mkdir out_root");
    let cfgs = build_asset_configs();
    let cfgs: Vec<_> = if cli.assets.is_empty() {
        cfgs
    } else {
        cfgs.into_iter()
            .filter(|c| cli.assets.iter().any(|a| c.asset.contains(a)))
            .collect()
    };
    eprintln!("[pnl] {} assets to process", cfgs.len());
    let t0 = Instant::now();
    for cfg in &cfgs {
        if !cfg.ohlcv_path.is_file() {
            eprintln!(
                "[pnl] {}: OHLCV missing at {:?}, skipping",
                cfg.asset, cfg.ohlcv_path
            );
            continue;
        }
        if !cfg.strategies_root.is_dir() {
            eprintln!(
                "[pnl] {}: strategies dir missing at {:?}, skipping",
                cfg.asset, cfg.strategies_root
            );
            continue;
        }
        if let Err(e) = process_asset(cfg, &cli.out_root) {
            eprintln!("[pnl] {}: error {:?}", cfg.asset, e);
        }
    }
    eprintln!("[pnl] all done in {:.1}s", t0.elapsed().as_secs_f32());
}
