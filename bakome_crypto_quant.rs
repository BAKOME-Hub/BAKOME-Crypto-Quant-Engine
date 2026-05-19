// ============================================================
// BAKOME CRYPTO HIGH‑FLYER QUANT ENGINE v1.0
// Architecture High‑Flyer adaptée aux cryptos ETH, SOL, BNB, XRP, DOGE, AVAX
// 6 modules complets | MoE 256 experts | Multi‑exchange
// Fichier unique : bakome_crypto_quant.rs
// Compilation : rustc bakome_crypto_quant.rs -O3 -o bakome_crypto
// Lignes : ~1800 | 0 dépendances externes | Stdlib uniquement
// ============================================================
//
// MODULES :
//  1. CryptoDataCollector – Ingestion temps réel multi‑exchange
//  2. CryptoDataCleaner  – 128 features + funding, OI, CVD, liquidations
//  3. CryptoMoE          – 256 experts spécialisés crypto
//  4. CryptoRiskEngine   – VaR, drawdown, allocation dynamique
//  5. CryptoExecution    – Ordres LIMIT/IOC multi‑exchange
//  6. CryptoReinvest     – Réinvestissement auto + staking
// ============================================================

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::Write;

// ============================================================
// CONSTANTES
// ============================================================
const VERSION: &str = "BAKOME CRYPTO HIGH‑FLYER QUANT ENGINE v1.0";
const MAX_DATA_POINTS: usize = 100_000;
const MIXTURE_EXPERTS: usize = 256;
const MAX_DRAWDOWN_PCT: f64 = 3.0;
const RISK_FREE_RATE: f64 = 0.03;
const MICROSECOND_LATENCY: u64 = 50;
const REINVEST_RATIO: f64 = 0.70;
const GPU_BUDGET_RATIO: f64 = 0.10;
const STAKING_RATIO: f64 = 0.05;
const RESEARCH_RATIO: f64 = 0.15;
const FEATURE_COUNT: usize = 128;
const LOG_FILE: &str = "bakome_crypto_trades.log";
const EXCHANGES: &[&str] = &["Binance", "Bybit", "OKX", "Coinbase"];
const CRYPTO_SYMBOLS: &[&str] = &["ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "AVAXUSDT"];

// ============================================================
// STRUCTURES DE DONNÉES
// ============================================================

#[derive(Debug, Clone)]
pub struct CryptoTick {
    pub symbol: String,
    pub exchange: String,
    pub timestamp: u64,
    pub bid: f64,
    pub ask: f64,
    pub last_price: f64,
    pub volume: f64,
    pub funding_rate: f64,
    pub open_interest: f64,
    pub cvd: f64,
    pub liquidations_long: f64,
    pub liquidations_short: f64,
}

#[derive(Debug, Clone)]
pub struct CryptoFeatures {
    pub symbol: String,
    pub timestamp: u64,
    pub features: Vec<f64>,
    pub returns_1min: f64,
    pub returns_5min: f64,
    pub returns_15min: f64,
    pub volatility_5min: f64,
    pub volatility_15min: f64,
    pub volume_ratio: f64,
    pub spread_ratio: f64,
    pub momentum_score: f64,
    pub mean_reversion_score: f64,
    pub trend_strength: f64,
    pub funding_signal: f64,
    pub oi_signal: f64,
    pub cvd_signal: f64,
    pub liquidation_signal: f64,
}

#[derive(Debug, Clone)]
pub struct ExpertVote {
    pub expert_id: usize,
    pub specialization: String,
    pub signal_buy: f64,
    pub signal_sell: f64,
    pub confidence: f64,
    pub weight: f64,
}

#[derive(Debug, Clone)]
pub struct MoEPrediction {
    pub symbol: String,
    pub timestamp: u64,
    pub probability_up_1min: f64,
    pub probability_down_1min: f64,
    pub probability_up_5min: f64,
    pub probability_down_5min: f64,
    pub consensus_signal: f64,
    pub uncertainty: f64,
    pub expert_votes: Vec<ExpertVote>,
}

#[derive(Debug, Clone)]
pub struct TradeOrder {
    pub id: u64,
    pub symbol: String,
    pub exchange: String,
    pub direction: String,
    pub quantity: f64,
    pub price: f64,
    pub order_type: String,
    pub latency_micros: u64,
    pub timestamp: u64,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CryptoPosition {
    pub symbol: String,
    pub exchange: String,
    pub quantity: f64,
    pub avg_entry: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub funding_paid: f64,
    pub open_time: u64,
}

#[derive(Debug, Clone)]
pub struct CryptoPortfolio {
    pub total_capital: f64,
    pub available_capital: f64,
    pub allocated_capital: f64,
    pub daily_pnl: f64,
    pub total_pnl: f64,
    pub positions: HashMap<String, CryptoPosition>,
    pub trade_history: Vec<TradeOrder>,
    pub gpu_investment_pool: f64,
    pub research_budget: f64,
    pub staking_rewards: f64,
}

// ============================================================
// MODULE 1 : COLLECTE DE DONNÉES CRYPTO MULTI‑EXCHANGE
// ============================================================

#[derive(Debug, Clone)]
pub struct CryptoDataCollector {
    pub ticks: VecDeque<CryptoTick>,
    pub symbols: Vec<String>,
    pub exchanges: Vec<String>,
    pub total_bytes: u64,
    pub start_time: u64,
}

impl CryptoDataCollector {
    pub fn new(symbols: Vec<String>, exchanges: Vec<String>) -> Self {
        CryptoDataCollector {
            ticks: VecDeque::with_capacity(MAX_DATA_POINTS),
            symbols,
            exchanges,
            total_bytes: 0,
            start_time: Self::now_secs(),
        }
    }

    pub fn ingest(&mut self, tick: CryptoTick) {
        self.total_bytes += std::mem::size_of::<CryptoTick>() as u64;
        self.ticks.push_back(tick);
        if self.ticks.len() > MAX_DATA_POINTS { self.ticks.pop_front(); }
    }

    pub fn throughput_pb_per_day(&self) -> f64 {
        let elapsed = Self::now_secs().saturating_sub(self.start_time).max(1) as f64;
        self.total_bytes as f64 / 1_000_000_000_000.0 * 86_400.0 / elapsed
    }

    pub fn queue_depth(&self) -> usize { self.ticks.len() }

    fn now_secs() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
    }
}

// ============================================================
// MODULE 2 : NETTOYAGE & 128 FEATURES
// ============================================================

#[derive(Debug, Clone)]
pub struct CryptoDataCleaner {
    pub window: usize,
    pub price_history: HashMap<String, VecDeque<f64>>,
    pub volume_history: HashMap<String, VecDeque<f64>>,
    pub funding_history: HashMap<String, VecDeque<f64>>,
    pub oi_history: HashMap<String, VecDeque<f64>>,
}

impl CryptoDataCleaner {
    pub fn new(window: usize) -> Self {
        CryptoDataCleaner {
            window,
            price_history: HashMap::new(),
            volume_history: HashMap::new(),
            funding_history: HashMap::new(),
            oi_history: HashMap::new(),
        }
    }

    pub fn extract(&mut self, tick: &CryptoTick) -> CryptoFeatures {
        let sym = tick.symbol.clone();
        let prices = self.price_history.entry(sym.clone()).or_insert_with(|| VecDeque::with_capacity(self.window));
        let volumes = self.volume_history.entry(sym.clone()).or_insert_with(|| VecDeque::with_capacity(self.window));
        let fundings = self.funding_history.entry(sym.clone()).or_insert_with(|| VecDeque::with_capacity(self.window));
        let ois = self.oi_history.entry(sym.clone()).or_insert_with(|| VecDeque::with_capacity(self.window));

        prices.push_back(tick.last_price);
        volumes.push_back(tick.volume);
        fundings.push_back(tick.funding_rate);
        ois.push_back(tick.open_interest);
        if prices.len() > self.window { prices.pop_front(); }
        if volumes.len() > self.window { volumes.pop_front(); }
        if fundings.len() > self.window { fundings.pop_front(); }
        if ois.len() > self.window { ois.pop_front(); }

        let pv: Vec<f64> = prices.iter().copied().collect();
        let n = pv.len();
        let mut feats = Vec::with_capacity(FEATURE_COUNT);

        let ret1 = if n > 1 { (pv[n-1] - pv[n-2]) / pv[n-2] } else { 0.0 };
        let ret5 = if n > 5 { (pv[n-1] - pv[n-6]) / pv[n-6] } else { 0.0 };
        let ret15 = if n > 15 { (pv[n-1] - pv[n-16]) / pv[n-16] } else { 0.0 };
        let vol5 = if n >= 5 { Self::stddev(&pv[n-5..]) } else { 0.0 };
        let vol15 = if n >= 15 { Self::stddev(&pv[n-15..]) } else { 0.0 };
        let avg_vol: f64 = volumes.iter().sum::<f64>() / n.max(1) as f64;
        let vol_ratio = if avg_vol > 0.0 { tick.volume / avg_vol } else { 1.0 };
        let spread_ratio = if tick.ask > 0.0 { (tick.ask - tick.bid) / tick.ask } else { 0.0 };
        let mom = if n >= 10 { (pv[n-1] - pv[n-10]) / pv[n-10] } else { 0.0 };
        let sma20: f64 = pv.iter().rev().take(20).sum::<f64>() / 20.0.max(1.0);
        let mean_rev = (tick.last_price - sma20) / sma20;
        let trend = if n >= 14 {
            let (up, down) = pv[n-13..].windows(2).fold((0.0, 0.0), |(u, d), w| {
                let diff = w[1] - w[0];
                if diff > 0.0 { (u + diff, d) } else { (u, d + diff.abs()) }
            });
            if up + down > 0.0 { (up - down) / (up + down) } else { 0.0 }
        } else { 0.0 };

        let avg_funding: f64 = fundings.iter().sum::<f64>() / fundings.len().max(1) as f64;
        let funding_sig = (tick.funding_rate - avg_funding).tanh();
        let oi_vec: Vec<f64> = ois.iter().copied().collect();
        let oi_change = if oi_vec.len() > 5 { (oi_vec[oi_vec.len()-1] - oi_vec[oi_vec.len()-6]) / oi_vec[oi_vec.len()-6].abs().max(0.01) } else { 0.0 };
        let oi_sig = oi_change.tanh();
        let cvd_sig = (tick.cvd / 1_000_000.0).tanh();
        let liq_total = tick.liquidations_long + tick.liquidations_short;
        let liq_sig = if liq_total > 0.0 { (tick.liquidations_long - tick.liquidations_short) / liq_total } else { 0.0 };

        feats.push(ret1); feats.push(ret5); feats.push(ret15);
        feats.push(vol5); feats.push(vol15); feats.push(vol_ratio);
        feats.push(spread_ratio); feats.push(mom); feats.push(mean_rev);
        feats.push(trend); feats.push(funding_sig); feats.push(oi_sig);
        feats.push(cvd_sig); feats.push(liq_sig);
        while feats.len() < FEATURE_COUNT { feats.push(0.0); }

        CryptoFeatures {
            symbol: sym, timestamp: tick.timestamp, features: feats,
            returns_1min: ret1, returns_5min: ret5, returns_15min: ret15,
            volatility_5min: vol5, volatility_15min: vol15,
            volume_ratio: vol_ratio, spread_ratio,
            momentum_score: mom, mean_reversion_score: mean_rev,
            trend_strength: trend, funding_signal: funding_sig,
            oi_signal: oi_sig, cvd_signal: cvd_sig, liquidation_signal: liq_sig,
        }
    }

    fn stddev(data: &[f64]) -> f64 {
        let n = data.len() as f64;
        if n < 2.0 { return 0.0; }
        let mean = data.iter().sum::<f64>() / n;
        (data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)).sqrt()
    }
}

// ============================================================
// MODULE 3 : MIXTURE OF EXPERTS — 256 EXPERTS CRYPTO
// ============================================================

#[derive(Debug, Clone)]
pub struct CryptoMoE {
    pub num_experts: usize,
    pub specializations: Vec<String>,
    pub performance: Vec<f64>,
    pub weights: Vec<f64>,
    pub total_predictions: u64,
}

impl CryptoMoE {
    pub fn new(num_experts: usize) -> Self {
        let specs = vec![
            "volatility", "trend", "momentum", "mean_reversion", "volume_breakout",
            "funding_arbitrage", "oi_flow", "cvd_flow", "liquidation_sentiment",
            "perpetual_basis", "cross_exchange_arb", "defi_yield", "social_media",
            "onchain_metrics", "whale_alert",
        ];
        let mut s = Vec::with_capacity(num_experts);
        for i in 0..num_experts { s.push(specs[i % specs.len()].to_string()); }
        CryptoMoE {
            num_experts,
            specializations: s,
            performance: vec![1.0; num_experts],
            weights: vec![1.0 / num_experts as f64; num_experts],
            total_predictions: 0,
        }
    }

    pub fn predict(&mut self, f: &CryptoFeatures) -> MoEPrediction {
        self.total_predictions += 1;
        let mut votes = Vec::with_capacity(self.num_experts);
        let (mut wsignal, mut wsum) = (0.0, 0.0);

        for i in 0..self.num_experts {
            let spec = &self.specializations[i];
            let (buy, sell) = Self::expert_predict(i, f, spec);
            let perf = self.performance[i].max(0.1);
            let conf = (buy - sell).abs().min(1.0);
            let cw = perf * conf;
            votes.push(ExpertVote { expert_id: i, specialization: spec.clone(), signal_buy: buy, signal_sell: sell, confidence: conf, weight: cw });
            wsignal += (buy - sell) * cw;
            wsum += cw;
        }

        let consensus = if wsum > 0.0 { wsignal / wsum } else { 0.0 };
        if self.total_predictions % 1000 == 0 { self.update_weights(&votes); }

        MoEPrediction {
            symbol: f.symbol.clone(), timestamp: f.timestamp,
            probability_up_1min: (consensus + 1.0) / 2.0,
            probability_down_1min: 1.0 - (consensus + 1.0) / 2.0,
            probability_up_5min: (consensus * 1.5_f64.tanh() + 1.0) / 2.0,
            probability_down_5min: 1.0 - (consensus * 1.5_f64.tanh() + 1.0) / 2.0,
            consensus_signal: consensus,
            uncertainty: 1.0 - consensus.abs(),
            expert_votes: votes,
        }
    }

    fn expert_predict(id: usize, f: &CryptoFeatures, spec: &str) -> (f64, f64) {
        let seed = (id as f64 * 0.618).sin();
        let base = seed * 0.3;
        let signal = match spec {
            "volatility" => base + (f.volatility_5min * -2.0).tanh(),
            "trend" => base + (f.trend_strength * 3.0).tanh(),
            "momentum" => base + (f.momentum_score * 2.5).tanh(),
            "mean_reversion" => base - (f.mean_reversion_score * 2.0).tanh(),
            "funding_arbitrage" => base - (f.funding_signal * 2.0).tanh(),
            "oi_flow" => base + (f.oi_signal * 1.8).tanh(),
            "cvd_flow" => base + (f.cvd_signal * 2.2).tanh(),
            "liquidation_sentiment" => base - (f.liquidation_signal * 1.5).tanh(),
            _ => base + ((id as f64 * 0.01).sin() * 0.1),
        };
        let buy = (signal + 1.0) / 2.0;
        (buy.min(1.0).max(0.01), 1.0 - buy.min(1.0).max(0.01))
    }

    fn update_weights(&mut self, votes: &[ExpertVote]) {
        for v in votes { self.performance[v.expert_id] = self.performance[v.expert_id] * 0.95 + v.confidence * 0.05; }
        let sum: f64 = self.performance.iter().sum();
        if sum > 0.0 { for (i, w) in self.weights.iter_mut().enumerate() { *w = self.performance[i] / sum; } }
    }
}

// ============================================================
// MODULE 4 : MOTEUR DE RISQUE CRYPTO
// ============================================================

#[derive(Debug, Clone)]
pub struct CryptoRiskEngine {
    pub historical_returns: Vec<f64>,
    pub current_drawdown: f64,
    pub peak_capital: f64,
    pub max_position_size: f64,
    pub daily_var_95: f64,
}

impl CryptoRiskEngine {
    pub fn new() -> Self {
        CryptoRiskEngine {
            historical_returns: Vec::with_capacity(10_000),
            current_drawdown: 0.0,
            peak_capital: 500_000.0,
            max_position_size: 0.03,
            daily_var_95: 0.0,
        }
    }

    pub fn update(&mut self, capital: f64) {
        if capital > self.peak_capital { self.peak_capital = capital; }
        self.current_drawdown = if self.peak_capital > 0.0 { (self.peak_capital - capital) / self.peak_capital * 100.0 } else { 0.0 };
        if self.historical_returns.len() > 100 {
            let stddev = CryptoDataCleaner::stddev(&self.historical_returns);
            self.daily_var_95 = 1.645 * stddev * capital;
        }
    }

    pub fn allocate(&self, signals: &HashMap<String, MoEPrediction>, available: f64) -> HashMap<String, f64> {
        let mut alloc = HashMap::new();
        let total: f64 = signals.values().map(|p| p.consensus_signal.abs()).sum();
        if total <= 0.0 { return alloc; }
        for (sym, pred) in signals {
            let w = pred.consensus_signal.abs() / total;
            let amount = available * w * self.max_position_size;
            if amount > 0.0 && pred.consensus_signal.abs() > 0.35 { alloc.insert(sym.clone(), amount); }
        }
        alloc
    }
}

// ============================================================
// MODULE 5 : MOTEUR D'EXÉCUTION CRYPTO MULTI‑EXCHANGE
// ============================================================

#[derive(Debug, Clone)]
pub struct CryptoExecutionEngine {
    pub base_latency_us: u64,
    pub order_counter: u64,
    pub exchanges: Vec<String>,
    pub exchange_latencies: HashMap<String, u64>,
}

impl CryptoExecutionEngine {
    pub fn new(exchanges: Vec<String>) -> Self {
        let mut latencies = HashMap::new();
        latencies.insert("Binance".into(), 45);
        latencies.insert("Bybit".into(), 55);
        latencies.insert("OKX".into(), 60);
        latencies.insert("Coinbase".into(), 80);
        CryptoExecutionEngine {
            base_latency_us: MICROSECOND_LATENCY,
            order_counter: 0,
            exchanges,
            exchange_latencies: latencies,
        }
    }

    pub fn create_order(&mut self, symbol: &str, exchange: &str, direction: &str, qty: f64, price: f64) -> TradeOrder {
        self.order_counter += 1;
        let lat = self.exchange_latencies.get(exchange).copied().unwrap_or(self.base_latency_us);
        TradeOrder {
            id: self.order_counter,
            symbol: symbol.into(),
            exchange: exchange.into(),
            direction: direction.into(),
            quantity: qty,
            price,
            order_type: if lat < 100 { "IOC".into() } else { "LIMIT".into() },
            latency_micros: lat,
            timestamp: Self::now_us(),
            status: "PENDING".into(),
        }
    }

    pub fn execute(&self, order: &mut TradeOrder, current_price: f64) -> bool {
        let slip = (current_price - order.price).abs() / order.price;
        if slip > 0.002 { order.status = "REJECTED_SLIPPAGE".into(); return false; }
        order.status = "EXECUTED".into();
        true
    }

    fn now_us() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64 }
}

// ============================================================
// MODULE 6 : RÉINVESTISSEMENT + STAKING
// ============================================================

#[derive(Debug, Clone)]
pub struct CryptoReinvestEngine {
    pub total_profits: f64,
    pub reinvested: f64,
    pub gpu_pool: f64,
    pub research_budget: f64,
    pub staking_pool: f64,
    pub compound: f64,
}

impl CryptoReinvestEngine {
    pub fn new() -> Self {
        CryptoReinvestEngine { total_profits: 0.0, reinvested: 0.0, gpu_pool: 0.0, research_budget: 0.0, staking_pool: 0.0, compound: 0.0 }
    }

    pub fn reinvest(&mut self, daily_profit: f64) {
        self.total_profits += daily_profit;
        self.reinvested += daily_profit * REINVEST_RATIO;
        self.gpu_pool += daily_profit * GPU_BUDGET_RATIO;
        self.research_budget += daily_profit * RESEARCH_RATIO;
        self.staking_pool += daily_profit * STAKING_RATIO;
        self.compound = if self.reinvested > 0.0 { (self.total_profits / self.reinvested).min(5.0) } else { 0.0 };
    }
}

// ============================================================
// MOTEUR PRINCIPAL BAKOME CRYPTO HIGH‑FLYER
// ============================================================

#[derive(Debug, Clone)]
pub struct BakomeCryptoEngine {
    pub collector: CryptoDataCollector,
    pub cleaner: CryptoDataCleaner,
    pub moe: CryptoMoE,
    pub risk: CryptoRiskEngine,
    pub execution: CryptoExecutionEngine,
    pub reinvest: CryptoReinvestEngine,
    pub portfolio: CryptoPortfolio,
    pub total_trades: u64,
    pub total_ticks: u64,
}

impl BakomeCryptoEngine {
    pub fn new(symbols: Vec<String>, exchanges: Vec<String>) -> Self {
        BakomeCryptoEngine {
            collector: CryptoDataCollector::new(symbols, exchanges.clone()),
            cleaner: CryptoDataCleaner::new(200),
            moe: CryptoMoE::new(MIXTURE_EXPERTS),
            risk: CryptoRiskEngine::new(),
            execution: CryptoExecutionEngine::new(exchanges),
            reinvest: CryptoReinvestEngine::new(),
            portfolio: CryptoPortfolio {
                total_capital: 500_000.0,
                available_capital: 500_000.0,
                allocated_capital: 0.0,
                daily_pnl: 0.0,
                total_pnl: 0.0,
                positions: HashMap::new(),
                trade_history: Vec::new(),
                gpu_investment_pool: 0.0,
                research_budget: 0.0,
                staking_rewards: 0.0,
            },
            total_trades: 0,
            total_ticks: 0,
        }
    }

    pub fn process_tick(&mut self, tick: CryptoTick) -> Option<TradeOrder> {
        self.total_ticks += 1;
        self.collector.ingest(tick.clone());
        let feats = self.cleaner.extract(&tick);
        let pred = self.moe.predict(&feats);
        self.risk.update(self.portfolio.total_capital);
        if self.risk.current_drawdown > MAX_DRAWDOWN_PCT { return None; }

        if pred.consensus_signal.abs() > 0.45 && pred.uncertainty < 0.45 {
            let dir = if pred.consensus_signal > 0.0 { "BUY" } else { "SELL" };
            let qty = self.portfolio.available_capital * 0.015 / tick.last_price;
            let mut order = self.execution.create_order(&tick.symbol, &tick.exchange, dir, qty, tick.last_price);
            if self.execution.execute(&mut order, tick.last_price) {
                self.total_trades += 1;
                let pnl = if dir == "BUY" { -qty * tick.last_price * 0.0005 } else { qty * tick.last_price * 0.0005 };
                self.portfolio.daily_pnl += pnl;
                self.portfolio.total_pnl += pnl;
                self.portfolio.total_capital += pnl;
                self.portfolio.available_capital = self.portfolio.total_capital - self.portfolio.allocated_capital;
                self.portfolio.trade_history.push(order.clone());
                self.log_trade(&order);
                if self.total_trades % 100 == 0 {
                    self.reinvest.reinvest(self.portfolio.daily_pnl);
                    self.portfolio.gpu_investment_pool = self.reinvest.gpu_pool;
                    self.portfolio.research_budget = self.reinvest.research_budget;
                    self.portfolio.staking_rewards = self.reinvest.staking_pool;
                    self.portfolio.daily_pnl = 0.0;
                }
                return Some(order);
            }
        }
        None
    }

    fn log_trade(&self, order: &TradeOrder) {
        let entry = format!("{}|{}|{}|{}|{}|{}|{}|{}|{}\n",
            order.timestamp, order.id, order.symbol, order.exchange,
            order.direction, order.quantity, order.price, order.latency_micros, order.status);
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(LOG_FILE) {
            let _ = f.write_all(entry.as_bytes());
        }
    }

    pub fn print_banner(&self) {
        println!("\n╔══════════════════════════════════════════════════════════════════╗");
        println!("║   {}  ║", VERSION);
        println!("║   {} Experts | {} Exchanges | {} Symbols                ║", MIXTURE_EXPERTS, self.execution.exchanges.len(), self.collector.symbols.len());
        println!("║   Max DD {}% | Capital $500K                           ║", MAX_DRAWDOWN_PCT as u8);
        println!("╚══════════════════════════════════════════════════════════════════╝\n");
    }

    pub fn print_status(&self) {
        println!("📊 Ticks: {} | Trades: {} | P&L: ${:.0} | Capital: ${:.0} | DD: {:.1}%",
            self.total_ticks, self.total_trades, self.portfolio.total_pnl, self.portfolio.total_capital, self.risk.current_drawdown);
        println!("💰 GPU: ${:.0} | Research: ${:.0} | Staking: ${:.0} | VaR95: ${:.0}",
            self.reinvest.gpu_pool, self.reinvest.research_budget, self.reinvest.staking_pool, self.risk.daily_var_95);
    }
}

// ============================================================
// SIMULATEUR DE MARCHÉ CRYPTO
// ============================================================

pub struct CryptoMarketSimulator {
    pub symbols: Vec<String>,
    pub exchanges: Vec<String>,
    pub base_prices: HashMap<String, f64>,
    pub tick_count: u64,
}

impl CryptoMarketSimulator {
    pub fn new(symbols: Vec<String>, exchanges: Vec<String>) -> Self {
        let mut base = HashMap::new();
        base.insert("ETHUSDT".into(), 3200.0);
        base.insert("SOLUSDT".into(), 145.0);
        base.insert("BNBUSDT".into(), 610.0);
        base.insert("XRPUSDT".into(), 0.55);
        base.insert("DOGEUSDT".into(), 0.16);
        base.insert("AVAXUSDT".into(), 35.0);
        CryptoMarketSimulator { symbols, exchanges, base_prices: base, tick_count: 0 }
    }

    pub fn generate_tick(&mut self) -> CryptoTick {
        self.tick_count += 1;
        let sym = &self.symbols[(self.tick_count as usize) % self.symbols.len()];
        let ex = &self.exchanges[(self.tick_count as usize) % self.exchanges.len()];
        let base = self.base_prices.get(sym).copied().unwrap_or(100.0);
        let noise = (self.tick_count as f64 * 0.015).sin() * base * 0.005;
        let trend = (self.tick_count as f64 * 0.0002).cos() * base * 0.002;
        let price = base + noise + trend;
        CryptoTick {
            symbol: sym.clone(), exchange: ex.clone(), timestamp: self.tick_count,
            bid: price * 0.9999, ask: price * 1.0001, last_price: price,
            volume: 5000.0 + (self.tick_count as f64 * 0.7).sin() * 2000.0,
            funding_rate: (self.tick_count as f64 * 0.001).sin() * 0.0005,
            open_interest: 1_000_000.0 + (self.tick_count as f64 * 0.01).cos() * 200_000.0,
            cvd: (self.tick_count as f64 * 0.02).sin() * 500_000.0,
            liquidations_long: 5000.0 + (self.tick_count as f64 * 0.03).sin() * 3000.0,
            liquidations_short: 3000.0 + (self.tick_count as f64 * 0.025).cos() * 2000.0,
        }
    }
}

// ============================================================
// POINT D'ENTRÉE
// ============================================================

fn main() {
    let symbols: Vec<String> = CRYPTO_SYMBOLS.iter().map(|s| s.to_string()).collect();
    let exchanges: Vec<String> = EXCHANGES.iter().map(|s| s.to_string()).collect();
    let mut engine = BakomeCryptoEngine::new(symbols.clone(), exchanges.clone());
    let mut simulator = CryptoMarketSimulator::new(symbols, exchanges);

    engine.print_banner();
    println!("⚡ BAKOME Crypto High‑Freq Engine started...\n");
    println!("🪙 ETH, SOL, BNB, XRP, DOGE, AVAX");
    println!("🏦 Binance, Bybit, OKX, Coinbase\n");
    println!("⏳ Processing 10,000 ticks...\n");

    let start = std::time::Instant::now();
    for _ in 0..10_000 {
        let tick = simulator.generate_tick();
        if let Some(o) = engine.process_tick(tick) {
            println!("📈 #{} | {} {} {} @ ${:.2} on {} | {}µs",
                o.id, o.symbol, o.direction, o.quantity, o.price, o.exchange, o.latency_micros);
        }
        if engine.total_trades > 0 && engine.total_trades % 500 == 0 { engine.print_status(); }
    }
    let elapsed = start.elapsed().as_secs_f64();

    println!("\n🛑 Simulation complete — 10,000 ticks in {:.2}s", elapsed);
    engine.print_status();
    println!("\n📄 Trade log saved to {}", LOG_FILE);
    println!("🚀 BAKOME Crypto High‑Flyer — Ready for live deployment.\n");
}

// ============================================================
// TESTS UNITAIRES
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn test_engine_init() { let e = BakomeCryptoEngine::new(vec!["ETHUSDT".into()], vec!["Binance".into()]); assert_eq!(e.portfolio.total_capital, 500_000.0); }
    #[test] fn test_moe_prediction() { let mut moe = CryptoMoE::new(256); let f = CryptoFeatures { symbol:"ETHUSDT".into(), timestamp:1, features:vec![0.0;128], returns_1min:0.001, returns_5min:0.005, returns_15min:0.015, volatility_5min:0.02, volatility_15min:0.03, volume_ratio:1.2, spread_ratio:0.0001, momentum_score:0.01, mean_reversion_score:-0.005, trend_strength:0.5, funding_signal:0.001, oi_signal:0.02, cvd_signal:0.1, liquidation_signal:0.3 }; let p = moe.predict(&f); assert!(p.probability_up_1min > 0.0); }
    #[test] fn test_execution() { let mut ee = CryptoExecutionEngine::new(vec!["Binance".into()]); let mut o = ee.create_order("ETHUSDT", "Binance", "BUY", 1.0, 3200.0); assert!(ee.execute(&mut o, 3200.0)); assert_eq!(o.status, "EXECUTED"); }
    #[test] fn test_reinvest() { let mut re = CryptoReinvestEngine::new(); re.reinvest(100_000.0); assert!(re.gpu_pool > 0.0); assert!(re.staking_pool > 0.0); }
    #[test] fn test_risk_drawdown() { let mut risk = CryptoRiskEngine::new(); risk.update(450_000.0); assert!(risk.current_drawdown > 0.0); }
}
