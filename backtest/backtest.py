# %%
# =============================================================================
# SPOT TRADING BACKTEST
# =============================================================================
import os, sys
import pandas as pd
import numpy as np
from binance.client import Client
from datetime import timedelta, datetime, timezone
import warnings
import plotly.graph_objects as go
from plotly.subplots import make_subplots
warnings.filterwarnings("ignore")

try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
if not SCRIPT_DIR or SCRIPT_DIR == os.sep:
    SCRIPT_DIR = os.getcwd()
print("Output dir:", SCRIPT_DIR)
for sub in ("ohlcv", "equity", "trades"):
    os.makedirs(os.path.join(SCRIPT_DIR, sub), exist_ok=True)

# %%
# =============================================================================
# SIGNAL PARAMETERS
# =============================================================================
btc_ema_short_seed        = 
btc_ema_long_seed         = 
lookback_mom_seed         = 
vol_lookback_seed         = 
recent_high_lookback_seed = 
cooldown_days_seed        = 

btc_ema_short        = btc_ema_short_seed        
btc_ema_long         = btc_ema_long_seed         
lookback_mom         = lookback_mom_seed        
vol_lookback         = vol_lookback_seed      
recent_high_lookback = recent_high_lookback_seed
cooldown_days        = cooldown_days_seed

warmup_days = max(btc_ema_short, btc_ema_long, vol_lookback, lookback_mom,
                  recent_high_lookback, cooldown_days)
print("Warm-up window:", warmup_days, "days")

# =============================================================================
# STRATEGY PARAMETERS
# =============================================================================
FEE_RATE   = 0.001    # 0.1 % Binance spot taker fee

EXPOSURE_BULL              = 
TOP_N                      = 
MAX_DD_FREEZE_BTC          = 
MAX_DD_FREEZE_COIN         = 

# =============================================================================
# UNIVERSE / DATES
# =============================================================================
TOTAL_CAPITAL      = 1000
CANDIDATE_UNIVERSE = [
    "BTCUSDT", 
    "ETHUSDT",  
    "XRPUSDT",
    "SOLUSDT", 
    "ADAUSDT",  
    "DOGEUSDT", 
    "HBARUSDT",
    "TRXUSDT", 
    "LINKUSDT", 
    "XLMUSDT",
    "VIRTUALUSDT", 
    "SUIUSDT", 
    "HUMAUSDT",
    "PEPEUSDT",
]
TOP_N = len(CANDIDATE_UNIVERSE)

today          = datetime.now(timezone.utc).date()
start_trade_dt = datetime.strptime("2022-01-01", "%Y-%m-%d").date()
end_trade_dt = datetime.strptime("2024-01-01", "%Y-%m-%d").date()
START_DATE     = (start_trade_dt + timedelta(days=-warmup_days)).strftime("%Y-%m-%d")
END_DATE       = today.strftime("%Y-%m-%d")
# END_DATE       = end_trade_dt.strftime("%Y-%m-%d")
print(START_DATE, "->", END_DATE)


# %%
# =============================================================================
# 1. FETCH OHLCV
# =============================================================================
def fetch_all_data(coins, start_date, end_date):
    client   = Client()
    all_data = {}
    for coin in coins:
        try:
            klines = client.get_historical_klines(
                coin, Client.KLINE_INTERVAL_1DAY,
                start_str=start_date, end_str=end_date)
            if not klines:
                print(f"  Skipping {coin} — no data")
                continue
            df = pd.DataFrame(klines, columns=[
                "Open time","Open","High","Low","Close","Volume",
                "Close time","Quote asset volume","Number of trades",
                "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
            ])[["Open time","Open","High","Low","Close","Volume"]]
            df.columns = ["date","open","high","low","close","volume"]
            df["date"] = pd.to_datetime(df["date"], unit="ms")
            df[["open","high","low","close","volume"]] = \
                df[["open","high","low","close","volume"]].astype(float)
            all_data[coin] = df
            print(f"  {coin}: {len(df)} days")
        except Exception as e:
            print(f"  Failed {coin}: {e}")
    return all_data


# =============================================================================
# 2. DAILY TOP-N UNIVERSE
# Uses same-day close to rank (matching original logic exactly).
# =============================================================================
def get_daily_topN(all_data, top_n=TOP_N):
    all_dates = sorted({d for df in all_data.values() for d in df["date"]})
    topN_dict = {}
    for d in all_dates:
        dt   = pd.to_datetime(d)
        caps = {c: df.loc[df["date"] == dt, "close"].iloc[0]
                for c, df in all_data.items() if dt in df["date"].values}
        topN_dict[dt] = sorted(caps, key=caps.get, reverse=True)[:top_n]
    return all_dates, topN_dict


# %%
# =============================================================================
# 3. SPOT BACKTEST ENGINE
# =============================================================================
#  Account model
#  ─────────────────────────────────────────────────────────────────────
#  cash              USDT not currently invested
#  portfolio         dict coin -> qty of base asset held
#  portfolio_equity  cash + Σ(qty_i × price_i)  (mark-to-market)
#
#  Pricing convention (matching original)
#  ─────────────────────────────────────────────────────────────────────
#  Signal/trade prices:  strict < current_date  (no look-ahead)
#  Equity snapshot:      <= current_date  (today's close for valuation)
#  ─────────────────────────────────────────────────────────────────────
def compute_signal_score(df):
    """
    Proprietary signal logic.
    Combines momentum and volatility
    """
    return score

def allocate_portfolio_weights(scores):
    """
    Portfolio allocation based on signal strength
    and diversification constraints.
    """
    return weights


def backtest_spot(all_data, daily_date, topN_dict,
                  total_capital        = TOTAL_CAPITAL,
                  btc_symbol           = "BTCUSDT",
                  btc_ema_short        = btc_ema_short,
                  btc_ema_long         = btc_ema_long,
                  exposure_bull        = EXPOSURE_BULL,
                  lookback_mom         = lookback_mom,
                  vol_lookback         = vol_lookback,
                  blend_equal_weight   = 0,
                  min_trade            = 5,
                  fee_rate             = FEE_RATE,
                  max_drawdown_before_freeze      = MAX_DD_FREEZE_BTC,
                  max_drawdown_before_freeze_coin = MAX_DD_FREEZE_COIN,
                  recent_high_lookback = recent_high_lookback,
                  warmup_days          = warmup_days,
                  tol                  = 0.001,
                  adjust_param         = True,
                  detailed             = True):

    # ── State ─────────────────────────────────────────────────────────────────
    portfolio = {coin: 0.0 for coin in all_data}
    cash      = float(total_capital)
    equity_history, trades = [], []

    # BTC EMA is computed but regime routing is disabled (exposure always = exposure_bull)
    btc_df = all_data[btc_symbol].copy().sort_values("date").reset_index(drop=True)
    # (EMA columns left uncomputed — regime gate is commented out in original)

    buy_frozen_until = None
    start_date       = daily_date[0] + pd.Timedelta(days=warmup_days)

    # ── Main loop ─────────────────────────────────────────────────────────────
    for current_date in daily_date:
        top_buys        = []
        current_date_dt = pd.to_datetime(current_date)

        # ── Adaptive params ───────────────────────────────────────────────────
        if adjust_param:
            if current_date_dt.year % 4 == 2:
                _lm  = 
                _vl  = 
                _rhl = 
            else:
                _lm  = lookback_mom_seed  
                _vl  = vol_lookback_seed
                _rhl = recent_high_lookback_seed
            _lm  = _lm 
            _vl  = _vl
            _rhl = _rhl
            warmup_days = max(btc_ema_short, btc_ema_long, _vl, _lm, _rhl, cooldown_days)
        else:
            _lm  = lookback_mom
            _vl  = vol_lookback
            _rhl = recent_high_lookback

        # regime and exposure (EMA gate commented out — always exposure_bull)
        regime   = ""
        exposure = exposure_bull

        # ── BTC drawdown / buy-freeze guard ───────────────────────────────────
        btc_prices_before = btc_df.loc[btc_df["date"] < current_date_dt, "close"]
        btc_drawdown      = 0.0
        if len(btc_prices_before):
            recent_high    = float(btc_prices_before.tail(_rhl).max())
            curr_btc_price = float(btc_prices_before.iloc[-1])
            if recent_high > 0:
                btc_drawdown = 1.0 - (curr_btc_price / recent_high)
        global_buy_freeze = btc_drawdown > max_drawdown_before_freeze

        buy_frozen_until = None
        if global_buy_freeze:
            buy_frozen_until = "FROZEN"

        # ── WARMUP: record snapshot, no trading ───────────────────────────────
        if current_date_dt < start_date:
            total_equity = cash
            row = {"date": current_date_dt, "cash": cash, "regime": regime,
                   "exposure": exposure, "note": "WARMUP"}
            for c in portfolio:
                price_rows = all_data[c].loc[all_data[c]["date"] <= current_date_dt, "close"]
                price      = float(price_rows.iloc[-1]) if len(price_rows) else 0.0
                equity_c   = portfolio[c] * price
                total_equity += equity_c
                if detailed:
                    row[f"{c}_qty"]    = portfolio[c]
                    row[f"{c}_equity"] = equity_c
            row["portfolio_equity"] = total_equity
            equity_history.append(row)
            continue

        # ── Candidates ────────────────────────────────────────────────────────
        candidates = topN_dict.get(current_date_dt, [])
        if not candidates:
            total_equity = cash
            row = {"date": current_date_dt, "cash": cash, "regime": regime,
                   "exposure": exposure}
            for c in portfolio:
                price_rows = all_data[c].loc[all_data[c]["date"] <= current_date_dt, "close"]
                price      = float(price_rows.iloc[-1]) if len(price_rows) else 0.0
                equity_c   = portfolio[c] * price
                total_equity += equity_c
                if detailed:
                    row[f"{c}_qty"]    = portfolio[c]
                    row[f"{c}_equity"] = equity_c
            row["portfolio_equity"] = total_equity
            equity_history.append(row)
            continue

        # ── Momentum-volatility scores ────────────────────────────────────────
        scores = {}
        for coin in candidates:
            df_local = all_data[coin][all_data[coin]["date"] < current_date_dt] \
                           .reset_index(drop=True).tail(warmup_days)
            if len(df_local) < warmup_days:
                scores[coin] = 0.0; continue
            scores[coin] = compute_signal_score(df_local)
        
        weights = allocate_portfolio_weights(scores)

        # ── Current equity & targets ──────────────────────────────────────────
        total_equity_now = cash
        for coin in portfolio:
            price_rows = all_data[coin].loc[all_data[coin]["date"] < current_date_dt, "close"]
            total_equity_now += portfolio[coin] * (float(price_rows.iloc[-1]) if len(price_rows) else 0.0)

        target_invest   = exposure * total_equity_now
        target_per_coin = {coin: target_invest * w for coin, w in zip(candidates, weights)}

        # ── REBALANCE: sell / reduce ──────────────────────────────────────────
        for coin in candidates:
            price_rows = all_data[coin].loc[all_data[coin]["date"] < current_date_dt, "close"]
            if len(price_rows) == 0:
                continue
            price = float(price_rows.iloc[-1])
            qty   = portfolio.get(coin, 0.0)
            value = qty * price
            delta = target_per_coin[coin] - value

            if abs(delta) < min_trade:
                continue

            # SELL: always allowed
            if delta < 0:
                sell_value  = min(-delta, value)
                if sell_value < min_trade:
                    continue
                price_sell = price * (1 - tol)
                qty_sell   = min(qty, sell_value / price_sell)
                cash_before = cash
                cash += qty_sell * price_sell * (1 - fee_rate)
                portfolio[coin] = qty - qty_sell
                trades.append({
                    "date": current_date_dt, "coin": coin, "type": "SELL",
                    "price": price_sell, "quantity_trade": qty_sell,
                    "quantity_before": qty, "quantity_after": portfolio[coin],
                    "cash_before_trade": cash_before, "cash_after_trade": cash,
                })
                continue

            # BUY: check freeze
            if buy_frozen_until is not None:
                continue

            # Per-coin drawdown guard
            coin_prices_before = all_data[coin].loc[
                all_data[coin]["date"] < current_date_dt, "close"]
            coin_recent_high = (float(coin_prices_before.tail(_rhl).max())
                                if len(coin_prices_before) > 0 else price)
            coin_drawdown = (1.0 - price / coin_recent_high
                             if coin_recent_high > 0 else 0.0)
            if coin_drawdown > max_drawdown_before_freeze_coin:
                continue

            spend = delta
            if spend < min_trade or spend < 1e-6:
                continue
            price_buy = price * (1 + tol)
            top_buys.append((coin, qty, price_buy, spend))

        # ── Sort buys by spend descending, then execute ───────────────────────
        top_buys.sort(key=lambda x: x[3], reverse=True)

        for coin, qty, price_buy, spend in top_buys:
            spend = min(spend, cash)
            if spend < min_trade or spend < 1e-6:
                continue
            qty_buy     = spend / price_buy * (1 - fee_rate)
            cash_before = cash
            cash       -= spend
            portfolio[coin] = qty + qty_buy
            trades.append({
                "date": current_date_dt, "coin": coin, "type": "BUY",
                "price": price_buy, "quantity_trade": qty_buy,
                "quantity_before": qty, "quantity_after": portfolio[coin],
                "cash_before_trade": cash_before, "cash_after_trade": cash,
            })

        # ── Equity snapshot ───────────────────────────────────────────────────
        # Uses <= current_date (today's close) for valuation — matches original
        total_equity = cash
        row = {"date": current_date_dt, "cash": cash, "regime": regime,
               "exposure": exposure, "btc_drawdown": btc_drawdown,
               "buy_frozen": bool(buy_frozen_until)}
        for c in portfolio:
            price_rows = all_data[c].loc[all_data[c]["date"] <= current_date_dt, "close"]
            price      = float(price_rows.iloc[-1]) if len(price_rows) else 0.0
            equity_c   = portfolio[c] * price
            total_equity += equity_c
            if detailed:
                row[f"{c}_qty"]    = portfolio[c]
                row[f"{c}_equity"] = equity_c
        row["portfolio_equity"] = total_equity
        equity_history.append(row)

    print("Last date:", current_date_dt.date())
    equity_df  = pd.DataFrame(equity_history)
    trades_df  = pd.DataFrame(trades)
    if not trades_df.empty:
        trades_df["cash_traded"] = trades_df["cash_after_trade"] - trades_df["cash_before_trade"]
    return equity_df, trades_df


# %%
# =============================================================================
# 4. SPOT HOLD BENCHMARK
# =============================================================================
def generate_hold_df(all_data, window_size=warmup_days):
    hold_df = pd.DataFrame({"date": sorted({d for df in all_data.values() for d in df["date"]})})
    for coin in CANDIDATE_UNIVERSE:
        if coin not in all_data:
            continue
        df = all_data[coin].sort_values("date").reset_index(drop=True)
        eq_list, shares = [], None
        for i, d in enumerate(hold_df["date"]):
            past  = df.loc[df["date"] <= d, "close"]
            price = past.iloc[-1] if len(past) else np.nan
            if shares is None and i >= window_size and not np.isnan(price):
                shares = TOTAL_CAPITAL / price
            eq_list.append(TOTAL_CAPITAL if (shares is None or np.isnan(price)) else shares * price)
        hold_df[coin + "_hold"] = eq_list
    hold_df["average_hold"] = hold_df.iloc[:, 1:].mean(axis=1)
    return hold_df


# %%
# =============================================================================
# 5. RUN PIPELINE
# =============================================================================
print("\n-- Fetching OHLCV --")
all_data = fetch_all_data(CANDIDATE_UNIVERSE, START_DATE, END_DATE)

print("\n-- Building date universe --")
daily_date, topN_dict = get_daily_topN(all_data)

print("\n-- Running backtest --")
portfolio_df, trades_df = backtest_spot(all_data, daily_date, topN_dict)

print("\n-- Building hold benchmark --")
hold_df = generate_hold_df(all_data)

def get_holdings_str(row):
    held = [c for c in CANDIDATE_UNIVERSE
            if f"{c}_qty" in row and row[f"{c}_qty"] > 0]
    return ", ".join(held) if held else "None"

portfolio_df["holdings_str"] = portfolio_df.apply(get_holdings_str, axis=1)

portfolio_df.to_csv(os.path.join(SCRIPT_DIR, "portfolio_df_spot.csv"), index=False)
trades_df.to_csv(   os.path.join(SCRIPT_DIR, "trades_spot.csv"),       index=False)
print(f"CSVs saved to {SCRIPT_DIR}")

# ── Print summary ─────────────────────────────────────────────────────────────
active = portfolio_df[portfolio_df.get("note", pd.Series("", index=portfolio_df.index)) != "WARMUP"]
# fallback filter using note column
active = portfolio_df[~portfolio_df.apply(lambda r: r.get("note","") == "WARMUP", axis=1)]
if len(active):
    eq_series = active["portfolio_equity"]
    max_dd    = (1 - eq_series / eq_series.cummax()).max()
    total_ret = eq_series.iloc[-1] / TOTAL_CAPITAL - 1
    print(f"\n{'─'*55}")
    print(f"  Final equity         : {eq_series.iloc[-1]:>10.2f} USDT")
    print(f"  Total return         : {total_ret:>10.2%}")
    print(f"  Max drawdown         : {max_dd:>10.2%}")
    print(f"  Total trades         : {len(trades_df):>10}")
    print(f"{'─'*55}")


# %%
# =============================================================================
# 6. PLOT — 2 panels: equity + cash balance
# =============================================================================

# ── Anchor: last warmup row reset to TOTAL_CAPITAL so lines start at 1000 ────
warmup_mask  = portfolio_df.apply(lambda r: r.get("note","") == "WARMUP", axis=1)
warmup_rows  = portfolio_df[warmup_mask]
trading_rows = portfolio_df[~warmup_mask].copy()

if len(warmup_rows):
    anchor = warmup_rows.iloc[[-1]].copy()
    anchor["portfolio_equity"] = float(TOTAL_CAPITAL)
    anchor["cash"]             = float(TOTAL_CAPITAL)
    anchor["holdings_str"]     = "None"
    plot_df = pd.concat([anchor, trading_rows], ignore_index=True)
    plot_df = plot_df.sort_values("date").reset_index(drop=True)
else:
    plot_df = trading_rows.reset_index(drop=True)

hold_plot = hold_df[hold_df["date"].isin(plot_df["date"])].reset_index(drop=True)
if len(hold_plot) != len(plot_df):
    hold_plot = hold_df[warmup_days - 1:].reset_index(drop=True)
    if len(hold_plot) != len(plot_df):
        hold_plot = hold_df[hold_df["date"].isin(plot_df["date"])].reset_index(drop=True)

fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
    subplot_titles=[
        "Portfolio Equity (USDT)",
        "Cash Balance (USDT)  —  uninvested capital",
    ],
    row_heights=[0.70, 0.30],
    vertical_spacing=0.06)

# ── Panel 1: Equity ───────────────────────────────────────────────────────────
fig.add_trace(go.Scatter(
    x=plot_df["date"], y=plot_df["portfolio_equity"],
    name="Spot Strategy", mode="lines",
    line=dict(width=3, color="#00d4ff"),
    hovertemplate="Equity: %{y:.2f}<br>%{customdata}<extra></extra>",
    customdata=plot_df["holdings_str"]), row=1, col=1)

for coin in CANDIDATE_UNIVERSE:
    col = coin + "_hold"
    if col in hold_plot.columns:
        fig.add_trace(go.Scatter(
            x=plot_df["date"], y=hold_plot[col].values,
            name=f"Hold {coin}", mode="lines",
            line=dict(dash="dash"), opacity=0.35,
            hovertemplate=f"{coin}: %{{y:.2f}}<extra></extra>"), row=1, col=1)

fig.add_trace(go.Scatter(
    x=plot_df["date"], y=hold_plot["average_hold"].values,
    name="Average Hold", mode="lines",
    line=dict(width=2.5, color="white", dash="dash"),
    hovertemplate="Average: %{y:.2f}<extra></extra>"), row=1, col=1)

# ── Panel 2: Cash ─────────────────────────────────────────────────────────────
fig.add_trace(go.Scatter(
    x=plot_df["date"], y=plot_df["cash"],
    name="Cash (uninvested)", mode="lines",
    line=dict(width=1.5, color="#7fba00"),
    fill="tozeroy", fillcolor="rgba(127,186,0,0.10)"), row=2, col=1)

fig.update_layout(
    title=(f"Spot Backtest  |  {TOTAL_CAPITAL} USDT  |  "
           f"Fee={FEE_RATE*100:.2f}%  |  Exposure={EXPOSURE_BULL}"),
    paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
    font=dict(color="#c9d1d9", family="monospace", size=12),
    hovermode="x unified", width=1600, height=950,
    legend=dict(bgcolor="rgba(13,17,23,0.8)", font=dict(size=10),
                x=1.01, y=1, xanchor="left"))
for ax in ("xaxis","xaxis2","yaxis","yaxis2"):
    fig.update_layout(**{ax: dict(gridcolor="#21262d", zerolinecolor="#30363d")})

html_path = os.path.join(SCRIPT_DIR, "spot_portfolio_plot.html")
fig.write_html(html_path)
fig.show(renderer="browser")
print("Plot saved ->", html_path)