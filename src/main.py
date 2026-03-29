# %%
# ------------------ Imports ------------------
import os, json
from dotenv import load_dotenv
import pandas as pd
from binance.client import Client
# from binance import ThreadedWebsocketManager
from datetime import date, timedelta, datetime, timezone
import numpy as np
import warnings
import random
import string
import time
import math
from pathlib import Path
from telegram_alert import send_telegram_message #, send_telegram_file
warnings.filterwarnings("ignore")

# %%
SCRIPT_DIR = Path(__file__).resolve().parent

# Load env from absolute path
load_dotenv(SCRIPT_DIR / ".env")

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

client_api = Client(API_KEY, API_SECRET)

# %%
# ------------------ Config ------------------
BASE_DIR = SCRIPT_DIR / "log"
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(BASE_DIR / 'ohlcv', exist_ok=True)
STATE_FILE = BASE_DIR / 'trade_state.json'

CANDIDATE_UNIVERSE = os.getenv("CANDIDATE_UNIVERSE", "")
CANDIDATE_UNIVERSE = [x.strip() for x in CANDIDATE_UNIVERSE.split(",") if x.strip()]

TOP_N = len(CANDIDATE_UNIVERSE)
today = datetime.now(timezone.utc).date()

# === PARAMETERS (outside function) ===
if today.year % 4 == 2:
    lookback_mom = 
    vol_lookback = 
    recent_high_lookback = 
else:
    lookback_mom = 
    vol_lookback = 
    recent_high_lookback = 
    
btc_ema_short = 
btc_ema_long  = 

# Compute warm-up window (max of all rolling spans)
warmup_days = max(btc_ema_short, btc_ema_long, vol_lookback, lookback_mom, recent_high_lookback, cooldown_days)
print("Warm-up window:", warmup_days, "days")


START_DATE = (today + timedelta(days=-warmup_days)).strftime("%Y-%m-%d")
END_DATE = (today + timedelta(days=0)).strftime("%Y-%m-%d")

print('Today:', today)
print('Start Date:', START_DATE)
print('End Date:', END_DATE)

# %%
# ---------------- Helper: normalize symbol ----------------
def to_usdt_symbol(coin: str) -> str:
    coin = coin.upper()
    return coin if coin.endswith("USDT") else coin + "USDT"


# ------------------ 1. Fetch historical data ------------------
def fetch_all_data(client, coins, start_date, end_date):
    """
    Fetch daily OHLCV for each symbol in `coins` from Binance,
    between start_date and end_date (inclusive), and save to CSV.
    `coins` example: ["BTCUSDT", "ETHUSDT"]
    Dates format: "YYYY-MM-DD"
    """
    all_data = {}

    for coin in coins:
        try:
            klines = client.get_historical_klines(
                f"{coin}USDT",
                Client.KLINE_INTERVAL_1DAY,
                start_str=start_date,
                end_str=end_date
            )
            
            # Still empty → skip
            if not klines:
                print(f"Skipping {coin}, insufficient data")
                continue

            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                "Open time","Open","High","Low","Close","Volume",
                "Close time","Quote asset volume","Number of trades",
                "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
            ])

            # Keep columns and types consistent with original code
            df = df[["Open time","Open","High","Low","Close","Volume"]]
            df.rename(columns={
                "Open time": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            }, inplace=True)

            # Convert timestamp → datetime
            df["date"] = pd.to_datetime(df["date"], unit="ms")
            df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)

            # Save to CSV
            os.makedirs(BASE_DIR / "ohlcv", exist_ok=True)
            df.to_csv(BASE_DIR / "ohlcv" / f"{coin}_ohlcv.csv", index=False)

            all_data[coin] = df

        except Exception as e:
            print(f"Failed to fetch {coin}: {e}")

    return all_data

# ------------------ 2. Get daily top N coins ------------------
def get_daily_topN(all_data, top_n=TOP_N):
    daily_date = sorted(list(set(date for df in all_data.values() for date in df['date'])))
    topN_dict = {}
    for date in daily_date:
        market_caps = {}
        for coin, df in all_data.items():
            # print(pd.to_datetime(date), pd.to_datetime(df.date).values[0])
            date = pd.to_datetime(date)
            if date in df['date'].values:
                # print('ok')
                price = df[df['date']==date]['close']
                market_caps[coin] = price  # using price as proxy
        topN = sorted(market_caps, key=lambda x: market_caps[x].iloc[0], reverse=True)[:top_n]
        topN_dict[date] = topN
    return daily_date, topN_dict


# %%
def get_balance(client, coin, free=True):
    # IMPORTANT: point the client to the testnet base URL
    # client.API_URL = 'https://testnet.binance.vision/api'

    # fetch all spot account balances
    account_info = client.get_account()

    balance = 0.0
    for asset in account_info['balances']:
        if asset['asset'] == coin:
            # 'free' = available to trade
            balance = float(asset['free' if free else 'locked'])
            break

    # now use this instead of a hard-coded total_capital
    return balance


# %%
def get_current_usdt_value(client,
                            coin: str):
    """
    Return a dict {coin: value_in_usdt} for all coins in `coins`
    based on your testnet spot balances and current USDT prices.
    """
    # 1. Get your current free balances
    balance = get_balance(client, coin)

    # skip if you have no free balance for this asset
    if balance == 0:
        value = 0.0
        pass

    # 2. Find the correct trading pair to USDT
    if coin == 'USDT':
        # USDT itself—value is just the balance
        value = balance['USDT']
        pass

    symbol = to_usdt_symbol(coin)

    # 3. Get the latest ticker price
    ticker = client.get_symbol_ticker(symbol=symbol)
    price = float(ticker['price'])

    # 4. Value in USDT = balance × price
    value = balance * price

    return value

# get_current_usdt_value(client_api, 'RENDER')


# %%
def get_yesterday_close(client, coin: str, interval: str = "1d"):
    """
    Get yesterday's close price for a given coin on Binance.

    Parameters:
        client  : Binance Client instance
        coin    : Base coin symbol, e.g. "BTC"
        interval: Kline interval, default "1d" (daily)
    
    Returns:
        float: Yesterday's close price in USDT
    """
    symbol = to_usdt_symbol(coin)
    # Calculate timestamps for yesterday
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    start_str = yesterday.strftime("%d %b %Y")
    end_str = (yesterday + timedelta(days=1)).strftime("%d %b %Y")
    
    # Fetch klines for yesterday
    klines = client.get_historical_klines(symbol, interval, start_str, end_str)
    
    if not klines:
        raise ValueError(f"No data found for {symbol} on {yesterday}")
    
    # Close price is the 5th element in each kline: [Open time, Open, High, Low, Close, Volume, ...]
    close_price = float(klines[0][4])
    return close_price


# ---------------- Helper: get best bid/ask with retries ----------------
def get_best_bid_ask(client, coin: str):
    symbol = to_usdt_symbol(coin)
    try:
        t = client.get_orderbook_ticker(symbol=symbol)
        return float(t["bidPrice"]), float(t["askPrice"])
    except Exception as e:
        print(f"[WARN] get_best_bid_ask failed for {symbol}: {e}")
        return None, None

def get_best_bid_ask_with_retry(client, coin: str, retries=2, delay=0.2):
    for _ in range(retries):
        bid, ask = get_best_bid_ask(client, coin)
        if bid is not None and ask is not None:
            return bid, ask
        time.sleep(delay)
    return None, None

# ---------------- Helper: current ticker price ----------------
def get_current_price(client, coin: str):
    symbol = to_usdt_symbol(coin)
    try:
        t = client.get_symbol_ticker(symbol=symbol)
        return float(t["price"])
    except Exception as e:
        print(f"[WARN] get_current_price failed for {symbol}: {e}")
        return None

# ---------------- Helper: 1-minute ago price ----------------
def get_price_1m_ago(client, coin: str):
    symbol = to_usdt_symbol(coin)
    try:
        kl = client.get_klines(symbol=symbol, interval="1m", limit=2)
        if len(kl) < 2:
            return None
        return float(kl[-2][4])  # close of 1-min candle
    except Exception as e:
        print(f"[WARN] get_price_1m_ago failed for {symbol}: {e}")
        return None
    

# %%
# --- Helper: round to allowed step ---
def floor_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def format_step(value: float, step: float) -> str:
    step_str = f"{step:.16f}".rstrip("0")
    decimals = step_str[::-1].find(".") if "." in step_str else 0
    return f"{value:.{decimals}f}"


def determine_price(client, coin: str, tick_size: float, price: float = 0) -> str:
    symbol = to_usdt_symbol(coin)

    if price <= 0:
        price = float(client.get_symbol_ticker(symbol=symbol)["price"])

    price_floored = floor_step(price, tick_size)
    return format_step(price_floored, tick_size)

def generate_order_id(type="", prefix: str = "api", rand_len: int = 4) -> str:
    """
    Generate a unique clientOrderId for Binance orders.
    Example output: api_20250926_091512_123_ab1X
    """
    type = 'NORMAL' if type == "" else type
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]  # UTC time to ms
    rand = ''.join(random.choices(string.ascii_letters + string.digits, k=rand_len))
    return f"{prefix}_{type}_{now}_{rand}"

# %%
def order_buy(
    client,
    coin: str,
    budget: float,
    price: float = 0,
    type=""
) -> tuple[bool, float]:

    symbol = to_usdt_symbol(coin)

    usdt_balance = get_balance(client, "USDT")
    spend_cap = min(budget, usdt_balance)

    if spend_cap <= 0:
        return False, 0.0

    info = client.get_symbol_info(symbol)

    lot = next(f for f in info["filters"] if f["filterType"] == "LOT_SIZE")
    price_f = next(f for f in info["filters"] if f["filterType"] == "PRICE_FILTER")
    notional_f = next(
        f for f in info["filters"]
        if f["filterType"] in ("MIN_NOTIONAL", "NOTIONAL")
    )

    step_size = float(lot["stepSize"])
    min_qty = float(lot["minQty"])
    tick_size = float(price_f["tickSize"])
    min_notional = float(notional_f["minNotional"])

    price_str = determine_price(client, coin, tick_size, price)
    price_val = float(price_str)

    fee_rate = 0.001  # adjust if needed
    max_qty = spend_cap / (price_val * (1 + fee_rate))
    qty_val = floor_step(max_qty, step_size)

    if qty_val < min_qty:
        return False, 0.0

    if qty_val * price_val < min_notional:
        return False, 0.0

    qty_str = format_step(qty_val, step_size)

    try:
        time.sleep(0.5)
        order_id = generate_order_id(type)
        print(f"--- Actual order: price: {price_str}, qty: {qty_str}")
        if price > 0:
            order = client.order_limit_buy(
                symbol=symbol,
                quantity=qty_str,
                price=price_str,
                newClientOrderId=order_id,
            )
        else:
            order = client.order_market_buy(
                symbol=symbol,
                quantity=qty_str,
                newClientOrderId=order_id,
            )
        # print(f"Binance response: {order}")
        time.sleep(0.5)
        status = client.get_order(symbol=symbol, orderId=order["orderId"])
        print("Order status:", status)
    except Exception as e:
        print(f"[{datetime.now()}] BUY failed {coin}: {e}")
        return False, 0.0

    usdt_committed = qty_val * price_val
    return True, usdt_committed

# %%
def order_sell(
    client,
    coin: str,
    qty: float,
    price: float = 0,
    type=""
) -> bool:

    symbol = to_usdt_symbol(coin)

    coin_balance = get_balance(client, coin)
    if coin_balance <= 0:
        return True

    info = client.get_symbol_info(symbol)

    lot = next(f for f in info["filters"] if f["filterType"] == "LOT_SIZE")
    price_f = next(f for f in info["filters"] if f["filterType"] == "PRICE_FILTER")

    step_size = float(lot["stepSize"])
    min_qty = float(lot["minQty"])
    tick_size = float(price_f["tickSize"])

    qty_val = floor_step(min(qty, coin_balance), step_size)

    if qty_val < min_qty:
        return True

    qty_str = format_step(qty_val, step_size)
    price_str = determine_price(client, coin, tick_size, price)

    try:
        time.sleep(0.5)
        order_id = generate_order_id(type)
        print(f"--- Actual order: price: {price_str}, qty: {qty_str}")
        if price > 0:
            order = client.order_limit_sell(
                symbol=symbol,
                quantity=qty_str,
                price=price_str,
                newClientOrderId=order_id,
            )
        else:
            order = client.order_market_sell(
                symbol=symbol,
                quantity=qty_str,
                newClientOrderId=order_id,
            )
        print(f"Binance response: {order}")
        time.sleep(0.5)
        status = client.get_order(symbol=symbol, orderId=order["orderId"])
        print("Order status:", status)
    except Exception as e:
        print(f"[{datetime.now()}] SELL failed {coin}: {e}")
        return True

    return False

# %%
def execute_buy(client, coin, budget, price=0, type=""):
    """
    Incremental buy: place orders until budget exhausted or orders rejected.
    
    Returns:
        total_spent: float, actual USDT successfully used for this coin
    """
    total_spent = 0.0

    while budget > 0:
        accepted, spent = order_buy(client, coin, budget, price, type)
        if not accepted or spent <= 0:
            break  # cannot place more
        budget -= spent
        total_spent += spent

        if budget <= 1e-6:  # safety dust threshold
            break

    return total_spent


def execute_sell(client, coin, qty, price=0, type=""):
    stop = False
    while qty > 0 and stop == False:
        coin_bal_before = get_balance(client, coin)
        stop = order_sell(client, coin, qty, price, type)
        coin_bal_after = get_balance(client, coin)
        qty -= (coin_bal_before - coin_bal_after)


# %%
def cancel_open_orders(client, coin=None):
    """
    Cancel open orders with clientOrderId starting with "api_".
    If `coin` is provided, cancel only for that coin.
    If `coin` is None, cancel for all symbols.
    """
    if coin:
        symbols = [f"{coin}USDT"]
    else:
        # Get all unique symbols from open orders
        all_orders = client.get_open_orders()
        symbols = list({o["symbol"] for o in all_orders if o.get("clientOrderId", "").startswith("api_")})

    for symbol in symbols:
        open_orders = client.get_open_orders(symbol=symbol)
        for o in open_orders:
            if o.get("clientOrderId", "").startswith("api_"):
                client.cancel_order(symbol=o["symbol"], orderId=o["orderId"])


def has_open_api_sells(client, prefix="api_"):
    open_orders = client.get_open_orders()
    return any(
        o["side"] == "SELL" and o["clientOrderId"].startswith(prefix)
        for o in open_orders
    )


def can_place_any_buy(client, remaining, idx, min_usdt=1e-6):
    if idx >= len(remaining):
        return False

    free_usdt = get_balance(client, "USDT")
    if free_usdt <= min_usdt:
        return False

    # There is at least one remaining buy quota
    return any(
        r["remaining_usdt"] > 0
        for r in remaining[idx:]
    )


def incremental_buy_loop(
    client,
    top_buys,
    stop_time,
    poll_sec=10,
    prefix="api_"
):
    """
    Incrementally execute buys for top_buys using available USDT.

    top_buys: list of tuples (coin, price_buy, spend_usdt)
    """

    # Initialize remaining allocation state
    remaining = [
        {
            "coin": coin,
            "price": price_buy,
            "remaining_usdt": float(spend),
            "blocked": False,   # permanently unbuyable
            "wait_logged": False,    # <-- NEW: printed waiting message already?
        }
        for coin, price_buy, spend in top_buys
        if spend > 1e-6
    ]

    print(f"[{datetime.now(timezone.utc)}] Starting incremental buys for {len(remaining)} coins.")

    while True:
        now = datetime.now(timezone.utc)
        if now >= stop_time:
            print(f"[{now}] Stop time reached.")
            break

        free_usdt = get_balance(client, "USDT")
        if free_usdt < 1e-6:
            print(f"[{datetime.now(timezone.utc)}] No available USDT to spend. Waiting for next poll.")
            time.sleep(poll_sec)
            continue

        idx = 0  # reset pointer each poll

        # ---- Sequential consumption loop ----
        while free_usdt > 1e-6:
            # Skip blocked / exhausted coins
            while idx < len(remaining) and (remaining[idx]["blocked"] or remaining[idx]["remaining_usdt"] <= 1e-6):
                idx += 1

            if idx >= len(remaining):
                break

            cur = remaining[idx]
            coin = cur["coin"]
            price = cur["price"]

            spend_now = min(free_usdt, cur["remaining_usdt"])

            # Execute buy
            spent = execute_buy(client, coin, spend_now, price)

            if spent > 0:
                cur["remaining_usdt"] -= spent
                free_usdt -= spent
                cur["wait_logged"] = False  # <-- reset after success
                print(f"[{datetime.now(timezone.utc)}] Buy executed: {coin} spend {spent:.2f} USDT at price {price}. Remaining allocation: {cur['remaining_usdt']:.2f} USDT.")

                if cur["remaining_usdt"] <= 1e-6:
                    print(f"[{datetime.now(timezone.utc)}] Allocation for {coin} fully used. Moving to next coin.")
                    idx += 1

            else:
                # Buy failed — decide whether to retry later
                no_more_sells = not has_open_api_sells(client, prefix)

                if no_more_sells:
                    # Nothing will change → permanently block
                    cur["blocked"] = True
                    print(f"[{datetime.now(timezone.utc)}] Blocking {coin}: buy failed and no SELLs remain.")
                    idx += 1
                else:
                    # Will retry next poll if more USDT is freed
                    if not cur["wait_logged"]:
                        print(f"[{datetime.now(timezone.utc)}] Buy attempt for {coin} failed. Waiting for more USDT from SELL fills.")
                        cur["wait_logged"] = True
                break

        # ---- Stop condition ----
        no_more_sells = not has_open_api_sells(client, prefix)
        no_buyable_coins = not any(c["remaining_usdt"] > 1e-6 and not c["blocked"] for c in remaining)

        if no_more_sells and no_buyable_coins:
            print(f"[{datetime.now(timezone.utc)}] No open SELLs and no buyable coins left. Stopping incremental buy loop.")
            break

        time.sleep(poll_sec)

    print(f"[{datetime.now(timezone.utc)}] Incremental buy loop finished for the day.")


# ---------------- Main smart_sell function ----------------
def smart_sell(client, coin: str, qty: float, price: float, type="",
               fast_tol: float = 0.001, panic_tol: float = 0.002):
    """
    Smart sell using 1-minute price velocity (v1m) to trigger regimes.
    Falls back to yesterday's close if 1-min price is not available.

    Args:
        client: exchange client
        coin: base asset symbol, e.g., 'BTC'
        qty: quantity to sell
        price: fallback price for NORMAL regime
        fast_tol: tolerance for FAST regime (fractional, e.g., 0.001 = 0.1%)
        panic_tol: tolerance for PANIC regime (fractional, e.g., 0.003 = 0.3%)
    """
    now = datetime.now(timezone.utc)
    symbol = to_usdt_symbol(coin)

    # ---------- Thresholds based on 1-min velocity ----------
    FAST_THRESHOLD = -0.003   # -0.3% drop in 1 min triggers FAST
    PANIC_THRESHOLD = -0.006  # -0.4% drop in 1 min triggers PANIC

    # ---------- Get current price ----------
    best_bid, best_ask = get_best_bid_ask_with_retry(client, coin)
    price_source = "orderbook"
    if best_bid is None or best_ask is None:
        cur_price = get_current_price(client, coin)
        if cur_price and cur_price > 0:
            best_bid = best_ask = cur_price
            price_source = "ticker"
        else:
            best_bid = best_ask = price
            price_source = "fallback"

    current_price = best_bid  # base for FAST / PANIC

    # ---------- Compute 1-min velocity ----------
    price_1m_ago = get_price_1m_ago(client, coin)
    if not price_1m_ago or price_1m_ago <= 0:
        # fallback to yesterday close if 1-min price unavailable
        price_1m_ago = get_yesterday_close(client, coin)

    v1m = (current_price - price_1m_ago) / price_1m_ago if price_1m_ago and price_1m_ago > 0 else 0

    # ---------- Decide sell regime ----------
    if v1m < PANIC_THRESHOLD:
        # PANIC regime: more aggressive
        sell_price = current_price * (1 - panic_tol)
        print(f"[{now}] {symbol} PANIC regime (v1m={v1m:.6f}, source={price_source}). "
              f"Aggressive LIMIT @ {sell_price:.6f}")
        return execute_sell(client, coin, qty, price=sell_price, type=type)

    elif v1m < FAST_THRESHOLD:
        # FAST regime: moderately aggressive
        sell_price = current_price * (1 - fast_tol)
        print(f"[{now}] {symbol} FAST regime (v1m={v1m:.6f}, source={price_source}). "
              f"Aggressive LIMIT @ {sell_price:.6f}")
        return execute_sell(client, coin, qty, price=sell_price, type=type)

    else:
        # # NORMAL regime: use caller-provided price but no higher than current market
        sell_price = min(price, current_price)
        print(f"[{now}] {symbol} NORMAL regime (v1m={v1m:.6f}, source={price_source}). "
              f"LIMIT @ {sell_price:.6f}")
        return execute_sell(client, coin, qty, price=sell_price, type=type)


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


# %%
def trade_automate(client, all_data, daily_date, topN_dict,
                   btc_symbol='BTC',
                   btc_ema_short=btc_ema_short,
                   btc_ema_long=btc_ema_long,
                   exposure_bull=, 
                   # exposure_neutral=, exposure_bear=, 
                   # neutral_band=,
                   lookback_mom=lookback_mom,
                   vol_lookback=vol_lookback,
                   blend_equal_weight=,
                   min_trade=,
                   max_drawdown_before_freeze=0.05,  
                   max_drawdown_before_freeze_coin=, 
                   recent_high_lookback=recent_high_lookback,
                   warmup_days=warmup_days,
                   tol = 0.001):
    """
    Hybrid regime + dynamic-weight backtest with:
      - per-coin trailing stop,
      - cooldown after bear regime,
      - buy-freeze guard when BTC has dropped from recent high more than threshold.
    Returns equity_df, trades_df
    """

    # --- init ---
    order_attempts= {}

    # prepare BTC EMAs and ensure btc exists
    if btc_symbol not in all_data:
        raise ValueError("BTC price series missing: need key " + btc_symbol)
    btc_df = all_data[btc_symbol].copy().sort_values("date").reset_index(drop=True)
    btc_df['EMA_short'] = btc_df['close'].ewm(span=btc_ema_short, adjust=False).mean()
    btc_df['EMA_long'] = btc_df['close'].ewm(span=btc_ema_long, adjust=False).mean()

    # current_regime = "neutral"
    buy_frozen_until = None  # optionally freeze buys until this date (when BTC drawdown recovers)

    start_date = daily_date[0] + pd.Timedelta(days=warmup_days)

    for current_date in daily_date:
        top_buys = []
        current_date_dt = pd.to_datetime(current_date)
        candidates = topN_dict.get(current_date, [])

        exposure = exposure_bull

        # --- compute BTC recent-high drawdown (buy-freeze guard) ---
        btc_prices_before = btc_df[btc_df['date'] < current_date_dt]['close']
        btc_drawdown = 0.0
        if len(btc_prices_before):
            recent_high = float(btc_prices_before.tail(recent_high_lookback).max())
            curr_btc_price = float(btc_prices_before.iloc[-1])
            if recent_high > 0:
                btc_drawdown = 1.0 - (curr_btc_price / recent_high)  # e.g. 0.05 if 5% down
        # Decide global buy-freeze boolean
        global_buy_freeze = btc_drawdown > max_drawdown_before_freeze

        buy_frozen_until = None

        # if global freeze, set buy_frozen_until None (we'll unfreeze when drawdown recovers)
        if global_buy_freeze and buy_frozen_until is None:
            # mark currently frozen (no automatic "until" date; unfreeze when drawdown <= threshold)
            buy_frozen_until = "FROZEN"

        # --- Skip buying during warmup ---
        if current_date_dt < start_date:
            continue

        scores = {}
        for coin in candidates:
            df = all_data[coin]
            df_local = df[df['date'] < current_date_dt].reset_index(drop=True)
            df_local = df_local.tail(warmup_days)
            if len(df_local) < warmup_days:
                scores[coin] = 0.0
                continue
            scores[coin] = compute_signal_score(df_local)

        weights = allocate_portfolio_weights(scores)

        # --- compute current total equity (use < current_date to avoid lookahead) ---
        total_equity_now = get_balance(client, 'USDT')
        for coin in all_data.keys():
            df = all_data[coin]
            price_rows = df.loc[df['date'] < current_date_dt, 'close']
            if len(price_rows) == 0:
                continue
            price = float(price_rows.iloc[-1])
            total_equity_now += get_balance(client, coin) * price

        target_invest = exposure * total_equity_now
        target_per_coin = {coin: target_invest * w for coin, w in zip(candidates, weights)}


        # --- REBALANCE / BUY (respect buy-freeze) ---
        # If global_buy_freeze is True, we skip any BUY operations, but still allow sells.
        for coin in candidates:
            df = all_data[coin]
            price_rows = df.loc[df['date'] < current_date_dt, 'close']
            if len(price_rows) == 0:
                continue
            price = float(price_rows.iloc[-1])
            qty = get_balance(client, coin)
            value = qty * price
            delta = target_per_coin[coin] - value

            # skip tiny adjustments
            if abs(delta) < min_trade:
                continue

            # SELL side: always allowed (if exposure or rebalancing requires sell)
            if delta < 0:
                sell_value = min(-delta, value)
                if sell_value < min_trade:
                    continue
                price_sell = price * (1 - tol)
                qty_sell = min(qty, sell_value / price_sell)
                if qty_sell *  price_sell < min_trade:
                    continue
                order_attempts[coin] = ('SELL-NORMAL', datetime.now(timezone.utc), qty_sell, price_sell)
                print(f"[{datetime.now(timezone.utc)}] Executing normal sell {coin}: {qty_sell:.6f} units at price {price_sell}, est value {qty_sell * price_sell:.2f}")
                smart_sell(client, coin, qty_sell, price_sell)

                # if get_balance(client, coin) <= 0:
                #     save_high_since_buy(coin, 0.0)

                continue

            # BUY side: check global freeze and optional per-coin drawdown guard
            # If buys are frozen globally (BTC drawdown) -> skip BUY
            if buy_frozen_until is not None:
                # skip buys while frozen
                continue

            # Optional per-coin guard: skip if coin itself dropped too much from its recent high
            # (helps avoid buying coins that are already collapsing even if BTC is OK)
            df = all_data[coin]
            coin_prices_before = df.loc[df['date'] < current_date_dt, 'close']
            coin_recent_high = float(coin_prices_before.tail(recent_high_lookback).max()) if len(coin_prices_before) else price
            coin_drawdown = 0.0
            if coin_recent_high > 0:
                coin_drawdown = 1.0 - (price / coin_recent_high)
            if coin_drawdown > max_drawdown_before_freeze_coin:
                # skip buying this coin (but do not prevent buying other coins)
                continue

            # proceed buy (spend up to delta but limited by cash)
            spend = delta
            if spend < min_trade or spend < 1e-6:
                continue
            price_buy = price * (1 + tol)
            order_attempts[coin] = ('BUY-NORMAL', datetime.now(timezone.utc), spend, price_buy)
            top_buys.append((coin, price_buy, spend))

        # --- sort by spend descending before executing buys ---
        top_buys.sort(key=lambda x: x[2], reverse=True)  # x[2] = spend
        
        # Save log attempts
        attempts_rows = []
        attempts_report = f'Trade Attempts on {END_DATE}: \n'
        if order_attempts:
            for coin, info in order_attempts.items():
                if isinstance(info, tuple):
                    action, datetm, qty, price = info
                else:
                    action, qty = info, None
                row = {
                    "date": current_date_dt,
                    "datetime": datetm,
                    "coin": coin,
                    "action": action,
                    "qty": qty,
                    "price": price}
                attempts_rows.append(row)
                attempts_report += f'Coin: {coin}\tAction: {action}\tQty_Spend: {qty:.6f}\tPrice: {price:.6f}\n'
        else:
            row = {
                    "date": current_date_dt,
                    "datetime": datetime.now(timezone.utc),
                    "coin": None,
                    "action": None}
            attempts_rows.append(row)
            attempts_report += f'No trade\n'
        send_telegram_message(attempts_report)

        df_attempts = pd.DataFrame(attempts_rows)
        df_attempts = df_attempts.sort_values(by=["datetime", "coin"]).reset_index(drop=True)

        # --- Save to log folder ---
        log_path = BASE_DIR / "order_attempts.csv"

        if os.path.exists(log_path):
            df_attempts.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_attempts.to_csv(log_path, index=False)

        # Buying sequence
        if not top_buys:
            print("No top buys, skipping buys.")
        else:
            stop_time = datetime.now(timezone.utc).replace(hour=23, minute=55, second=0, microsecond=0)
            if datetime.now(timezone.utc) >= stop_time:
                stop_time += timedelta(days=1)

            incremental_buy_loop(
                client=client,
                top_buys=top_buys,
                stop_time=stop_time,
                poll_sec=10,
                prefix="api_"
            )
            
        print(f"[{datetime.now(timezone.utc)}] trade_automate finished for the day.")
        
    return df_attempts

# %%
# ------------------ 5. Run full pipeline ------------------
client = client_api
cancel_open_orders(client)
all_data = fetch_all_data(Client(), CANDIDATE_UNIVERSE, START_DATE, END_DATE)
daily_date, topN_dict = get_daily_topN(all_data)
df_attempts = trade_automate(client, all_data, daily_date, topN_dict)
