# %%
# ------------------ Imports ------------------
import os
from dotenv import load_dotenv
import pandas as pd
from binance.client import Client
from datetime import date, timedelta, datetime, timezone
from telegram_alert import send_telegram_message, send_telegram_file
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path

import plotly.graph_objects as go

# %%
# Load env
load_dotenv("../tradebot_deploy/.env")

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
client_api = Client(API_KEY, API_SECRET)

# %%
# ------------------ Config ------------------
BASE_DIR = Path(__file__).resolve().parent / "log"
os.makedirs(BASE_DIR, exist_ok=True)

CANDIDATE_UNIVERSE = os.getenv("CANDIDATE_UNIVERSE", "")
CANDIDATE_UNIVERSE = [x.strip() for x in CANDIDATE_UNIVERSE.split(",") if x.strip()]

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


# %%
def get_balance(client, coin, free=True):
    # fetch all spot account balances
    account_info = client.get_account()

    balance = 0.0
    for asset in account_info['balances']:
        if asset['asset'] == coin:
            # 'free' = available to trade
            balance = float(asset['free' if free else 'locked'])
            break

    return balance

# %%
def get_price(client, coin):
    # Fetch the latest USDT price for a given coin symbol from Binance.
    try:
        symbol = f"{coin}USDT"
        ticker = client.get_symbol_ticker(symbol=symbol)
        return float(ticker["price"])
    except Exception as e:
        print(f"Error fetching price for {coin}: {e}")
        return None

# %%
def get_yesterday_close(client, coin: str, interval: str = "1d"):
    # Get yesterday's close price for a given coin on Binance.
    symbol = f"{coin}USDT"
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

# %%
def get_api_orders_today(client, coins):
    if not coins:
        return pd.DataFrame(), pd.DataFrame()

    symbols = [f"{coin}USDT" for coin in coins]

    # --- time range: today UTC ---
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    start_ms = int(today_start.timestamp() * 1000)
    end_ms = int(today_end.timestamp() * 1000)

    rows = []
    for symbol in symbols:
        try:
            all_orders = client.get_all_orders(symbol=symbol, startTime=start_ms, endTime=end_ms)
        except Exception as e:
            print(f"Error fetching orders for {symbol}: {e}")
            continue

        for o in all_orders:
            cid = o.get("clientOrderId", "")
            if not cid.startswith("api_"):
                continue

            # Extract type from e.g. api_NORMAL_20251004_155246_352_h5H4
            parts = cid.split("_")
            order_type = parts[1] if len(parts) > 1 else "UNKNOWN"

            order_dt = datetime.fromtimestamp(o["time"] / 1000, tz=timezone.utc)
            orig_qty = float(o["origQty"])
            qty_coin = float(o["executedQty"])
            usdt_amount = float(o["cummulativeQuoteQty"])
            price_order = float(o["price"]) if float(o["price"]) > 0 else (
                usdt_amount / qty_coin if qty_coin > 0 else 0
            )
            price_filled = usdt_amount / qty_coin if qty_coin > 0 else 0

            rows.append({
                "date": order_dt.date(),
                "datetime": order_dt,
                "symbol": symbol,
                "side": o["side"],
                "type": order_type,
                "order_id": cid,
                "status": o["status"],
                "orig_qty": orig_qty,
                "coin_qty": qty_coin,
                "usdt": usdt_amount,
                "price_order": price_order,
                "price_filled": price_filled
            })

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    df_detail = pd.DataFrame(rows).sort_values("datetime").reset_index(drop=True)

    # --- Summary: only filled or partially filled ---
    df_summary = df_detail[df_detail["status"].isin(["FILLED", "PARTIALLY_FILLED"])].copy()
    if not df_summary.empty:
        grouped = df_summary.groupby(
            ["date", "symbol", "side", "type", "price_order"], as_index=False
        ).agg({
            "usdt": "sum",
            "coin_qty": "sum",
            "orig_qty": "sum"
        })
        grouped["price_filled"] = grouped["usdt"] / grouped["coin_qty"]
        df_summary = grouped[[
            "date", "symbol", "side", "type", "price_order",
            "orig_qty", "coin_qty", "usdt", "price_filled"
        ]]
    df_summary['datetime'] = datetime.now(timezone.utc)

    return df_summary, df_detail

# %%
def get_portfolio_df(client, candidates):
    # Return a DataFrame of today's portfolio value (USDT) including all coins + free USDT.
    row = {"date": datetime.now(timezone.utc).date(),
           "datetime": datetime.now(timezone.utc)}
    
    # free USDT
    free_usdt = get_balance(client, 'USDT')
    row['cash'] = free_usdt
    total_value = free_usdt
    
    for coin in candidates:
        if coin == 'USDT':
            row['USDT'] = free_usdt
            continue
        qty = get_balance(client, coin)
        if qty <= 0:
            row[coin] = 0
        else:
            price = get_price(client, coin)  # price in USDT
            value = qty * price
            row[coin] = value
            total_value += value
    
    row['total'] = total_value
    
    df = pd.DataFrame([row])
    # Optional: sort columns (date first, then total, cash, then coins alphabetically)
    cols = ['date', 'datetime', 'total', 'cash'] + sorted([c for c in row.keys() if c not in ['date', 'datetime', 'total', 'cash']])
    df = df[cols]

    # add holding coins names
    exclude_cols = {"date", "datetime", "total", "cash"}
    coin_cols = [c for c in df.columns if c not in exclude_cols]

    def get_holdings_str(row):
        held = [coin for coin in coin_cols if row.get(coin, 0) > 0]
        return ", ".join(held) if held else ""

    df["holdings_str"] = df.apply(get_holdings_str, axis=1)
    
    return df

# %%
def get_api_orders_today(client, coins, delta=0):
    # Return two DataFrames of API orders today (UTC) for given coins:
    if not coins:
        return pd.DataFrame(), pd.DataFrame()

    symbols = [f"{coin}USDT" for coin in coins]

    # --- time range: today UTC ---
    today_start = (datetime.now(timezone.utc)+timedelta(days=delta)).replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    start_ms = int(today_start.timestamp() * 1000)
    end_ms = int(today_end.timestamp() * 1000)

    rows = []
    for symbol in symbols:
        try:
            all_orders = client.get_all_orders(symbol=symbol, startTime=start_ms, endTime=end_ms)
        except Exception as e:
            print(f"Error fetching orders for {symbol}: {e}")
            continue

        for o in all_orders:
            cid = o.get("clientOrderId", "")
            if not cid.startswith("api_"):
                continue

            # Extract type from e.g. api_NORMAL_20251004_155246_352_h5H4
            parts = cid.split("_")
            order_type = parts[1] if len(parts) > 1 else "UNKNOWN"

            order_dt = datetime.fromtimestamp(o["time"] / 1000, tz=timezone.utc)
            orig_qty = float(o.get("origQty", 0))
            qty_coin = float(o.get("executedQty", 0))
            usdt_amount = float(o.get("cummulativeQuoteQty", 0))
            price_order = float(o.get("price", 0)) if float(o.get("price", 0)) > 0 else (
                usdt_amount / qty_coin if qty_coin > 0 else 0
            )
            price_filled = usdt_amount / qty_coin if qty_coin > 0 else 0
            orig_usdt = orig_qty * price_order

            rows.append({
                "date": order_dt.date(),
                "datetime": order_dt,
                "symbol": symbol,
                "side": o["side"],
                "type": order_type,
                "order_id": cid,
                "status": o["status"],
                "orig_qty": orig_qty,
                "coin_qty": qty_coin,
                "usdt": usdt_amount,
                "orig_usdt": orig_usdt,
                "price_order": price_order,
                "price_filled": price_filled
            })

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    df_detail = pd.DataFrame(rows)
    df_detail = df_detail.sort_values("datetime").reset_index(drop=True)

    # --- Summary: only FILLED or PARTIALLY_FILLED ---
    df_summary = df_detail[df_detail["status"].isin(["FILLED", "PARTIALLY_FILLED"])].copy()
    if not df_summary.empty:
        grouped = df_summary.groupby(
            ["date", "symbol", "side", "type", "price_order"], as_index=False
        ).agg({
            "usdt": "sum",
            "coin_qty": "sum",
            "orig_qty": "sum",
            "orig_usdt": "sum"
        })
        grouped["price_filled"] = grouped["usdt"] / grouped["coin_qty"]
        df_summary = grouped[[
            "date", "symbol", "side", "type", "price_order",
            "usdt", "coin_qty", "orig_qty", "orig_usdt", "price_filled"
        ]]
        df_summary = df_summary.sort_values("date").reset_index(drop=True)

    return df_summary, df_detail

# %%
def save_log(df, filename):
    # Save orders_filled, orders_detail, and portfolio_df to CSV files in log_folder.
    filled_file = BASE_DIR / f"{filename}.csv"
    if not df.empty:
        if os.path.exists(filled_file):
            df.to_csv(filled_file, mode='a', index=False, header=False)
        else:
            df.to_csv(filled_file, index=False)

    print(f"Logs saved to folder '{BASE_DIR}'.")


# %%
def generate_hold_df(client, portfolio_df, candidates, log_dir="log"):
    # Generate hold_single_df that tracks 'hold' value of each coin and portfolio total.
    hold_csv = BASE_DIR / "hold_single_df.csv"

    if portfolio_df is None or portfolio_df.empty:
        raise ValueError("portfolio_df is empty")

    # Normalize and extract dates
    portfolio_df = portfolio_df.copy()
    portfolio_df["date"] = pd.to_datetime(portfolio_df["date"]).dt.date
    all_dates = portfolio_df["date"].unique()
    total_first = float(portfolio_df.iloc[0]["total"])

    # === If file exists, load and continue ===
    if hold_csv.exists():
        hold_df = pd.read_csv(hold_csv)
        hold_df["date"] = pd.to_datetime(hold_df["date"]).dt.date
    else:
        hold_df = pd.DataFrame(columns=["date", *candidates, "portfolio", "average_hold"])

    # Only append new dates
    existing_dates = set(hold_df["date"].tolist())
    new_dates = [d for d in all_dates if d not in existing_dates]
    if not new_dates:
        print("[hold] No new dates to append.")
        return hold_df

    # Initialize hold values memory
    last_hold = {c: None for c in candidates}
    if not hold_df.empty:
        for c in candidates:
            if c in hold_df.columns:
                val = hold_df[c].dropna()
                last_hold[c] = val.iloc[-1] if len(val) > 0 else None
    else:
        for c in candidates:
            last_hold[c] = None

    # === Build new rows ===
    new_rows = []
    for d in new_dates:
        row = {"date": d}

        # portfolio total
        p_row = portfolio_df[portfolio_df["date"] == d]
        row["portfolio"] = float(p_row["total"].iloc[0]) if not p_row.empty else None

        for coin in candidates:
            # --- Determine if coin's first row ---
            if last_hold[coin] is None:
                # Coin not yet listed before
                try:
                    curr_price = get_price(client, coin)
                except Exception:
                    curr_price = None

                if curr_price and curr_price > 0:
                    # First appearance — assign portfolio's first total
                    row[coin] = total_first
                    last_hold[coin] = total_first
                else:
                    row[coin] = None  # Still not listed
            else:
                # Already had hold value
                try:
                    curr_price = get_price(client, coin)
                    yest_close = get_yesterday_close(client, coin)
                except Exception as e:
                    print(f"[hold WARN] price fetch fail for {coin}: {e}")
                    curr_price, yest_close = None, None

                if curr_price and yest_close and yest_close > 0:
                    row[coin] = last_hold[coin] * (curr_price / yest_close)
                    last_hold[coin] = row[coin]
                else:
                    # keep previous value (no price data)
                    row[coin] = last_hold[coin]

        # Average of non-empty holds
        coin_vals = [v for c, v in row.items() if c in candidates and pd.notna(v)]
        row["average_hold"] = sum(coin_vals) / len(coin_vals) if coin_vals else None

        new_rows.append(row)

    new_df = pd.DataFrame(new_rows)

    # Merge and save
    out_df = pd.concat([hold_df, new_df], ignore_index=True).sort_values("date")
    out_df.to_csv(hold_csv, index=False)
    print(f"[hold] Updated {hold_csv} with {len(new_rows)} new date(s).")

    return out_df.reset_index(drop=True)

# %%
client = client_api
cancel_open_orders(client)
orders_filled, orders_detail = get_api_orders_today(client, CANDIDATE_UNIVERSE, delta=0)
portfolio_df = get_portfolio_df(client, CANDIDATE_UNIVERSE)
hold_df = generate_hold_df(Client(), portfolio_df, CANDIDATE_UNIVERSE)
save_log(orders_filled, 'orders_filled')
save_log(orders_detail, 'orders_detail')
save_log(portfolio_df, 'portfolio_df')

# %%
portfolio_df = pd.read_csv(BASE_DIR / "portfolio_df.csv")
hold_df = pd.read_csv(BASE_DIR / "hold_single_df.csv")


# %%
# ------------------ 6. Plot ------------------
fig = go.Figure()

# Strategy Portfolio (bold black line)
fig.add_trace(go.Scatter(
    x=portfolio_df['date'],
    y=portfolio_df['total'],
    mode='lines',
    name='Strategy Portfolio',
    line=dict(width=3, color='black'),
    hovertemplate='Portfolio: %{y:.2f}<br>Holdings: %{customdata}<extra></extra>',
    customdata=portfolio_df['holdings_str']
))

# Each coin hold
for coin in CANDIDATE_UNIVERSE:
    col = coin
    if col in hold_df.columns:
        fig.add_trace(go.Scatter(
            x=portfolio_df['date'],
            y=hold_df[col],
            mode='lines',
            name=f'Hold {coin}',
            line=dict(dash='dash'),
            hovertemplate=f'{coin}: %{{y:.2f}}<extra></extra>',
            opacity=0.6  # start slightly transparent
        ))

# Average coin hold
fig.add_trace(go.Scatter(
    x=portfolio_df['date'],
    y=hold_df['average_hold'],
    mode='lines',
    name=f'Average Hold',
    line=dict(width=3, color='black', dash='dash'),
    hovertemplate=f'Average: %{{y:.2f}}<extra></extra>',

))

fig.update_layout(
    title="Portfolio Equity vs Single-Coin Hold",
    xaxis_title="Date",
    yaxis_title="Equity",
    hovermode="x unified",
    width=1200,   
    height=600    
)


# ---------------- Legend interaction: highlight instead of hide ----------------
fig.update_layout(
    legend=dict(itemclick="toggle", itemdoubleclick=False)  # disable default "isolate"
)

fig.update_traces(
    selector=dict(mode="lines"),
    line=dict(width=2)
)

fig.write_html(BASE_DIR / "portfolio_plot.html")
# pio.write_image(fig, BASE_DIR / "portfolio_plot.png", width=1200, height=600, scale=2)
# fig.show(renderer="browser")

# %%
send_telegram_file(BASE_DIR / "portfolio_plot.html")

