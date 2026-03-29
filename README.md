# Algorithmic Crypto Spot Trading Bot (Binance)

## Overview
This project is an automated crypto spot trading system executing algorithmic strategies on Binance.  
It includes modules for live trading, trade recording, and backtesting strategies with visual results.

---

## Folder Structure
src/ → Python scripts (main.py, recorder.py, telegram_alert.py)
scripts/ → Automation shell scripts
config/ → Environment configuration (.env)
backtest/ → backtest.py, results.html
requirements.txt → Python dependencies
README.md → Project overview

---

## Features
- Automated live spot trading via Binance API
- Trade recording and logging
- Strategy backtesting with visual output
- Daily reports are sent to telegram

---

## Strategy
- Rule-based algorithm using momentum and mean-reversion signals
- Risk management: position sizing and stop-loss

---

## Backtesting
The backtest script generates performance metrics and a visual HTML chart.

---

## Disclaimer
This project is for research and educational purposes only. Not financial advice.

---

## Note:
Core signal generation, portfolio weights and parameter optimization logic have been abstracted
to protect proprietary research. 