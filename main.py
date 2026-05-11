import keep_alive
import time
import os
import telebot
import time
import logging
from datetime import datetime, timedelta

import pytz
import requests
import pandas as pd

import yfinance as yf 

# ─────────────────────────────────────────────
#  CONFIGURATION LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  SECRETS (Replit Secrets / variables d'env)
# ─────────────────────────────────────────────
TELEGRAM_TOKEN   = "8794987935:AAECh2yzzM_g9dZ3ki3tlKC1UWdC44YOjCk"
TELEGRAM_CHAT_ID = "1432682636"

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    logger.warning("⚠️  TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID manquant — les alertes ne seront pas envoyées.")

# ─────────────────────────────────────────────
#  MONEY MANAGEMENT
# ─────────────────────────────────────────────
ACCOUNT_SIZE   = 40_000   # €
RISK_PER_TRADE = 200      # €
STOP_LOSS_PTS  = 4.0      # points (= 40 pips XAU)
TAKE_PROFIT_PTS = 8.0     # points (= 80 pips XAU)
PIP_VALUE      = 100      # valeur par point par lot standard XAU

LOT_SIZE = round(RISK_PER_TRADE / (STOP_LOSS_PTS * PIP_VALUE), 2)
logger.info(f"💰 Taille de lot calculée : {LOT_SIZE} lot(s)")

# ─────────────────────────────────────────────
#  PARAMÈTRES DE STRATÉGIE
# ─────────────────────────────────────────────
RSI_PERIOD      = 14
EMA_H4_PERIOD   = 200
EMA_H1_PERIOD   = 50
VOL_SMA_PERIOD  = 20
VOL_MULTIPLIER  = 1.2
RSI_BUY_CROSS   = 55
RSI_SELL_CROSS  = 45
RSI_M15_MID     = 50

COOLDOWN_MINUTES = 15     # anti-doublon

# ─────────────────────────────────────────────
#  FILTRE HORAIRE PARIS
# ─────────────────────────────────────────────
PARIS_TZ = pytz.timezone("Europe/Paris")

SESSION_WINDOWS = [
    (9, 0,  11, 30),   # 09:00 → 11:30
    (14, 30, 17, 30),  # 14:30 → 17:30
]

def is_in_session() -> bool:
    """Vérifie si l'heure actuelle est dans une fenêtre de trading Paris."""
    now = datetime.now(PARIS_TZ)
    for h_start, m_start, h_end, m_end in SESSION_WINDOWS:
        start = now.replace(hour=h_start, minute=m_start, second=0, microsecond=0)
        end   = now.replace(hour=h_end,   minute=m_end,   second=0, microsecond=0)
        if start <= now <= end:
            return True
    return False

# ─────────────────────────────────────────────
#  TÉLÉGRAM
# ─────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    """Envoie un message Telegram. Retourne True si succès."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram non configuré — message non envoyé.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("✅ Message Telegram envoyé.")
        return True
    except requests.RequestException as e:
        logger.error(f"❌ Erreur Telegram : {e}")
        return False

def format_signal_message(direction: str, price: float, sl: float, tp: float, lot: float) -> str:
    """Formate le message de signal Telegram."""
    arrow = "🟢 ACHAT" if direction == "BUY" else "🔴 VENTE"
    return (
        f"<b>⚡ SIGNAL XAU/USD — {arrow}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 <b>Entrée  :</b> {price:.2f}\n"
        f"🛑 <b>Stop Loss :</b> {sl:.2f}  ({STOP_LOSS_PTS:.1f} pts)\n"
        f"🎯 <b>Take Profit :</b> {tp:.2f}  ({TAKE_PROFIT_PTS:.1f} pts)\n"
        f"📦 <b>Volume  :</b> {lot} lot(s)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <i>Volume confirmé par ETF GLD</i>\n"
        f"🕐 {datetime.now(PARIS_TZ).strftime('%d/%m/%Y %H:%M')} (Paris)"
    )

# ─────────────────────────────────────────────
#  RÉCUPÉRATION DES DONNÉES
# ─────────────────────────────────────────────
def fetch_ohlcv(ticker: str, interval: str, period: str) -> pd.DataFrame:
    """
    Télécharge les données OHLCV via yfinance.
    interval : '5m', '15m', '1h', '4h', '1d' …
    period   : '1d', '5d', '60d' …
    """
    try:
        df = yf.download(ticker, interval=interval, period=period,
                         progress=False, auto_adjust=True)
        if df.empty:
            logger.warning(f"Données vides pour {ticker} ({interval})")
        return df
    except Exception as e:
        logger.error(f"Erreur téléchargement {ticker} ({interval}): {e}")
        return pd.DataFrame()

# ─────────────────────────────────────────────
#  CALCUL DES INDICATEURS
# ─────────────────────────────────────────────
def get_latest_close(df: pd.DataFrame) -> float | None:
    if df.empty or "Close" not in df.columns:
        return None
    series = df["Close"].dropna()
    return float(series.iloc[-1]) if not series.empty else None

def compute_ema(df: pd.DataFrame, period: int) -> float | None:
    if df.empty or "Close" not in df.columns:
        return None
    closes = df["Close"].squeeze()
    ema = ta.ema(closes, length=period)
    if ema is None or ema.empty:
        return None
    return float(ema.dropna().iloc[-1])

def compute_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> tuple[float | None, float | None]:
    """Retourne (rsi_prev, rsi_last) pour détecter les croisements."""
    if df.empty or "Close" not in df.columns:
        return None, None
    closes = df["Close"].squeeze()
    rsi = ta.rsi(closes, length=period)
    if rsi is None or rsi.empty:
        return None, None
    rsi_clean = rsi.dropna()
    if len(rsi_clean) < 2:
        return None, None
    return float(rsi_clean.iloc[-2]), float(rsi_clean.iloc[-1])

def compute_volume_signal(df_gld: pd.DataFrame) -> bool:
    """
    Vérifie si le volume M5 de GLD > 1.2 × SMA(Volume, 20).
    Utilise la colonne Volume du DataFrame GLD.
    """
    if df_gld.empty or "Volume" not in df_gld.columns:
        return False
    vol = df_gld["Volume"].dropna()
    if len(vol) < VOL_SMA_PERIOD + 1:
        return False
    vol_sma = vol.rolling(VOL_SMA_PERIOD).mean()
    last_vol = float(vol.iloc[-1])
    last_sma = float(vol_sma.iloc[-1])
    if last_sma == 0:
        return False
    ratio = last_vol / last_sma
    logger.info(f"📊 Volume GLD ratio : {ratio:.2f} (seuil {VOL_MULTIPLIER})")
    return ratio > VOL_MULTIPLIER

# ─────────────────────────────────────────────
#  LOGIQUE DE SIGNAL MTF
# ─────────────────────────────────────────────
def analyse() -> dict | None:
    """
    Exécute l'analyse Multi-Timeframe complète.
    Retourne un dict {'direction', 'price', 'sl', 'tp'} ou None.
    """
    logger.info("🔍 Début de l'analyse MTF…")

    # ── H4 : Prix vs EMA 200 ──
    df_h4 = fetch_ohlcv("XAUUSD=X", "1h", "60d")   # yfinance n'a pas de 4h natif → on agrège
    if df_h4.empty:
        logger.warning("H4 : données indisponibles.")
        return None

    # Resample 1h → 4h pour obtenir un vrai H4
    df_h4 = df_h4.resample("4h").agg({
        "Open": "first", "High": "max", "Low": "min",
        "Close": "last", "Volume": "sum"
    }).dropna()

    price_h4  = get_latest_close(df_h4)
    ema200_h4 = compute_ema(df_h4, EMA_H4_PERIOD)

    if price_h4 is None or ema200_h4 is None:
        logger.warning("H4 : prix ou EMA200 indisponible.")
        return None

    h4_bull = price_h4 > ema200_h4
    h4_bear = price_h4 < ema200_h4
    logger.info(f"H4 → Prix={price_h4:.2f} | EMA200={ema200_h4:.2f} | Bull={h4_bull} Bear={h4_bear}")

    # ── H1 : Prix vs EMA 50 ──
    df_h1 = fetch_ohlcv("XAUUSD=X", "1h", "30d")
    price_h1  = get_latest_close(df_h1)
    ema50_h1  = compute_ema(df_h1, EMA_H1_PERIOD)

    if price_h1 is None or ema50_h1 is None:
        logger.warning("H1 : prix ou EMA50 indisponible.")
        return None

    h1_bull = price_h1 > ema50_h1
    h1_bear = price_h1 < ema50_h1
    logger.info(f"H1 → Prix={price_h1:.2f} | EMA50={ema50_h1:.2f} | Bull={h1_bull} Bear={h1_bear}")

    # ── M15 : RSI > / < 50 ──
    df_m15 = fetch_ohlcv("XAUUSD=X", "15m", "5d")
    _, rsi_m15 = compute_rsi(df_m15)

    if rsi_m15 is None:
        logger.warning("M15 : RSI indisponible.")
        return None

    m15_bull = rsi_m15 > RSI_M15_MID
    m15_bear = rsi_m15 < RSI_M15_MID
    logger.info(f"M15 → RSI={rsi_m15:.2f} | Bull={m15_bull} Bear={m15_bear}")

    # ── M5 : RSI croisement + Volume GLD ──
    df_m5_xau = fetch_ohlcv("XAUUSD=X", "5m", "2d")
    df_m5_gld = fetch_ohlcv("GLD", "5m", "2d")

    rsi_m5_prev, rsi_m5_last = compute_rsi(df_m5_xau)
    vol_ok = compute_volume_signal(df_m5_gld)
    price_now = get_latest_close(df_m5_xau)

    if rsi_m5_prev is None or rsi_m5_last is None:
        logger.warning("M5 : RSI indisponible.")
        return None
    if price_now is None:
        logger.warning("M5 : prix indisponible.")
        return None

    logger.info(f"M5 → RSI prev={rsi_m5_prev:.2f} last={rsi_m5_last:.2f} | Volume OK={vol_ok}")

    # Croisements RSI
    cross_up   = (rsi_m5_prev < RSI_BUY_CROSS)  and (rsi_m5_last >= RSI_BUY_CROSS)
    cross_down = (rsi_m5_prev > RSI_SELL_CROSS)  and (rsi_m5_last <= RSI_SELL_CROSS)

    # ── Décision finale ──
    if h4_bull and h1_bull and m15_bull and cross_up and vol_ok:
        sl = round(price_now - STOP_LOSS_PTS, 2)
        tp = round(price_now + TAKE_PROFIT_PTS, 2)
        logger.info(f"✅ SIGNAL ACHAT → Entrée={price_now:.2f} SL={sl} TP={tp}")
        return {"direction": "BUY", "price": price_now, "sl": sl, "tp": tp}

    if h4_bear and h1_bear and m15_bear and cross_down and vol_ok:
        sl = round(price_now + STOP_LOSS_PTS, 2)
        tp = round(price_now - TAKE_PROFIT_PTS, 2)
        logger.info(f"✅ SIGNAL VENTE → Entrée={price_now:.2f} SL={sl} TP={tp}")
        return {"direction": "SELL", "price": price_now, "sl": sl, "tp": tp}

    logger.info("⏳ Aucun signal validé sur ce cycle.")
    return None

# ─────────────────────────────────────────────
#  BOUCLE PRINCIPALE
# ─────────────────────────────────────────────
def main():
    logger.info("🚀 Bot XAU/USD démarré — Précision Institutionnelle")
    logger.info(f"   Lot fixe  : {LOT_SIZE} | Risque : {RISK_PER_TRADE}€ | SL : {STOP_LOSS_PTS}pts | TP : {TAKE_PROFIT_PTS}pts")

    last_signal_time: datetime | None = None
    last_direction:   str | None      = None

    while True:
        try:
            now_paris = datetime.now(PARIS_TZ)

            # ── Filtre horaire ──
            if not is_in_session():
                next_check = 60  # vérifie toutes les minutes hors session
                logger.info(f"🕐 Hors session Paris ({now_paris.strftime('%H:%M')}) — prochain scan dans {next_check}s")
                time.sleep(next_check)
                continue

            # ── Analyse MTF ──
            signal = analyse()

            if signal:
                direction = signal["direction"]

                # ── Anti-doublon (cooldown 15 min) ──
                cooldown_ok = True
                if last_signal_time and last_direction == direction:
                    elapsed = (now_paris - last_signal_time).total_seconds() / 60
                    if elapsed < COOLDOWN_MINUTES:
                        cooldown_ok = False
                        logger.info(f"🔁 Signal {direction} ignoré (cooldown {elapsed:.1f}/{COOLDOWN_MINUTES} min)")

                if cooldown_ok:
                    msg = format_signal_message(
                        direction=direction,
                        price=signal["price"],
                        sl=signal["sl"],
                        tp=signal["tp"],
                        lot=LOT_SIZE
                    )
                    logger.info(f"\n{msg}")
                    send_telegram(msg)
                    last_signal_time = now_paris
                    last_direction   = direction

            # Scan toutes les 5 minutes (aligné M5)
            time.sleep(300)

        except KeyboardInterrupt:
            logger.info("🛑 Bot arrêté manuellement.")
            break
        except Exception as e:
            logger.error(f"💥 Erreur inattendue : {e}", exc_info=True)
            time.sleep(60)

# ─────────────────────────────────────────────
if __name__ == "__main__":
    try:
        from keep_alive import keep_alive
        keep_alive()
        print("🚀 Bot XAU/USD démarré et en attente de messages...")
        bot.polling(none_stop=True)
    except Exception as e:
        print(f"❌ Erreur au lancement : {e}")

