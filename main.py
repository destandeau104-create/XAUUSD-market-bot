import os
import time
import threading
from datetime import datetime
import pytz
import pandas as pd
import numpy as np
import yfinance as yf
import telebot
from flask import Flask

TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
bot     = telebot.TeleBot(TOKEN)

PARIS_TZ       = pytz.timezone("Europe/Paris")
LOT_SIZE       = 0.50
ATR_PERIOD     = 14
ATR_SPIKE_MULT = 2.5
SL_ATR_MULT    = 1.5
SL_MIN_PIPS    = 15.0
SL_MAX_PIPS    = 100.0
TP_RR          = 2.0

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot XAU/USD Sniper actif", 200

@app.route("/health")
def health():
    now = datetime.now(PARIS_TZ).strftime("%H:%M:%S")
    return "OK " + now, 200

def run_flask():
    print("Flask demarre sur 0.0.0.0:8080", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)

# ============================================================
#  INDICATEURS
# ============================================================

def calc_ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def calc_rsi(series, length=14):
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_g = gain.ewm(com=length - 1, adjust=False).mean()
    avg_l = loss.ewm(com=length - 1, adjust=False).mean()
    rs    = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    high  = df["High"].squeeze()
    low   = df["Low"].squeeze()
    close = df["Close"].squeeze()
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def ema_bias(df):
    """
    +1 : prix > EMA20 ET prix > EMA50
    -1 : prix < EMA20 ET prix < EMA50
     0 : pas de biais clair
    """
    if df is None or len(df) < 55:
        return 0
    close = df["Close"].squeeze()
    ema20 = calc_ema(close, 20)
    ema50 = calc_ema(close, 50)
    price = float(close.iloc[-2])
    e20   = float(ema20.iloc[-2])
    e50   = float(ema50.iloc[-2])
    if price > e20 and price > e50:
        return 1
    if price < e20 and price < e50:
        return -1
    return 0

# ============================================================
#  SESSION
# ============================================================

def is_in_session():
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    matin = (h, m) >= (8, 0) and (h, m) <= (12, 0)
    aprem = (h, m) >= (14, 15) and (h, m) <= (18, 0)
    return matin or aprem

def get_data(ticker, interval, period):
    try:
        return yf.download(ticker, interval=interval, period=period, progress=False, auto_adjust=True)
    except Exception as e:
        print("Erreur get_data " + ticker + " : " + str(e), flush=True)
        return pd.DataFrame()

# ============================================================
#  VOLUME HYBRIDE XAU/USD
#  Source 1 : GC=F  (Gold Futures) - prioritaire
#  Source 2 : XAUUSD=X (Spot)      - fallback
#  Source 3 : Skip si data KO et 4 TF alignes
# ============================================================

def get_volume_series(ticker):
    try:
        df = yf.download(ticker, interval="5m", period="2d", progress=False, auto_adjust=True)
        if df is None or df.empty or "Volume" not in df.columns:
            return None
        vol = df["Volume"].squeeze()
        if len(vol) < 17:
            return None
        if float(vol.iloc[-2]) == 0 and float(vol.iloc[-17:-2].mean()) == 0:
            return None
        return vol
    except Exception as e:
        print("Erreur volume " + ticker + " : " + str(e), flush=True)
        return None

def check_volume():
    vol_series = get_volume_series("GC=F")
    if vol_series is not None:
        source = "Futures (GC=F)"
    else:
        vol_series = get_volume_series("XAUUSD=X")
        if vol_series is not None:
            source = "Spot (XAUUSD=X)"
        else:
            print("Volume source : Skip - autorisation exceptionnelle", flush=True)
            return True, "Skip", 0, 0
    vol_signal = float(vol_series.iloc[-2])
    vol_avg_15 = float(vol_series.iloc[-17:-2].mean())
    print("Volume source : " + source + " | Signal=" + str(round(vol_signal, 0)) + " | Moyenne15=" + str(round(vol_avg_15, 0)), flush=True)
    if vol_avg_15 > 0 and vol_signal <= vol_avg_15:
        print("Volume insuffisant - signal annule", flush=True)
        return False, source, vol_signal, vol_avg_15
    print("Volume OK", flush=True)
    return True, source, vol_signal, vol_avg_15

# ============================================================
#  FILTRE DXY (correlation inverse XAU/USD)
#  BUY Gold  => DXY bougie M5 rouge (Close < Open)
#  SELL Gold => DXY bougie M5 verte (Close > Open)
# ============================================================

def check_dxy(direction):
    """
    Retourne (ok, dxy_close, dxy_tendance)
    ok           = True si DXY confirme la direction
    dxy_close    = valeur actuelle du DXY
    dxy_tendance = description lisible de la tendance DXY
    """
    try:
        df_dxy = yf.download("DX-Y.NYB", interval="5m", period="2d", progress=False, auto_adjust=True)
        if df_dxy is None or df_dxy.empty or len(df_dxy) < 3:
            print("DXY indisponible - filtre bypasse", flush=True)
            return True, 0.0, "N/A"
        dxy_close    = float(df_dxy["Close"].squeeze().iloc[-2])
        dxy_open_val = float(df_dxy["Open"].squeeze().iloc[-2])
        dxy_baisse   = dxy_close < dxy_open_val
        dxy_hausse   = dxy_close > dxy_open_val
        dxy_tendance = "baisse (Dollar faible)" if dxy_baisse else "hausse (Dollar fort)"
        print("DXY M5 : Close=" + str(round(dxy_close, 3)) + " Open=" + str(round(dxy_open_val, 3)) + " => " + dxy_tendance, flush=True)
        if direction == "BUY" and not dxy_baisse:
            print("DXY non baissier - BUY Gold annule", flush=True)
            return False, dxy_close, dxy_tendance
        if direction == "SELL" and not dxy_hausse:
            print("DXY non haussier - SELL Gold annule", flush=True)
            return False, dxy_close, dxy_tendance
        print("DXY OK pour " + direction, flush=True)
        return True, dxy_close, dxy_tendance
    except Exception as e:
        print("Erreur DXY : " + str(e) + " - filtre bypasse", flush=True)
        return True, 0.0, "N/A"

# ============================================================
#  ANALYSE PRINCIPALE
# ============================================================

def analyse_market():

    # === CHARGEMENT 4 TIMEFRAMES ===
    df_m5     = get_data("XAUUSD=X", "5m",  "5d")
    df_m15    = get_data("XAUUSD=X", "15m", "10d")
    df_h1     = get_data("XAUUSD=X", "1h",  "30d")
    df_h1_raw = get_data("XAUUSD=X", "1h",  "60d")

    for name, df, min_b in [("M5", df_m5, 55), ("M15", df_m15, 55), ("H1", df_h1, 55)]:
        if df is None or len(df) < min_b:
            print(name + " insuffisant : " + str(len(df) if df is not None else 0), flush=True)
            return None
    if df_h1_raw is None or len(df_h1_raw) < 55:
        print("H1 raw insuffisant pour H4", flush=True)
        return None

    df_h4 = df_h1_raw.resample("4h").agg({
        "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"
    }).dropna()
    if len(df_h4) < 55:
        print("H4 insuffisant : " + str(len(df_h4)), flush=True)
        return None

    # ============================================================
    #  ETAPE 1 : EMA 20/50 SUR 4 TF
    # ============================================================

    bias_m5  = ema_bias(df_m5)
    bias_m15 = ema_bias(df_m15)
    bias_h1  = ema_bias(df_h1)
    bias_h4  = ema_bias(df_h4)
    print("EMA Bias M5=" + str(bias_m5) + " M15=" + str(bias_m15) + " H1=" + str(bias_h1) + " H4=" + str(bias_h4), flush=True)

    biases = [bias_m5, bias_m15, bias_h1, bias_h4]
    if 0 in biases:
        print("EMA non aligne sur au moins un TF - pas de signal", flush=True)
        return None
    all_bullish = all(b == 1  for b in biases)
    all_bearish = all(b == -1 for b in biases)
    if not all_bullish and not all_bearish:
        print("EMA non aligne sur les 4 TF - pas de signal", flush=True)
        return None
    direction = "BUY" if all_bullish else "SELL"

    # ============================================================
    #  ETAPE 2 : BOUGIE M5 CLOTUREE (iloc[-2])
    # ============================================================

    close_m5 = df_m5["Close"].squeeze()
    open_m5  = df_m5["Open"].squeeze()
    high_m5  = df_m5["High"].squeeze()
    low_m5   = df_m5["Low"].squeeze()
    p = float(close_m5.iloc[-2])
    o = float(open_m5.iloc[-2])
    h = float(high_m5.iloc[-2])
    l = float(low_m5.iloc[-2])

    # ============================================================
    #  ETAPE 3 : ANTI-PANIQUE (2.5 x ATR)
    # ============================================================

    atr_val     = float(calc_atr(df_m5, ATR_PERIOD).iloc[-2])
    candle_size = h - l
    if candle_size > ATR_SPIKE_MULT * atr_val:
        print("Bougie de panique : taille=" + str(round(candle_size, 2)) + " seuil=" + str(round(ATR_SPIKE_MULT * atr_val, 2)), flush=True)
        return None

    # ============================================================
    #  ETAPE 4 : SL DYNAMIQUE + CASCADE (1 pip XAU = 0.10$)
    # ============================================================

    sl_raw  = SL_ATR_MULT * atr_val
    sl_pips = sl_raw * 10
    if sl_pips < SL_MIN_PIPS:
        sl_pips = SL_MIN_PIPS
        print("SL force a " + str(SL_MIN_PIPS) + " pips (minimum)", flush=True)
    if sl_pips > SL_MAX_PIPS:
        print("SL trop grand (" + str(round(sl_pips, 1)) + " pips) - signal annule", flush=True)
        return None
    sl_pts = sl_pips / 10.0
    tp_pts = sl_pts * TP_RR

    # ============================================================
    #  ETAPE 5 : VOLUME HYBRIDE GC=F > XAUUSD=X > Skip
    # ============================================================

    vol_ok, vol_source, _, _ = check_volume()
    if not vol_ok:
        return None

    # ============================================================
    #  ETAPE 6 : RSI M5 (< 70 BUY / > 30 SELL)
    # ============================================================

    rsi_val = float(calc_rsi(close_m5, 14).iloc[-2])
    print("RSI M5 = " + str(round(rsi_val, 1)), flush=True)
    if direction == "BUY" and rsi_val >= 70:
        print("RSI sur-achete (" + str(round(rsi_val, 1)) + ") - BUY annule", flush=True)
        return None
    if direction == "SELL" and rsi_val <= 30:
        print("RSI sur-vendu (" + str(round(rsi_val, 1)) + ") - SELL annule", flush=True)
        return None
    print("RSI OK pour " + direction, flush=True)

    # ============================================================
    #  ETAPE 7 : FILTRE DXY (correlation inverse XAU/USD)
    # ============================================================

    dxy_ok, dxy_close, dxy_tendance = check_dxy(direction)
    if not dxy_ok:
        return None

    # ============================================================
    #  ETAPE 8 : COULEUR BOUGIE M5
    # ============================================================

    if direction == "BUY" and p <= o:
        print("Bougie M5 non verte - BUY annule", flush=True)
        return None
    if direction == "SELL" and p >= o:
        print("Bougie M5 non rouge - SELL annule", flush=True)
        return None

    # ============================================================
    #  SIGNAL VALIDE
    # ============================================================

    if direction == "BUY":
        sl = round(p - sl_pts, 2)
        tp = round(p + tp_pts, 2)
    else:
        sl = round(p + sl_pts, 2)
        tp = round(p - tp_pts, 2)

    print("SIGNAL VALIDE : " + direction + " Prix=" + str(round(p, 2)) + " RSI=" + str(round(rsi_val, 1)) + " DXY=" + str(round(dxy_close, 3)) + " SL=" + str(round(sl_pips, 1)) + "pips", flush=True)

    return {
        "dir":         direction,
        "p":           round(p, 2),
        "sl":          sl,
        "tp":          tp,
        "sl_pips":     round(sl_pips, 1),
        "tp_pips":     round(sl_pips * TP_RR, 1),
        "rsi":         round(rsi_val, 1),
        "dxy_close":   round(dxy_close, 3),
        "dxy_tendance": dxy_tendance,
    }

# ============================================================
#  BOUCLE DE TRADING
# ============================================================

def wait_for_candle_close():
    now     = datetime.now(PARIS_TZ)
    seconds = now.second + (now.minute % 5) * 60
    wait    = 300 - seconds
    if wait <= 2:
        wait += 300
    print("Prochaine fermeture M5 dans " + str(wait) + "s", flush=True)
    time.sleep(wait)

def trading_loop():
    print("Boucle XAU/USD Sniper demarree", flush=True)
    while True:
        try:
            wait_for_candle_close()
            now_str = datetime.now(PARIS_TZ).strftime("%H:%M")
            print("[" + now_str + "] Bougie M5 fermee - verification...", flush=True)
            if is_in_session():
                print("[" + now_str + "] Session active - analyse en cours", flush=True)
                signal = analyse_market()
                if signal:
                    direction = "ACHAT" if signal["dir"] == "BUY" else "VENTE"
                    msg = ("XAU/USD SNIPER - " + direction + "\n"
                           + "Entree : " + str(signal["p"]) + "\n"
                           + "Stop   : " + str(signal["sl"]) + " (" + str(signal["sl_pips"]) + " pips)\n"
                           + "Cible  : " + str(signal["tp"]) + " (" + str(signal["tp_pips"]) + " pips)\n"
                           + "RR     : 1:" + str(TP_RR) + "\n"
                           + "RSI M5 : " + str(signal["rsi"]) + "\n"
                           + "DXY    : " + str(signal["dxy_close"]) + " (" + signal["dxy_tendance"] + ")\n"
                           + "Lot    : " + str(LOT_SIZE) + "\n"
                           + "MTF    : M5+M15+H1+H4 EMA alignes")
                    bot.send_message(CHAT_ID, msg)
                    print("[" + now_str + "] Signal envoye : " + signal["dir"], flush=True)
                else:
                    print("[" + now_str + "] Pas de signal", flush=True)
            else:
                print("[" + now_str + "] Hors session", flush=True)
        except Exception as e:
            print("ERREUR : " + str(e), flush=True)
            time.sleep(60)

# ============================================================
#  LANCEMENT
# ============================================================

if __name__ == "__main__":
    print("Demarrage bot XAU/USD Sniper...", flush=True)
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    time.sleep(2)
    print("Flask actif - lancement boucle trading", flush=True)
    now_start = datetime.now(PARIS_TZ).strftime("%d/%m/%Y a %H:%M")
    msg_start = ("XAU/USD SNIPER demarre\n"
                 + "Date : " + now_start + "\n"
                 + "Strategie : EMA 20/50 sur M5+M15+H1+H4\n"
                 + "Filtre DXY : correlation inverse actif\n"
                 + "Volume : GC=F > XAUUSD=X > Skip\n"
                 + "SL : 1.5x ATR (min " + str(SL_MIN_PIPS) + " / max " + str(SL_MAX_PIPS) + " pips)\n"
                 + "TP : RR 1:" + str(TP_RR) + "\n"
                 + "Filtres : Anti-panique + RSI + DXY + Couleur\n"
                 + "Sessions : 08h-12h et 14h15-18h\n"
                 + "Statut : En attente de signal...")
    try:
        bot.send_message(CHAT_ID, msg_start)
        print("Message demarrage envoye", flush=True)
    except Exception as e:
        print("Erreur message demarrage : " + str(e), flush=True)
    trading_loop()