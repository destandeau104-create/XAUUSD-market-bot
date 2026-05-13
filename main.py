import os, sys
import pandas as pd
import numpy as np
import yfinance as yf
import requests
from datetime import datetime
import pytz

# ============================================================
#  CONFIGURATION
# ============================================================

TOKEN   = os.getenv("TELEGRAM_TOKEN", "8218163213:AAEDXu19mXfeUSM65JIZAiBucxUAxmRHwy4")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "1432682636")

PARIS_TZ       = pytz.timezone("Europe/Paris")
ATR_PERIOD     = 14
ATR_SPIKE_MULT = 2.5
SL_ATR_MULT    = 1.5
SL_MIN_PIPS    = 15.0
SL_MAX_PIPS    = 100.0
TP_RR          = 2.0
RETEST_THRESH  = 0.35   # ecart max prix vs EMA20 pour un retest
VOL_THRESHOLD  = 0.80   # volume signal > 80% moyenne 20 bougies
STOCH_OB       = 80     # DXY stochastique surachete
STOCH_OS       = 20     # DXY stochastique survendu

# ============================================================
#  TELEGRAM
# ============================================================

def send_telegram(msg):
    try:
        url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage"
        r   = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=10)
        if r.status_code == 200:
            print("Telegram OK", flush=True)
        else:
            print("Telegram erreur : " + str(r.status_code), flush=True)
    except Exception as e:
        print("Telegram exception : " + str(e), flush=True)

# ============================================================
#  SESSION PARIS
# ============================================================

def is_in_session():
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    wd   = now.weekday()
    if wd >= 5:
        print("Weekend - marche ferme", flush=True)
        return False
    matin = (h, m) >= (9, 0) and (h, m) <= (13, 0)
    aprem = (h, m) >= (14, 30) and (h, m) <= (19, 0)
    return matin or aprem

def get_session_label():
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    if (h, m) >= (9, 0) and (h, m) <= (13, 0):   return "Matin 09h-13h"
    if (h, m) >= (14, 30) and (h, m) <= (19, 0): return "Apres-midi 14h30-19h"
    return "Hors session"

# ============================================================
#  INDICATEURS - calculs natifs identiques MetaTrader
# ============================================================

def calc_ema(series, length):
    """EMA Wilder - identique MetaTrader."""
    return series.ewm(span=length, min_periods=length, adjust=False).mean()

def calc_atr(df, period=14):
    """ATR Wilder - identique MetaTrader."""
    h = df["High"]
    l = df["Low"]
    c = df["Close"]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, min_periods=period, adjust=False).mean()

def calc_stochastic(df, k_period=14, d_period=3):
    """
    Stochastique %K et %D.
    Retourne (K_actuel, D_actuel)
    """
    high  = df["High"]
    low   = df["Low"]
    close = df["Close"]
    lowest_low   = low.rolling(k_period).min()
    highest_high = high.rolling(k_period).max()
    denom = (highest_high - lowest_low).replace(0, np.nan)
    k     = 100 * (close - lowest_low) / denom
    d     = k.rolling(d_period).mean()
    return float(k.iloc[-2]), float(d.iloc[-2])

def calc_ema200_h4_from_h1(df_h1):
    """
    EMA 200 H4 reconstruite depuis H1.
    H4 = resample 4h depuis H1.
    """
    df_h4 = df_h1.resample("4h").agg({
        "Open": "first", "High": "max",
        "Low":  "min",   "Close": "last", "Volume": "sum"
    }).dropna()
    return calc_ema(df_h4["Close"], 200)

# ============================================================
#  CHARGEMENT DONNEES - nettoyage Multi-Index yfinance
# ============================================================

def fetch(ticker, interval, period):
    """
    Charge les donnees yfinance et nettoie le Multi-Index.
    CRITIQUE : yfinance retourne parfois un Multi-Index sur les colonnes.
    df.columns.get_level_values(0) supprime le niveau ticker.
    """
    try:
        df = yf.download(
            ticker,
            interval=interval,
            period=period,
            progress=False,
            auto_adjust=True
        )
        if df is None or df.empty:
            print("fetch " + ticker + " vide", flush=True)
            return None
        # Nettoyage Multi-Index
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        else:
            df.columns = df.columns
        # Supprime doublons de colonnes
        df = df.loc[:, ~df.columns.duplicated()]
        return df
    except Exception as e:
        print("fetch " + ticker + " erreur : " + str(e), flush=True)
        return None

# ============================================================
#  ANALYSE - ONE SHOT
# ============================================================

def analyse():
    now_paris = datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M")
    print("Analyse Gold v4.3 - " + now_paris, flush=True)

    # --- Chargement des donnees
    df_gcf_h1  = fetch("GC=F",     "1h",  "60d")
    df_gcf_m5  = fetch("GC=F",     "5m",  "5d")
    df_dxy_h1  = fetch("DX-Y.NYB", "1h",  "30d")

    for name, df in [("GC=F H1", df_gcf_h1), ("GC=F M5", df_gcf_m5), ("DXY H1", df_dxy_h1)]:
        if df is None or len(df) < 30:
            print(name + " insuffisant", flush=True)
            return None

    # --- EMA 200 H4 (tendance maitre)
    ema200_h4_series = calc_ema200_h4_from_h1(df_gcf_h1)
    if len(ema200_h4_series) < 5:
        print("EMA200 H4 insuffisante", flush=True)
        return None
    ema200_h4 = float(ema200_h4_series.iloc[-1])

    # --- Donnees bougie M5 cloturee (iloc[-2])
    price  = float(df_gcf_m5["Close"].iloc[-2])
    open_  = float(df_gcf_m5["Open"].iloc[-2])
    high_  = float(df_gcf_m5["High"].iloc[-2])
    low_   = float(df_gcf_m5["Low"].iloc[-2])
    volume = float(df_gcf_m5["Volume"].iloc[-2]) if "Volume" in df_gcf_m5.columns else 0

    # --- EMA 20 M5 (signal retest)
    ema20_m5 = float(calc_ema(df_gcf_m5["Close"], 20).iloc[-2])

    # --- ATR M5
    atr = float(calc_atr(df_gcf_m5).iloc[-2])

    # --- Volume M5 (moyenne 20 bougies)
    vol_avg20 = float(df_gcf_m5["Volume"].iloc[-22:-2].mean()) if "Volume" in df_gcf_m5.columns else 0

    # --- Stochastique DXY H1
    stoch_k, stoch_d = calc_stochastic(df_dxy_h1)

    print("Prix GC=F     : " + str(round(price, 2)), flush=True)
    print("EMA 200 H4    : " + str(round(ema200_h4, 2)), flush=True)
    print("EMA 20 M5     : " + str(round(ema20_m5, 2)), flush=True)
    print("Ecart retest  : " + str(round(abs(price - ema20_m5), 3)), flush=True)
    print("ATR M5        : " + str(round(atr, 2)), flush=True)
    print("Volume        : " + str(round(volume, 0)) + " (seuil " + str(round(vol_avg20 * VOL_THRESHOLD, 0)) + ")", flush=True)
    print("DXY Stoch K   : " + str(round(stoch_k, 1)) + " D : " + str(round(stoch_d, 1)), flush=True)

    # ============================================================
    #  FILTRES
    # ============================================================

    # FILTRE 1 : Tendance H4 EMA 200
    if price > ema200_h4:
        direction = "BUY"
    elif price < ema200_h4:
        direction = "SELL"
    else:
        print("Prix sur EMA200 H4 - pas de direction", flush=True)
        return None
    print("Tendance H4   : " + direction, flush=True)

    # FILTRE 2 : Retest EMA 20 M5 (ecart < 0.35 point)
    ecart = abs(price - ema20_m5)
    if ecart > RETEST_THRESH:
        print("Pas de retest EMA20 M5 - ecart " + str(round(ecart, 3)) + " > " + str(RETEST_THRESH), flush=True)
        return None
    print("Retest EMA20 M5 OK - ecart " + str(round(ecart, 3)), flush=True)

    # FILTRE 3 : Anti-panique ATR
    candle_size = high_ - low_
    if candle_size > ATR_SPIKE_MULT * atr:
        print("Bougie de panique - annule", flush=True)
        return None

    # FILTRE 4 : Volume > 80% moyenne 20 bougies
    if vol_avg20 > 0 and volume < vol_avg20 * VOL_THRESHOLD:
        print("Volume insuffisant - " + str(round(volume,0)) + " < " + str(round(vol_avg20 * VOL_THRESHOLD, 0)), flush=True)
        return None
    print("Volume OK", flush=True)

    # FILTRE 5 : Stochastique DXY
    if direction == "BUY" and stoch_k > STOCH_OB:
        print("DXY Stoch surachete (" + str(round(stoch_k,1)) + ") - BUY Gold annule", flush=True)
        return None
    if direction == "SELL" and stoch_k < STOCH_OS:
        print("DXY Stoch survendu (" + str(round(stoch_k,1)) + ") - SELL Gold annule", flush=True)
        return None
    print("DXY Stoch OK pour " + direction, flush=True)

    # FILTRE 6 : Couleur bougie M5 dans le sens du trade
    if direction == "BUY" and price <= open_:
        print("Bougie M5 non verte - BUY annule", flush=True)
        return None
    if direction == "SELL" and price >= open_:
        print("Bougie M5 non rouge - SELL annule", flush=True)
        return None

    # ============================================================
    #  CALCUL SL / TP
    # ============================================================

    sl_pips = atr * SL_ATR_MULT * 10   # 1 pip XAU = 0.10$
    if sl_pips < SL_MIN_PIPS: sl_pips = SL_MIN_PIPS
    if sl_pips > SL_MAX_PIPS:
        print("SL trop grand (" + str(round(sl_pips,1)) + " pips) - annule", flush=True)
        return None
    sl_pts = sl_pips / 10.0
    tp_pts = sl_pts * TP_RR

    if direction == "BUY":
        sl = round(price - sl_pts, 2)
        tp = round(price + tp_pts, 2)
    else:
        sl = round(price + sl_pts, 2)
        tp = round(price - tp_pts, 2)

    print("SIGNAL VALIDE : " + direction + " @ " + str(round(price,2)), flush=True)

    return {
        "direction": direction,
        "price":     round(price, 2),
        "sl":        sl,
        "tp":        tp,
        "sl_pips":   round(sl_pips, 1),
        "tp_pips":   round(sl_pips * TP_RR, 1),
        "ema200_h4": round(ema200_h4, 2),
        "ema20_m5":  round(ema20_m5, 2),
        "ecart":     round(ecart, 3),
        "stoch_k":   round(stoch_k, 1),
        "stoch_d":   round(stoch_d, 1),
        "atr":       round(atr, 2),
        "session":   get_session_label(),
    }

# ============================================================
#  MAIN - ONE SHOT (pas de while True)
# ============================================================

def main():
    print("=" * 50, flush=True)
    print("Gold Sniper v4.3 - GitHub Actions", flush=True)
    print(datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M:%S"), flush=True)
    print("=" * 50, flush=True)

    # Verification session
    if not is_in_session():
        now  = datetime.now(PARIS_TZ)
        h, m = now.hour, now.minute
        print("Hors session (" + str(h) + "h" + str(m).zfill(2) + " Paris) - fin du script", flush=True)
        sys.exit(0)

    print("Session active : " + get_session_label(), flush=True)

    # Analyse unique
    signal = analyse()

    if signal is None:
        print("Pas de signal - fin du script", flush=True)
        sys.exit(0)

    # Signal detecte - envoi Telegram
    direction = "ACHAT" if signal["direction"] == "BUY" else "VENTE"
    emoji     = "XAU/USD v4.3"
    msg = (
        emoji + " - " + direction + "\n"
        + "Entree  : " + str(signal["price"]) + "\n"
        + "Stop    : " + str(signal["sl"]) + " (" + str(signal["sl_pips"]) + " pips)\n"
        + "Cible   : " + str(signal["tp"]) + " (" + str(signal["tp_pips"]) + " pips)\n"
        + "RR      : 1:" + str(TP_RR) + "\n"
        + "EMA200 H4 : " + str(signal["ema200_h4"]) + "\n"
        + "EMA20 M5  : " + str(signal["ema20_m5"]) + " (ecart " + str(signal["ecart"]) + ")\n"
        + "DXY Stoch : K=" + str(signal["stoch_k"]) + " D=" + str(signal["stoch_d"]) + "\n"
        + "ATR M5    : " + str(signal["atr"]) + "\n"
        + "Session   : " + signal["session"]
    )

    send_telegram(msg)
    print("Signal envoye - fin du script", flush=True)
    sys.exit(0)

if __name__ == "__main__":
    main()