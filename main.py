import os, time, threading, gc
from datetime import datetime
import pytz, pandas as pd, numpy as np, yfinance as yf
import telebot
from flask import Flask

# ============================================================
#  CONFIGURATION
# ============================================================

TOKEN   = os.getenv("TELEGRAM_TOKEN",   "8218163213:AAEDXu19mXfeUSM65JIZAiBucxUAxmRHwy4")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "1432682636")

try:
    bot = telebot.TeleBot(TOKEN)
    print("Telebot initialise", flush=True)
except Exception as e:
    print("Telebot erreur init : " + str(e), flush=True)
    bot = None

PARIS_TZ       = pytz.timezone("Europe/Paris")
LOT_SIZE       = 0.50
ATR_PERIOD     = 14
ATR_SPIKE_MULT = 2.5
SL_ATR_MULT    = 1.5
SL_MIN_PIPS    = 15.0
SL_MAX_PIPS    = 150.0
TP_RR          = 2.0
COOLDOWN_MIN   = 30
RETEST_THRESH  = 1.50
VOL_THRESHOLD  = 0.80
STOCH_OB       = 80
STOCH_OS       = 20

_last_signal_dir  = None
_last_signal_time = None

# ============================================================
#  FLASK KEEP-ALIVE
# ============================================================

app = Flask(__name__)

@app.route("/")
def home(): return "XAU/USD Sniper v4.3 actif", 200

@app.route("/health")
def health(): return "OK " + datetime.now(PARIS_TZ).strftime("%H:%M:%S"), 200

def run_flask():
    print("Flask demarre sur 0.0.0.0:8080", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)

# ============================================================
#  TELEGRAM
# ============================================================

def send_msg(msg):
    if not bot or not CHAT_ID:
        print("Telegram non configure", flush=True)
        return
    try:
        bot.send_message(CHAT_ID, msg)
        print("Telegram OK", flush=True)
    except Exception as e:
        print("Telegram erreur : " + str(e), flush=True)

# ============================================================
#  INDICATEURS - identiques MetaTrader
# ============================================================

def calc_ema(series, length):
    """EMA Wilder - identique MetaTrader."""
    return series.ewm(span=length, min_periods=length, adjust=False).mean()

def calc_rsi(series, length=14):
    """RSI Wilder - identique MetaTrader."""
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_g = gain.ewm(com=length - 1, min_periods=length, adjust=False).mean()
    avg_l = loss.ewm(com=length - 1, min_periods=length, adjust=False).mean()
    rs    = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    """ATR Wilder - identique MetaTrader."""
    h = df["High"].squeeze()
    l = df["Low"].squeeze()
    c = df["Close"].squeeze()
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, min_periods=period, adjust=False).mean()

def calc_stochastic(df, k_period=14, d_period=3):
    """Stochastique (14,3,3) - identique MetaTrader."""
    h = df["High"].squeeze()
    l = df["Low"].squeeze()
    c = df["Close"].squeeze()
    lowest  = l.rolling(k_period).min()
    highest = h.rolling(k_period).max()
    denom   = (highest - lowest).replace(0, np.nan)
    k       = 100 * (c - lowest) / denom
    d       = k.rolling(d_period).mean()
    k_val   = float(k.iloc[-2])
    d_val   = float(d.iloc[-2])
    return k_val, d_val

def ema_bias(df, label=""):
    """
    Biais EMA 20/50 sur bougie cloturee (iloc[-2]).
    +1 bullish | -1 bearish | 0 neutre
    """
    try:
        if df is None or len(df) < 55: return 0
        c     = df["Close"].squeeze()
        price = float(c.iloc[-2])
        e20   = float(calc_ema(c, 20).iloc[-2])
        e50   = float(calc_ema(c, 50).iloc[-2])
        if price > e20 and price > e50: return 1
        if price < e20 and price < e50: return -1
        return 0
    except Exception as e:
        print("ema_bias " + label + " : " + str(e), flush=True)
        return 0

# ============================================================
#  SESSION + MARCHE OUVERT
# ============================================================

def is_market_open():
    """Bloque le weekend (Or ferme sam + dim avant 22h UTC)."""
    now = datetime.now(pytz.utc)
    wd  = now.weekday()
    if wd == 5: return False
    if wd == 6 and now.hour < 22: return False
    return True

def is_in_session():
    """Sessions de trading Paris : 09h-13h et 14h30-19h."""
    if not is_market_open(): return False
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    matin = (h, m) >= (8, 0) and (h, m) <= (13, 0)
    aprem = (h, m) >= (14, 30) and (h, m) <= (19, 0)
    return matin or aprem

def get_session_label():
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    if (h, m) >= (8, 0) and (h, m) <= (13, 0):    return "Matin 08h-13h"
    if (h, m) >= (14, 30) and (h, m) <= (19, 0):  return "Apres-midi 14h30-19h"
    return "Hors session"

# ============================================================
#  ANTI-DOUBLON
# ============================================================

def is_signal_allowed(direction):
    global _last_signal_dir, _last_signal_time
    now = datetime.now(PARIS_TZ)
    if _last_signal_time is not None:
        elapsed = (now - _last_signal_time).total_seconds() / 60
        if direction == _last_signal_dir and elapsed < COOLDOWN_MIN:
            print("Doublon signal - cooldown " + str(round(elapsed,1)) + "min", flush=True)
            return False
    return True

def register_signal(direction):
    global _last_signal_dir, _last_signal_time
    _last_signal_dir  = direction
    _last_signal_time = datetime.now(PARIS_TZ)

# ============================================================
#  DONNEES - fetch robuste avec retry + fix Multi-Index
# ============================================================

def get_data(ticker, interval, period, retries=3):
    """Charge yfinance avec retry et nettoyage Multi-Index."""
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(ticker, interval=interval, period=period,
                             progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.loc[:, ~df.columns.duplicated()]
                return df
            print("get_data " + ticker + " vide tentative " + str(attempt), flush=True)
        except Exception as e:
            print("get_data " + ticker + " err " + str(attempt) + " : " + str(e), flush=True)
        if attempt < retries: time.sleep(5)
    return pd.DataFrame()

# ============================================================
#  VOLUME HYBRIDE GC=F > XAUUSD=X > Skip
# ============================================================

def get_vol(ticker):
    try:
        df = yf.download(ticker, interval="5m", period="2d",
                         progress=False, auto_adjust=True)
        if df is None or df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        if "Volume" not in df.columns: return None
        vol = df["Volume"].squeeze()
        if len(vol) < 17: return None
        if float(vol.iloc[-2]) == 0 and float(vol.iloc[-17:-2].mean()) == 0: return None
        return vol
    except Exception as e:
        print("get_vol " + ticker + " : " + str(e), flush=True)
        return None

def check_volume():
    for ticker, label in [("GC=F","Futures (GC=F)"),("XAUUSD=X","Spot (XAUUSD=X)")]:
        vol = get_vol(ticker)
        if vol is not None:
            sig = float(vol.iloc[-2])
            avg = float(vol.iloc[-17:-2].mean())
            print("Volume " + label + " sig=" + str(round(sig,0)) + " avg=" + str(round(avg,0)), flush=True)
            if avg > 0 and sig < avg * VOL_THRESHOLD:
                print("Volume insuffisant - annule", flush=True)
                return False, label
            print("Volume OK", flush=True)
            return True, label
    print("Volume Skip - autorisation exceptionnelle", flush=True)
    return True, "Skip"

# ============================================================
#  FILTRE DXY STOCHASTIQUE (14,3,3) sur H1
#  BUY Or  : DXY Stoch K < 80 (pas en surachat)
#  SELL Or : DXY Stoch K > 20 (pas en survente)
# ============================================================

def check_dxy_stoch(direction):
    """
    Filtre DXY via Stochastique H1.
    Ne bloque pas si DXY indisponible (bypasse).
    """
    try:
        df_dxy = get_data("DX-Y.NYB", "1h", "10d")
        if df_dxy is None or df_dxy.empty or len(df_dxy) < 20:
            print("DXY indisponible - bypasse", flush=True)
            return True, 0.0, "N/A"
        k, d = calc_stochastic(df_dxy)
        print("DXY Stoch K=" + str(round(k,1)) + " D=" + str(round(d,1)), flush=True)
        if direction == "BUY" and k > STOCH_OB:
            print("DXY surachete (" + str(round(k,1)) + ">80) - BUY Or annule", flush=True)
            return False, k, "surachete"
        if direction == "SELL" and k < STOCH_OS:
            print("DXY survendu (" + str(round(k,1)) + "<20) - SELL Or annule", flush=True)
            return False, k, "survendu"
        tendance = "neutre"
        if k > 50: tendance = "haussier"
        if k < 50: tendance = "baissier"
        print("DXY Stoch OK pour " + direction + " - DXY " + tendance, flush=True)
        return True, k, tendance
    except Exception as e:
        print("DXY Stoch erreur : " + str(e) + " - bypasse", flush=True)
        return True, 0.0, "N/A"

# ============================================================
#  ANALYSE - Pipeline V4.3
#  ETAPE 1 : EMA 200 H1 (tendance maitre)
#  ETAPE 2 : Alignement EMA 20/50 sur H4, H1, M15, M5
#  ETAPE 3 : Retest EMA20 M5 (ecart < 0.35)
#  ETAPE 4 : Anti-panique ATR
#  ETAPE 5 : SL cascade
#  ETAPE 6 : Volume hybride GC=F
#  ETAPE 7 : RSI M5
#  ETAPE 8 : DXY Stochastique H1
#  ETAPE 9 : Couleur bougie M5
# ============================================================

def get_price_data(ticker_primary, ticker_fallback, interval, period):
    """
    Fetch avec cascade : essaie ticker_primary d'abord,
    bascule sur ticker_fallback si echec.
    """
    df = get_data(ticker_primary, interval, period)
    if df is not None and not df.empty:
        print("Source prix : " + ticker_primary, flush=True)
        return df
    print("Fallback vers " + ticker_fallback, flush=True)
    df = get_data(ticker_fallback, interval, period)
    if df is not None and not df.empty:
        print("Source prix : " + ticker_fallback, flush=True)
        return df
    return pd.DataFrame()

def analyse_market():
    try:
        # Chargement donnees - cascade XAUUSD=X -> GC=F
        df_h1_raw = get_price_data("XAUUSD=X", "GC=F", "1h", "60d")
        df_m5     = get_price_data("XAUUSD=X", "GC=F", "5m", "5d")
        df_m15    = get_price_data("XAUUSD=X", "GC=F", "15m","10d")

        for name, df, n in [("M5",df_m5,55),("M15",df_m15,55),("H1raw",df_h1_raw,200)]:
            if df is None or len(df) < n:
                print(name + " insuffisant (" + str(0 if df is None else len(df)) + ")", flush=True)
                return None

        df_h1 = df_h1_raw.tail(720)
        df_h4 = df_h1_raw.resample("4h").agg(
            {"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}
        ).dropna()
        if len(df_h4) < 55:
            print("H4 insuffisant", flush=True)
            return None

        # ETAPE 1 : EMA 200 H1 - tendance maitre
        ema200_h1 = float(calc_ema(df_h1["Close"].squeeze(), 200).iloc[-2])
        price_h1  = float(df_h1["Close"].squeeze().iloc[-2])
        if price_h1 > ema200_h1:
            direction = "BUY"
        elif price_h1 < ema200_h1:
            direction = "SELL"
        else:
            print("Prix sur EMA200 H1 - neutre", flush=True)
            return None
        print("EMA200 H1 : " + direction + " (prix=" + str(round(price_h1,2)) + " ema=" + str(round(ema200_h1,2)) + ")", flush=True)

        # ETAPE 2 : Anti-doublon cooldown
        if not is_signal_allowed(direction):
            return None

        # ETAPE 3 : Alignement EMA 20/50 sur H4, H1, M15, M5
        for name, df in [("H4",df_h4),("H1",df_h1),("M15",df_m15),("M5",df_m5)]:
            b = ema_bias(df, name)
            if b == 0:
                print(name + " EMA neutre - stop", flush=True)
                return None
            expected = 1 if direction == "BUY" else -1
            if b != expected:
                print(name + " EMA contre tendance - stop", flush=True)
                return None
        print("EMA 20/50 alignes sur H4+H1+M15+M5", flush=True)

        # Valeurs bougie M5 cloturee (iloc[-2] strict)
        c_m5 = df_m5["Close"].squeeze()
        o_m5 = df_m5["Open"].squeeze()
        h_m5 = df_m5["High"].squeeze()
        l_m5 = df_m5["Low"].squeeze()
        p = float(c_m5.iloc[-2])
        o = float(o_m5.iloc[-2])
        h = float(h_m5.iloc[-2])
        l = float(l_m5.iloc[-2])

        # ETAPE 4 : Retest EMA20 M5 (signal sniper)
        ema20_m5 = float(calc_ema(c_m5, 20).iloc[-2])
        ecart    = abs(p - ema20_m5)
        print("Retest EMA20 M5 : prix=" + str(round(p,2)) + " ema20=" + str(round(ema20_m5,2)) + " ecart=" + str(round(ecart,3)), flush=True)
        if ecart > RETEST_THRESH:
            print("Ecart " + str(round(ecart,3)) + " > " + str(RETEST_THRESH) + " - pas de retest", flush=True)
            return None
        print("Retest EMA20 M5 OK", flush=True)

        # ETAPE 5 : Anti-panique ATR
        atr = float(calc_atr(df_m5, ATR_PERIOD).iloc[-2])
        if (h - l) > ATR_SPIKE_MULT * atr:
            print("Panique ATR - annule", flush=True)
            return None

        # ETAPE 6 : SL cascade (1 pip XAU = 0.10$)
        sl_pips = (SL_ATR_MULT * atr) * 10
        if sl_pips < SL_MIN_PIPS: sl_pips = SL_MIN_PIPS
        if sl_pips > SL_MAX_PIPS:
            print("SL trop grand - annule", flush=True)
            return None
        sl_pts = sl_pips / 10.0
        tp_pts = sl_pts * TP_RR

        # ETAPE 7 : Volume hybride GC=F > XAUUSD=X > Skip
        vol_ok, vol_src = check_volume()
        if not vol_ok: return None

        # ETAPE 8 : RSI M5
        rsi = float(calc_rsi(c_m5, 14).iloc[-2])
        print("RSI M5=" + str(round(rsi,1)), flush=True)
        if direction == "BUY" and rsi >= 70:
            print("RSI surachete - annule", flush=True); return None
        if direction == "SELL" and rsi <= 30:
            print("RSI survendu - annule", flush=True); return None

        # ETAPE 9 : DXY Stochastique H1
        dxy_ok, dxy_k, dxy_t = check_dxy_stoch(direction)
        if not dxy_ok: return None

        # ETAPE 10 : Couleur bougie M5
        if direction == "BUY" and p <= o:
            print("Bougie M5 non verte - annule", flush=True); return None
        if direction == "SELL" and p >= o:
            print("Bougie M5 non rouge - annule", flush=True); return None

        # Signal valide
        sl = round(p - sl_pts, 2) if direction == "BUY" else round(p + sl_pts, 2)
        tp = round(p + tp_pts, 2) if direction == "BUY" else round(p - tp_pts, 2)
        register_signal(direction)
        gc.collect()
        print("SIGNAL VALIDE " + direction + " @ " + str(round(p,2)) + " SL=" + str(round(sl_pips,1)) + "pips", flush=True)

        return {
            "dir":       direction,
            "p":         round(p, 2),
            "sl":        sl,
            "tp":        tp,
            "sl_pips":   round(sl_pips, 1),
            "tp_pips":   round(sl_pips * TP_RR, 1),
            "rsi":       round(rsi, 1),
            "ema200":    round(ema200_h1, 2),
            "ema20_m5":  round(ema20_m5, 2),
            "ecart":     round(ecart, 3),
            "dxy_k":     round(dxy_k, 1),
            "dxy_t":     dxy_t,
            "atr":       round(atr, 2),
            "session":   get_session_label(),
        }
    except Exception as e:
        print("analyse_market ERREUR : " + str(e), flush=True)
        return None

# ============================================================
#  BOUCLE DE TRADING (while True + time.sleep pour Render)
# ============================================================

def wait_for_candle_close():
    """Attend la fermeture exacte de la prochaine bougie M5."""
    now  = datetime.now(PARIS_TZ)
    wait = 300 - (now.second + (now.minute % 5) * 60)
    if wait <= 2: wait += 300
    print("Prochaine bougie M5 dans " + str(wait) + "s", flush=True)
    time.sleep(wait)

def trading_loop():
    print("Boucle XAU/USD Sniper v4.3 demarree", flush=True)
    while True:
        try:
            wait_for_candle_close()
            now_str = datetime.now(PARIS_TZ).strftime("%H:%M")
            if not is_market_open():
                print("[" + now_str + "] Weekend - marche ferme", flush=True)
                continue
            if is_in_session():
                print("[" + now_str + "] " + get_session_label() + " - analyse...", flush=True)
                s = analyse_market()
                if s:
                    d   = "ACHAT" if s["dir"] == "BUY" else "VENTE"
                    msg = ("XAU/USD SNIPER v4.3 - " + d + "\n"
                           + "Entree   : " + str(s["p"]) + "\n"
                           + "Stop     : " + str(s["sl"]) + " (" + str(s["sl_pips"]) + " pips)\n"
                           + "Cible    : " + str(s["tp"]) + " (" + str(s["tp_pips"]) + " pips)\n"
                           + "RR       : 1:" + str(TP_RR) + "\n"
                           + "EMA200H1 : " + str(s["ema200"]) + "\n"
                           + "EMA20 M5 : " + str(s["ema20_m5"]) + " (ecart " + str(s["ecart"]) + ")\n"
                           + "RSI M5   : " + str(s["rsi"]) + "\n"
                           + "DXY Stoch: K=" + str(s["dxy_k"]) + " (" + s["dxy_t"] + ")\n"
                           + "ATR M5   : " + str(s["atr"]) + "\n"
                           + "Session  : " + s["session"] + "\n"
                           + "Lot      : " + str(LOT_SIZE))
                    send_msg(msg)
                    print("[" + now_str + "] Signal envoye " + s["dir"], flush=True)
                else:
                    print("[" + now_str + "] Pas de signal", flush=True)
            else:
                print("[" + now_str + "] Hors session", flush=True)
        except Exception as e:
            print("BOUCLE ERREUR : " + str(e), flush=True)
            time.sleep(30)

# ============================================================
#  LANCEMENT - Flask + Trading en parallele
# ============================================================

if __name__ == "__main__":
    print("=" * 50, flush=True)
    print("XAU/USD Sniper v4.3 - Render", flush=True)
    print(datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M:%S"), flush=True)
    print("=" * 50, flush=True)

    # Flask dans un thread separe (daemon=True)
    threading.Thread(target=run_flask, daemon=True).start()
    time.sleep(2)
    print("Flask actif - lancement boucle trading", flush=True)

    # Message de demarrage Telegram
    now_s = datetime.now(PARIS_TZ).strftime("%d/%m/%Y a %H:%M")
    send_msg(
        "XAU/USD SNIPER v4.3 demarre\n"
        + "Date     : " + now_s + "\n"
        + "Tendance : EMA 200 H1\n"
        + "Signal   : Retest EMA20 M5 (ecart<0.35)\n"
        + "MTF      : H4+H1+M15+M5 EMA 20/50\n"
        + "DXY      : Stochastique (14,3,3) H1\n"
        + "Volume   : GC=F > XAUUSD=X > Skip\n"
        + "SL       : 1.5xATR (15-100 pips)\n"
        + "TP       : RR 1:2\n"
        + "Sessions : 09h-13h + 14h30-19h"
    )

    # Boucle trading (bloquante - garde Render actif)
    trading_loop()