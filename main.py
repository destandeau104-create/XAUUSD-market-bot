import os, time, threading, gc
from datetime import datetime
import pytz, pandas as pd, numpy as np, yfinance as yf, telebot
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
COOLDOWN_MIN   = 30   # minutes minimum entre deux signaux

# Anti-doublon : memorise le dernier signal envoye
_last_signal_dir  = None
_last_signal_time = None

app = Flask(__name__)

@app.route("/")
def home(): return "XAU/USD Sniper v4", 200

@app.route("/health")
def health(): return "OK " + datetime.now(PARIS_TZ).strftime("%H:%M:%S"), 200

def run_flask():
    print("Flask 0.0.0.0:8080", flush=True)
    app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)

# ============================================================
#  INDICATEURS - identiques MetaTrader
# ============================================================

def calc_ema(series, length):
    return series.ewm(span=length, min_periods=length, adjust=False).mean()

def calc_rsi(series, length=14):
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_g = gain.ewm(com=length - 1, min_periods=length, adjust=False).mean()
    avg_l = loss.ewm(com=length - 1, min_periods=length, adjust=False).mean()
    rs    = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    h = df["High"].squeeze()
    l = df["Low"].squeeze()
    c = df["Close"].squeeze()
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, min_periods=period, adjust=False).mean()

def ema_bias(df, label=""):
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
    """Filtre weekend + heures de fermeture Gold."""
    now = datetime.now(pytz.utc)
    wd  = now.weekday()  # 0=lundi 6=dimanche
    # Samedi 22h UTC -> Dimanche 22h UTC = marche ferme
    if wd == 5: return False  # Samedi
    if wd == 6 and now.hour < 22: return False  # Dimanche avant 22h UTC
    return True

def is_in_session():
    if not is_market_open(): return False
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    return ((h, m) >= (8, 0) and (h, m) <= (12, 0)) or ((h, m) >= (14, 0) and (h, m) <= (18, 0))

def get_session_label():
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    if (h, m) >= (8, 0) and (h, m) <= (12, 0): return "Session Londres"
    if (h, m) >= (14, 0) and (h, m) <= (18, 0): return "Session New York"
    return "Hors session"

# ============================================================
#  ANTI-DOUBLON DE SIGNAL
# ============================================================

def is_signal_allowed(direction):
    """
    Bloque un signal si :
    - Meme direction que le signal precedent ET
    - Moins de COOLDOWN_MIN minutes depuis le dernier signal
    """
    global _last_signal_dir, _last_signal_time
    now = datetime.now(PARIS_TZ)
    if _last_signal_time is not None:
        elapsed = (now - _last_signal_time).total_seconds() / 60
        if direction == _last_signal_dir and elapsed < COOLDOWN_MIN:
            print("Doublon signal " + direction + " - cooldown " + str(round(elapsed,1)) + "/" + str(COOLDOWN_MIN) + "min", flush=True)
            return False
    return True

def register_signal(direction):
    global _last_signal_dir, _last_signal_time
    _last_signal_dir  = direction
    _last_signal_time = datetime.now(PARIS_TZ)

# ============================================================
#  DONNEES - fetch robuste avec retry
# ============================================================

def get_data(ticker, interval, period, retries=3):
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(ticker, interval=interval, period=period,
                             progress=False, auto_adjust=True)
            if df is not None and not df.empty: return df
            print("get_data " + ticker + " vide tentative " + str(attempt), flush=True)
        except Exception as e:
            print("get_data " + ticker + " err " + str(attempt) + " : " + str(e), flush=True)
        if attempt < retries: time.sleep(5)
    return pd.DataFrame()

# ============================================================
#  VOLUME HYBRIDE  GC=F > XAUUSD=X > Skip
# ============================================================

def get_vol(ticker):
    try:
        df = yf.download(ticker, interval="5m", period="2d", progress=False, auto_adjust=True)
        if df is None or df.empty or "Volume" not in df.columns: return None
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
            if avg > 0 and sig <= avg:
                print("Volume insuffisant", flush=True)
                return False, label
            print("Volume OK", flush=True)
            return True, label
    print("Volume Skip", flush=True)
    return True, "Skip"

# ============================================================
#  FILTRE DXY - un seul fetch avec les deux TF
# ============================================================

def fetch_dxy_data():
    """Charge DXY M5 et H1 en une seule passe pour economiser les requetes."""
    m5 = get_data("DX-Y.NYB", "5m",  "2d")
    h1 = get_data("DX-Y.NYB", "1h", "10d")
    return m5, h1

def check_dxy(direction, dxy_m5, dxy_h1):
    """DXY securite absolue - passe dxy_m5 et dxy_h1 deja charges."""
    try:
        if dxy_m5 is None or dxy_m5.empty or len(dxy_m5) < 3:
            print("DXY indisponible - bypasse", flush=True)
            return True, 0.0, "N/A"
        close    = float(dxy_m5["Close"].squeeze().iloc[-2])
        open_val = float(dxy_m5["Open"].squeeze().iloc[-2])
        baisse   = close < open_val
        hausse   = close > open_val
        tendance = "baisse Dollar" if baisse else "hausse Dollar"
        # Confirmation EMA20 H1
        if dxy_h1 is not None and len(dxy_h1) >= 25:
            h1c      = dxy_h1["Close"].squeeze()
            ema20_v  = float(calc_ema(h1c, 20).iloc[-2])
            h1_price = float(h1c.iloc[-2])
            if direction == "BUY" and h1_price >= ema20_v:
                print("DXY H1 > EMA20 - BUY annule", flush=True)
                return False, close, tendance
            if direction == "SELL" and h1_price <= ema20_v:
                print("DXY H1 < EMA20 - SELL annule", flush=True)
                return False, close, tendance
            print("DXY EMA20 H1 OK", flush=True)
        if direction == "BUY" and not baisse:
            print("DXY M5 non baissier - BUY annule", flush=True)
            return False, close, tendance
        if direction == "SELL" and not hausse:
            print("DXY M5 non haussier - SELL annule", flush=True)
            return False, close, tendance
        print("DXY OK " + tendance, flush=True)
        return True, close, tendance
    except Exception as e:
        print("DXY erreur : " + str(e) + " bypasse", flush=True)
        return True, 0.0, "N/A"

# ============================================================
#  ANALYSE - H4 filtre MAITRE
# ============================================================

def analyse_market():
    try:
        # Un seul fetch H1 long pour couvrir H1 ET H4
        df_h1_raw = get_data("XAUUSD=X", "1h", "60d")
        df_m5     = get_data("XAUUSD=X", "5m", "5d")
        df_m15    = get_data("XAUUSD=X", "15m","10d")
        dxy_m5, dxy_h1 = fetch_dxy_data()

        for name, df, n in [("M5",df_m5,55),("M15",df_m15,55),("H1raw",df_h1_raw,55)]:
            if df is None or len(df) < n:
                print(name + " insuffisant", flush=True)
                return None

        # H1 = 30 derniers jours depuis df_h1_raw
        df_h1 = df_h1_raw.tail(720)  # ~30 jours de bougies 1H

        # H4 reconstruit filtre MAITRE
        df_h4 = df_h1_raw.resample("4h").agg(
            {"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}
        ).dropna()
        if len(df_h4) < 55:
            print("H4 insuffisant", flush=True)
            return None

        # ETAPE 1 : H4 filtre maitre - arret immediat si neutre
        bias_h4 = ema_bias(df_h4, "H4")
        if bias_h4 == 0:
            print("H4 neutre - stop", flush=True)
            return None
        direction = "BUY" if bias_h4 == 1 else "SELL"
        print("H4 MAITRE : " + direction, flush=True)

        # ETAPE 2 : Anti-doublon cooldown
        if not is_signal_allowed(direction):
            return None

        # ETAPE 3 : Alignement H1, M15, M5
        for name, df in [("H1",df_h1),("M15",df_m15),("M5",df_m5)]:
            b = ema_bias(df, name)
            if b == 0 or b != bias_h4:
                print(name + " non aligne - stop", flush=True)
                return None
        print("EMA 4 TF alignes", flush=True)

        # ETAPE 4 : Bougie M5 cloturee
        c_m5 = df_m5["Close"].squeeze()
        o_m5 = df_m5["Open"].squeeze()
        h_m5 = df_m5["High"].squeeze()
        l_m5 = df_m5["Low"].squeeze()
        p = float(c_m5.iloc[-2])
        o = float(o_m5.iloc[-2])
        h = float(h_m5.iloc[-2])
        l = float(l_m5.iloc[-2])

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

        # ETAPE 7 : Volume hybride
        vol_ok, vol_src = check_volume()
        if not vol_ok: return None

        # ETAPE 8 : RSI M5
        rsi = float(calc_rsi(c_m5, 14).iloc[-2])
        print("RSI M5=" + str(round(rsi,1)), flush=True)
        if direction == "BUY" and rsi >= 70:
            print("RSI surachete - annule", flush=True); return None
        if direction == "SELL" and rsi <= 30:
            print("RSI survendu - annule", flush=True); return None

        # ETAPE 9 : DXY securite absolue (donnees deja chargees)
        dxy_ok, dxy_v, dxy_t = check_dxy(direction, dxy_m5, dxy_h1)
        if not dxy_ok: return None

        # ETAPE 10 : Couleur bougie M5
        if direction == "BUY" and p <= o:
            print("Bougie non verte - annule", flush=True); return None
        if direction == "SELL" and p >= o:
            print("Bougie non rouge - annule", flush=True); return None

        # Signal valide
        sl = round(p - sl_pts, 2) if direction == "BUY" else round(p + sl_pts, 2)
        tp = round(p + tp_pts, 2) if direction == "BUY" else round(p - tp_pts, 2)
        register_signal(direction)
        gc.collect()
        print("SIGNAL " + direction + " p=" + str(round(p,2)) + " sl=" + str(round(sl_pips,1)) + "pips", flush=True)
        return {"dir":direction,"p":round(p,2),"sl":sl,"tp":tp,
                "sl_pips":round(sl_pips,1),"tp_pips":round(sl_pips*TP_RR,1),
                "rsi":round(rsi,1),"dxy":round(dxy_v,3),"dxy_t":dxy_t,
                "session":get_session_label()}
    except Exception as e:
        print("analyse_market ERREUR : " + str(e), flush=True)
        return None

# ============================================================
#  BOUCLE
# ============================================================

def wait_for_candle_close():
    now  = datetime.now(PARIS_TZ)
    wait = 300 - (now.second + (now.minute % 5) * 60)
    if wait <= 2: wait += 300
    print("Prochaine M5 dans " + str(wait) + "s", flush=True)
    time.sleep(wait)

def trading_loop():
    print("Boucle XAU/USD Sniper v4 demarree", flush=True)
    while True:
        try:
            wait_for_candle_close()
            now_str = datetime.now(PARIS_TZ).strftime("%H:%M")
            if not is_market_open():
                print("[" + now_str + "] Weekend - marche ferme", flush=True)
                continue
            if is_in_session():
                print("[" + now_str + "] " + get_session_label(), flush=True)
                s = analyse_market()
                if s:
                    d   = "ACHAT" if s["dir"] == "BUY" else "VENTE"
                    msg = ("XAU/USD SNIPER v4 - " + d + "\n"
                           + "Entree  : " + str(s["p"]) + "\n"
                           + "Stop    : " + str(s["sl"]) + " (" + str(s["sl_pips"]) + " pips)\n"
                           + "Cible   : " + str(s["tp"]) + " (" + str(s["tp_pips"]) + " pips)\n"
                           + "RR      : 1:" + str(TP_RR) + "\n"
                           + "RSI M5  : " + str(s["rsi"]) + "\n"
                           + "DXY     : " + str(s["dxy"]) + " (" + s["dxy_t"] + ")\n"
                           + "Session : " + s["session"] + "\n"
                           + "Lot     : " + str(LOT_SIZE) + "\n"
                           + "MTF     : H4(maitre)+H1+M15+M5 alignes")
                    bot.send_message(CHAT_ID, msg)
                    print("[" + now_str + "] Signal envoye " + s["dir"], flush=True)
                else:
                    print("[" + now_str + "] Pas de signal", flush=True)
            else:
                print("[" + now_str + "] Hors session", flush=True)
        except Exception as e:
            print("BOUCLE ERREUR : " + str(e), flush=True)
            time.sleep(30)

if __name__ == "__main__":
    print("Demarrage XAU/USD Sniper v4...", flush=True)
    threading.Thread(target=run_flask, daemon=True).start()
    time.sleep(2)
    now_s = datetime.now(PARIS_TZ).strftime("%d/%m/%Y a %H:%M")
    try:
        bot.send_message(CHAT_ID,
            "XAU/USD SNIPER v4 demarre\n"
            + "Date : " + now_s + "\n"
            + "H4 = filtre maitre EMA 20/50\n"
            + "Anti-doublon : cooldown " + str(COOLDOWN_MIN) + "min\n"
            + "Filtre weekend actif\n"
            + "DXY : 1 fetch pour M5+H1\n"
            + "Volume : GC=F > XAUUSD=X > Skip\n"
            + "SL : 1.5xATR Wilder (15-100 pips)\n"
            + "TP : RR 1:2\n"
            + "Sessions : 08h-12h + 14h-18h")
    except Exception as e:
        print("Msg demarrage : " + str(e), flush=True)
    trading_loop()
