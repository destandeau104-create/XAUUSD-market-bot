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
    print("Telebot erreur : " + str(e), flush=True)
    bot = None

PARIS_TZ       = pytz.timezone("Europe/Paris")
LOT_SIZE       = 0.50
ATR_PERIOD     = 14
ATR_SPIKE_MULT = 2.5
SL_ATR_MULT    = 1.5
SL_MIN_PIPS    = 15.0
SL_MAX_PIPS    = 150.0
TP_RR_MARKET   = 1.5
TP_RR_SNIPER   = 3.0
COOLDOWN_MIN   = 30
RETEST_THRESH  = 1.50
VOL_THRESHOLD  = 0.80
STOCH_OB       = 80
STOCH_OS       = 20
STOCH_GAP_HIGH = 70
STOCH_GAP_LOW  = 30
PIP_GOLD       = 0.10

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
    """
    ATR Median anti-spike (plus robuste que Wilder en marche volatile).
    Utilise rolling median au lieu de ewm pour eliminer l'impact
    des bougies de news sur le calcul du SL.
    Wilder garde pour les autres indicateurs (RSI etc).
    """
    h = df["High"].squeeze()
    l = df["Low"].squeeze()
    c = df["Close"].squeeze()
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).median()

def calc_stochastic(df, k_period=14, d_period=3):
    h = df["High"].squeeze()
    l = df["Low"].squeeze()
    c = df["Close"].squeeze()
    lowest  = l.rolling(k_period).min()
    highest = h.rolling(k_period).max()
    denom   = (highest - lowest).replace(0, np.nan)
    k       = 100 * (c - lowest) / denom
    d       = k.rolling(d_period).mean()
    return float(k.iloc[-2]), float(d.iloc[-2])

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
    now = datetime.now(pytz.utc)
    wd  = now.weekday()
    if wd == 5: return False
    if wd == 6 and now.hour < 22: return False
    return True

def is_in_session():
    if not is_market_open(): return False
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    matin = (h, m) >= (8, 0) and (h, m) <= (13, 0)
    aprem = (h, m) >= (14, 30) and (h, m) <= (19, 0)
    return matin or aprem

def get_session_label():
    now  = datetime.now(PARIS_TZ)
    h, m = now.hour, now.minute
    if (h, m) >= (8, 0) and (h, m) <= (13, 0):   return "Matin 08h-13h"
    if (h, m) >= (14, 30) and (h, m) <= (19, 0): return "Apres-midi 14h30-19h"
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
            print("Doublon - cooldown " + str(round(elapsed,1)) + "min", flush=True)
            return False
    return True

def register_signal(direction):
    global _last_signal_dir, _last_signal_time
    _last_signal_dir  = direction
    _last_signal_time = datetime.now(PARIS_TZ)

# ============================================================
#  DONNEES - fetch robuste + fix Multi-Index
# ============================================================

def get_data(ticker, interval, period, retries=3):
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(ticker, interval=interval, period=period,
                             progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.loc[:, ~df.columns.duplicated()]
                return df
            print("get_data " + ticker + " vide " + str(attempt), flush=True)
        except Exception as e:
            print("get_data " + ticker + " err " + str(attempt) + " : " + str(e), flush=True)
        if attempt < retries: time.sleep(5)
    return pd.DataFrame()

def get_price_data(t1, t2, interval, period):
    df = get_data(t1, interval, period)
    if df is not None and not df.empty:
        print("Source : " + t1, flush=True)
        return df
    print("Fallback vers " + t2, flush=True)
    df = get_data(t2, interval, period)
    if df is not None and not df.empty:
        print("Source : " + t2, flush=True)
        return df
    return pd.DataFrame()

# ============================================================
#  VOLUME HYBRIDE + CORRECTIF ATR
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
        return vol
    except Exception as e:
        print("get_vol " + ticker + " : " + str(e), flush=True)
        return None

def check_volume(candle_high, candle_low, atr_val):
    for ticker, label in [("GC=F","Futures (GC=F)"),("XAUUSD=X","Spot (XAUUSD=X)")]:
        vol = get_vol(ticker)
        if vol is not None:
            sig = float(vol.iloc[-2])
            avg = float(vol.iloc[-17:-2].mean())
            print("Volume " + label + " sig=" + str(round(sig,0)) + " avg=" + str(round(avg,0)), flush=True)
            if sig == 0 and avg == 0:
                if (candle_high - candle_low) > atr_val * 0.8:
                    print("Volume Valide (ATR)", flush=True)
                    return True, "ATR validation"
                return False, label
            if avg > 0 and sig < avg * VOL_THRESHOLD:
                print("Volume insuffisant - annule", flush=True)
                return False, label
            print("Volume OK", flush=True)
            return True, label
    if (candle_high - candle_low) > atr_val * 0.8:
        print("Volume Valide (ATR fallback)", flush=True)
        return True, "ATR fallback"
    return False, "Skip"

# ============================================================
#  MODULE DUO SNIPER
#  MARKET : SL ATR dynamique | RR 1:1.5  (reactivite)
#  SNIPER : SL derriere OB   | RR 1:3    (precision)
# ============================================================

def get_sniper_levels(df, direction):
    """OB + Fibo 0.5 pour le prix LIMIT et SL technique."""
    try:
        closes = df["Close"].squeeze()
        opens  = df["Open"].squeeze()
        highs  = df["High"].squeeze()
        lows   = df["Low"].squeeze()
        ob = None
        for i in range(3, 22):
            idx = len(df) - 2 - i
            if idx < 0: break
            c = float(closes.iloc[idx])
            o = float(opens.iloc[idx])
            h = float(highs.iloc[idx])
            l = float(lows.iloc[idx])
            if direction == "BUY" and c < o:
                ob = {"high": h, "low": l, "mid": round((h + l) / 2, 2)}
                break
            if direction == "SELL" and c > o:
                ob = {"high": h, "low": l, "mid": round((h + l) / 2, 2)}
                break
        if ob is None:
            print("OB non trouve", flush=True)
            return None
        swing_high  = float(highs.iloc[-12:-2].max())
        swing_low   = float(lows.iloc[-12:-2].min())
        if swing_high <= swing_low: return None
        fib_50      = round(swing_low + (swing_high - swing_low) * 0.5, 2)
        limit_price = round((ob["mid"] + fib_50) / 2, 2)
        print("OB=" + str(ob["low"]) + "-" + str(ob["high"]) + " Fibo50=" + str(fib_50) + " LIMIT=" + str(limit_price), flush=True)
        return {"ob": ob, "fib_50": fib_50, "limit": limit_price}
    except Exception as e:
        print("get_sniper_levels : " + str(e), flush=True)
        return None

def calc_sniper_option(direction, entry_market, sl_market, levels):
    """
    OPTION SNIPER : SL derriere OB + RR 1:3.
    Optimisation 1 : SL MARKET ajuste (pas annule) si chevauchement LIMIT.
    Optimisation 2 : ATR median deja integre dans calc_atr.
    Buffer = 5 pips entre SL MARKET et prix LIMIT.
    """
    BUFFER = 5 * PIP_GOLD
    if levels is None: return None
    try:
        ob          = levels["ob"]
        limit_price = levels["limit"]
        if direction == "BUY":
            improvement = round((entry_market - limit_price) / PIP_GOLD, 1)
            if limit_price >= entry_market or improvement < 10:
                print("SNIPER : amelioration insuffisante (" + str(improvement) + " pips)", flush=True)
                return None
            # Ajustement SL MARKET si trop proche du LIMIT (au lieu d'annuler)
            sl_mkt_adj = sl_market
            if sl_mkt_adj >= limit_price - BUFFER:
                sl_mkt_adj = round(limit_price - BUFFER - PIP_GOLD, 2)
                print("SL MARKET ajuste -> " + str(sl_mkt_adj) + " (5 pips sous LIMIT)", flush=True)
            sl_sniper = round(ob["low"] - PIP_GOLD * 2, 2)
        else:
            improvement = round((limit_price - entry_market) / PIP_GOLD, 1)
            if limit_price <= entry_market or improvement < 10:
                print("SNIPER : amelioration insuffisante (" + str(improvement) + " pips)", flush=True)
                return None
            sl_mkt_adj = sl_market
            if sl_mkt_adj <= limit_price + BUFFER:
                sl_mkt_adj = round(limit_price + BUFFER + PIP_GOLD, 2)
                print("SL MARKET ajuste -> " + str(sl_mkt_adj) + " (5 pips sur LIMIT)", flush=True)
            sl_sniper = round(ob["high"] + PIP_GOLD * 2, 2)
        sl_dist = abs(limit_price - sl_sniper) / PIP_GOLD
        if sl_dist > SL_MAX_PIPS:
            print("SNIPER : OB trop loin (" + str(round(sl_dist,1)) + " pips) - annule", flush=True)
            return None
        if sl_dist < 3:
            print("SNIPER : SL trop serre - annule", flush=True)
            return None
        tp_dist   = sl_dist * TP_RR_SNIPER
        tp_sniper = round(limit_price + tp_dist * PIP_GOLD, 2) if direction == "BUY" else round(limit_price - tp_dist * PIP_GOLD, 2)
        # TP MARKET recalcule avec SL ajuste
        sl_mkt_dist = abs(entry_market - sl_mkt_adj) / PIP_GOLD
        tp_mkt_adj  = round(entry_market + sl_mkt_dist * TP_RR_MARKET * PIP_GOLD, 2) if direction == "BUY" else round(entry_market - sl_mkt_dist * TP_RR_MARKET * PIP_GOLD, 2)
        print("SNIPER VALIDE +" + str(improvement) + " pips SL OB", flush=True)
        return {
            "limit":       limit_price,
            "sl":          sl_sniper,
            "tp":          tp_sniper,
            "sl_pips":     round(sl_dist, 1),
            "tp_pips":     round(tp_dist, 1),
            "improvement": improvement,
            "ob_zone":     str(ob["low"]) + "-" + str(ob["high"]),
            "fib_50":      levels["fib_50"],
            "sl_mkt_adj":  sl_mkt_adj,
            "tp_mkt_adj":  tp_mkt_adj,
            "sl_mkt_pips": round(sl_mkt_dist, 1),
            "tp_mkt_pips": round(sl_mkt_dist * TP_RR_MARKET, 1),
        }
    except Exception as e:
        print("calc_sniper_option : " + str(e), flush=True)
        return None

# ============================================================
#  FILTRE DXY STOCHASTIQUE H1 + MOMENTUM GAP
# ============================================================

def check_dxy_stoch(direction):
    try:
        df_dxy = get_data("DX-Y.NYB", "1h", "10d")
        if df_dxy is None or df_dxy.empty or len(df_dxy) < 20:
            print("DXY indisponible - bypasse", flush=True)
            return True, 0.0, "N/A"
        k, d = calc_stochastic(df_dxy)
        print("DXY Stoch K=" + str(round(k,1)) + " D=" + str(round(d,1)), flush=True)
        if direction == "BUY" and k > STOCH_OB:
            print("DXY surachete - BUY annule", flush=True)
            return False, k, "surachete"
        if direction == "SELL" and k < STOCH_OS:
            print("DXY survendu - SELL annule", flush=True)
            return False, k, "survendu"
        if direction == "BUY" and k < STOCH_GAP_LOW:
            print("DXY Momentum Gap K<30 - BUY annule", flush=True)
            return False, k, "bout de course baissier"
        if direction == "SELL" and k > STOCH_GAP_HIGH:
            print("DXY Momentum Gap K>70 - SELL annule", flush=True)
            return False, k, "bout de course haussier"
        tendance = "haussier" if k > d else "baissier"
        print("DXY OK pour " + direction, flush=True)
        return True, k, tendance
    except Exception as e:
        print("DXY erreur : " + str(e) + " - bypasse", flush=True)
        return True, 0.0, "N/A"

# ============================================================
#  ANALYSE PRINCIPALE
# ============================================================

def analyse_market():
    try:
        df_h1_raw = get_price_data("XAUUSD=X", "GC=F", "1h", "60d")
        df_m5     = get_price_data("XAUUSD=X", "GC=F", "5m", "5d")
        df_m15    = get_price_data("XAUUSD=X", "GC=F", "15m","10d")

        for name, df, n in [("M5",df_m5,55),("M15",df_m15,55),("H1raw",df_h1_raw,200)]:
            if df is None or len(df) < n:
                print(name + " insuffisant", flush=True)
                return None

        df_h1 = df_h1_raw.tail(720)
        df_h4 = df_h1_raw.resample("4h").agg(
            {"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}
        ).dropna()
        if len(df_h4) < 55:
            print("H4 insuffisant", flush=True)
            return None

        # ETAPE 1 : EMA 200 H1 tendance maitre
        ema200_h1 = float(calc_ema(df_h1["Close"].squeeze(), 200).iloc[-2])
        price_h1  = float(df_h1["Close"].squeeze().iloc[-2])
        if price_h1 > ema200_h1:   direction = "BUY"
        elif price_h1 < ema200_h1: direction = "SELL"
        else: print("Prix sur EMA200 H1 - neutre", flush=True); return None
        print("EMA200 H1 : " + direction, flush=True)
        expected = 1 if direction == "BUY" else -1

        # ETAPE 2 : Cooldown
        if not is_signal_allowed(direction): return None

        # ETAPE 3 : Alignement EMA H4+H1+M15+M5
        for name, df in [("H4",df_h4),("H1",df_h1),("M15",df_m15),("M5",df_m5)]:
            b = ema_bias(df, name)
            if b == 0 or b != expected:
                print(name + " non aligne - stop", flush=True)
                return None
        print("EMA H4+H1+M15+M5 alignes", flush=True)

        # Bougie M5 cloturee
        c_m5 = df_m5["Close"].squeeze()
        o_m5 = df_m5["Open"].squeeze()
        h_m5 = df_m5["High"].squeeze()
        l_m5 = df_m5["Low"].squeeze()
        p = float(c_m5.iloc[-2])
        o = float(o_m5.iloc[-2])
        h = float(h_m5.iloc[-2])
        l = float(l_m5.iloc[-2])

        # ETAPE 4 : Retest EMA20 M5 (declencheur)
        ema20_m5 = float(calc_ema(c_m5, 20).iloc[-2])
        ecart    = abs(p - ema20_m5)
        print("Retest EMA20 M5 : ecart=" + str(round(ecart,2)) + "$ seuil=" + str(RETEST_THRESH) + "$", flush=True)
        if ecart > RETEST_THRESH:
            print("Pas de retest", flush=True); return None
        print("Retest OK", flush=True)

        # ETAPE 5 : Anti-panique ATR
        atr = float(calc_atr(df_m5, ATR_PERIOD).iloc[-2])
        if (h - l) > ATR_SPIKE_MULT * atr:
            print("Panique ATR - annule", flush=True); return None

        # ETAPE 6 : SL MARKET = ATR dynamique (median anti-spike)
        sl_pips = (SL_ATR_MULT * atr) * 10
        if sl_pips < SL_MIN_PIPS: sl_pips = SL_MIN_PIPS
        if sl_pips > SL_MAX_PIPS:
            print("SL ATR trop grand - annule", flush=True); return None
        sl_pts  = sl_pips / 10.0
        sl_mkt  = round(p - sl_pts, 2) if direction == "BUY" else round(p + sl_pts, 2)
        tp_m_dist = sl_pips * TP_RR_MARKET
        tp_mkt  = round(p + tp_m_dist/10.0, 2) if direction == "BUY" else round(p - tp_m_dist/10.0, 2)

        # ETAPE 7 : Volume + correctif ATR
        vol_ok, vol_src = check_volume(h, l, atr)
        if not vol_ok: return None

        # ETAPE 8 : RSI M5
        rsi = float(calc_rsi(c_m5, 14).iloc[-2])
        print("RSI M5=" + str(round(rsi,1)), flush=True)
        if direction == "BUY" and rsi >= 70:
            print("RSI surachete - annule", flush=True); return None
        if direction == "SELL" and rsi <= 30:
            print("RSI survendu - annule", flush=True); return None

        # ETAPE 9 : DXY Stoch + Momentum Gap
        dxy_ok, dxy_k, dxy_t = check_dxy_stoch(direction)
        if not dxy_ok: return None

        # ETAPE 10 : Couleur bougie M5
        if direction == "BUY" and p <= o:
            print("Bougie non verte - annule", flush=True); return None
        if direction == "SELL" and p >= o:
            print("Bougie non rouge - annule", flush=True); return None

        # ETAPE 11 : OB + Fibo -> OPTION SNIPER (SL OB, RR 1:3)
        levels = get_sniper_levels(df_m5, direction)
        sniper = calc_sniper_option(direction, p, sl_mkt, levels)

        register_signal(direction)
        gc.collect()
        print("SIGNAL VALIDE " + direction + " @ " + str(round(p,2)), flush=True)

        return {
            "dir":      direction,
            "p":        round(p, 2),
            "sl_mkt":   sl_mkt,
            "tp_mkt":   tp_mkt,
            "sl_pips":  round(sl_pips, 1),
            "tp_pips":  round(tp_m_dist, 1),
            "rsi":      round(rsi, 1),
            "ema200":   round(ema200_h1, 2),
            "ema20_m5": round(ema20_m5, 2),
            "ecart":    round(ecart, 2),
            "dxy_k":    round(dxy_k, 1),
            "dxy_t":    dxy_t,
            "atr":      round(atr, 2),
            "vol_src":  vol_src,
            "session":  get_session_label(),
            "sniper":   sniper,
        }
    except Exception as e:
        print("analyse_market ERREUR : " + str(e), flush=True)
        return None

# ============================================================
#  BOUCLE DE TRADING
# ============================================================

def wait_for_candle_close():
    now  = datetime.now(PARIS_TZ)
    wait = 300 - (now.second + (now.minute % 5) * 60)
    if wait <= 2: wait += 300
    print("Prochaine M5 dans " + str(wait) + "s", flush=True)
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
                    d  = "ACHAT" if s["dir"] == "BUY" else "VENTE"
                    sn = s["sniper"]
                    # SL MARKET : utilise version ajustee si SNIPER present
                    sl_show  = sn["sl_mkt_adj"] if sn and "sl_mkt_adj" in sn else s["sl_mkt"]
                    tp_show  = sn["tp_mkt_adj"] if sn and "tp_mkt_adj" in sn else s["tp_mkt"]
                    slp_show = sn["sl_mkt_pips"] if sn and "sl_mkt_pips" in sn else s["sl_pips"]
                    tpp_show = sn["tp_mkt_pips"] if sn and "tp_mkt_pips" in sn else s["tp_pips"]
                    msg = ("XAU/USD SNIPER v4.3 - " + d + "\n"
                           + "\n"
                           + "⚡ OPTION MARKET (entree immediate)\n"
                           + "Entree : " + str(s["p"]) + "\n"
                           + "Stop   : " + str(sl_show) + " (" + str(slp_show) + " pips | SL ATR)\n"
                           + "Cible  : " + str(tp_show) + " (" + str(tpp_show) + " pips | RR 1:" + str(TP_RR_MARKET) + ")\n"
                           + "\n")
                    if sn:
                        msg += ("🎯 OPTION SNIPER (ordre LIMIT)\n"
                                + "Entree : " + str(sn["limit"]) + " (+" + str(sn["improvement"]) + " pips)\n"
                                + "Stop   : " + str(sn["sl"]) + " (" + str(sn["sl_pips"]) + " pips | SL OB)\n"
                                + "Cible  : " + str(sn["tp"]) + " (" + str(sn["tp_pips"]) + " pips | RR 1:" + str(TP_RR_SNIPER) + ")\n"
                                + "OB     : " + str(sn["ob_zone"]) + "\n"
                                + "Fibo   : " + str(sn["fib_50"]) + "\n"
                                + "\n")
                    else:
                        msg += "🎯 OPTION SNIPER : pas de confluence OB/Fibo\n\n"
                    msg += ("EMA200 H1 : " + str(s["ema200"]) + "\n"
                            + "Retest M5 : " + str(s["ecart"]) + "$ de EMA20\n"
                            + "RSI M5    : " + str(s["rsi"]) + "\n"
                            + "DXY Stoch : K=" + str(s["dxy_k"]) + " (" + s["dxy_t"] + ")\n"
                            + "Volume    : " + s["vol_src"] + "\n"
                            + "Session   : " + s["session"] + "\n"
                            + "Lot       : " + str(LOT_SIZE))
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
#  LANCEMENT
# ============================================================

if __name__ == "__main__":
    print("=" * 52, flush=True)
    print("XAU/USD Sniper v4.3 - Render", flush=True)
    print(datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M:%S"), flush=True)
    print("=" * 52, flush=True)
    threading.Thread(target=run_flask, daemon=True).start()
    time.sleep(2)
    print("Flask actif - lancement boucle trading", flush=True)
    now_s = datetime.now(PARIS_TZ).strftime("%d/%m/%Y a %H:%M")
    send_msg(
        "XAU/USD SNIPER v4.3 demarre\n"
        + "Date      : " + now_s + "\n"
        + "Tendance  : EMA 200 H1\n"
        + "MTF       : H4+H1+M15+M5 EMA 20/50\n"
        + "Declench. : Retest EMA20 M5 (<" + str(RETEST_THRESH) + "$)\n"
        + "MARKET    : SL ATR | RR 1:" + str(TP_RR_MARKET) + "\n"
        + "SNIPER    : SL OB  | RR 1:" + str(TP_RR_SNIPER) + "\n"
        + "DXY       : Stoch (14,3,3) + Momentum Gap 30/70\n"
        + "Volume    : GC=F > XAUUSD=X > ATR validation\n"
        + "Sessions  : 08h-13h + 14h30-19h"
    )
    trading_loop()