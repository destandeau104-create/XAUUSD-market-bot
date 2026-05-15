"""
XAU/USD SNIPER v4.3 - MODE SIMULATION METAAPI
Source : RaiseFX via metaapi-cloud-sdk
ZERO ordre reel - logs terminal uniquement
SCORE_MIN = 50 (simulation - production = 60)
"""
import asyncio, os, gc, math
from datetime import datetime, timezone, timedelta
import pytz, pandas as pd, numpy as np
import telebot

# ============================================================
#  ACCÈS METAAPI - via variables d'environnement UNIQUEMENT
#  Ne jamais hardcoder le token dans le code
#  Sur Railway : Settings > Variables
#    META_API_TOKEN  = ton token MetaApi
#    META_ACCOUNT_ID = 7fed6592-a20e-4542-8720-52c9618f16e5
# ============================================================

META_TOKEN  = os.getenv("META_API_TOKEN",  "")
META_ACCT   = os.getenv("META_ACCOUNT_ID", "7fed6592-a20e-4542-8720-52c9618f16e5")
TG_TOKEN    = os.getenv("TELEGRAM_TOKEN",  "8218163213:AAEDXu19mXfeUSM65JIZAiBucxUAxmRHwy4")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID","1432682636")
SYMBOL      = "GOLD"
SIMULATION  = True   # TOUJOURS True - jamais d ordre reel

# ============================================================
#  PARAMETRES STRATEGIE
# ============================================================

PARIS_TZ        = pytz.timezone("Europe/Paris")
ATR_PERIOD      = 14
ATR_SPIKE_MULT  = 2.5
SL_ATR_MULT     = 1.5
SL_MIN_PIPS     = 15.0   # SL minimum Gold en pips (1 pip = 0.10$)
SL_MAX_PIPS     = 150.0
PIP_GOLD        = 0.10
TP_RR_MARKET    = 1.5
TP_RR_SNIPER    = 3.0
RETEST_THRESH   = 2.00
VOL_THRESHOLD   = 0.70
STOCH_OB        = 80
STOCH_OS        = 20
STOCH_GAP_HIGH  = 70
STOCH_GAP_LOW   = 30
ADR_BLOCK_PCT   = 0.85
DXY_KD_MIN_GAP  = 3.0
STRONG_TREND_PCT= 0.001

# Score stratifie - SIMULATION plus souple qu'en production
SCORE_MIN       = 50   # production = 60 / simulation = 50
SCORE_VOL       = 30
SCORE_RSI       = 25
SCORE_DXY_GAP   = 25
SCORE_MOMENTUM  = 20

try:
    bot = telebot.TeleBot(TG_TOKEN)
except Exception as e:
    print("Telebot init erreur : " + str(e), flush=True)
    bot = None

# ============================================================
#  METAAPI - RECUPERATION DES BOUGIES
#  Connexion async via websocket RaiseFX
# ============================================================

async def get_candles_metaapi(api, account, symbol, timeframe, count=300):
    """
    Recupere les bougies OHLCV depuis MetaApi RaiseFX.
    timeframe : "5m", "15m", "1h", "4h"
    Retourne un DataFrame pandas identique au format yfinance.
    """
    try:
        # Mapping timeframe MetaApi
        tf_map = {"5m":"5m","15m":"15m","1h":"1h","4h":"4h","1d":"1d"}
        tf = tf_map.get(timeframe, "5m")
        # Attente connexion terminal
        await account.wait_connected()
        connection = account.get_streaming_connection()
        await connection.connect()
        await connection.wait_synchronized()
        # Recuperation bougies
        candles = await connection.get_historical_candles(symbol, tf, count)
        if not candles:
            print("MetaApi : aucune bougie pour " + symbol + " " + tf, flush=True)
            return pd.DataFrame()
        # Conversion en DataFrame pandas
        rows = []
        for c in candles:
            rows.append({
                "Open":   float(c.get("brokerOpen",  c.get("open",  0))),
                "High":   float(c.get("brokerHigh",  c.get("high",  0))),
                "Low":    float(c.get("brokerLow",   c.get("low",   0))),
                "Close":  float(c.get("brokerClose", c.get("close", 0))),
                "Volume": float(c.get("tickVolume",  c.get("volume",0))),
            })
            # Timestamp
            ts = c.get("time", c.get("brokerTime",""))
            if isinstance(ts, str):
                try:
                    rows[-1]["Datetime"] = pd.to_datetime(ts)
                except:
                    rows[-1]["Datetime"] = pd.Timestamp.now()
            else:
                rows[-1]["Datetime"] = pd.Timestamp.now()
        df = pd.DataFrame(rows)
        df = df.set_index("Datetime").sort_index()
        df = df.dropna(subset=["Open","High","Low","Close"])
        print("MetaApi : " + str(len(df)) + " bougies recues " + symbol + " " + tf, flush=True)
        await connection.close()
        return df
    except Exception as e:
        print("MetaApi get_candles erreur : " + str(e), flush=True)
        return pd.DataFrame()

async def get_current_price_metaapi(api, account, symbol):
    """Prix bid/ask en temps reel depuis RaiseFX."""
    try:
        await account.wait_connected()
        connection = account.get_streaming_connection()
        await connection.connect()
        await connection.wait_synchronized()
        price = await connection.get_symbol_price(symbol)
        await connection.close()
        if price:
            bid = float(price.get("bid", 0))
            ask = float(price.get("ask", 0))
            spread = round((ask - bid) / PIP_GOLD, 1)
            print("Prix RaiseFX : bid=" + str(bid) + " ask=" + str(ask)
                  + " spread=" + str(spread) + " pips", flush=True)
            return bid, ask, spread
        return None, None, None
    except Exception as e:
        print("MetaApi get_price erreur : " + str(e), flush=True)
        return None, None, None

# ============================================================
#  INDICATEURS (identiques v4.3 - travaillent sur df MetaApi)
# ============================================================

def calc_ema(series, length):
    return series.ewm(span=length, min_periods=length, adjust=False).mean()

def calc_rsi(series, length=14):
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_g = gain.ewm(com=length-1, min_periods=length, adjust=False).mean()
    avg_l = loss.ewm(com=length-1, min_periods=length, adjust=False).mean()
    rs    = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    """ATR Median anti-spike."""
    h  = df["High"].squeeze()
    l  = df["Low"].squeeze()
    c  = df["Close"].squeeze()
    tr = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).median()

def calc_stochastic(df, k_period=14, d_period=3):
    h       = df["High"].squeeze()
    l       = df["Low"].squeeze()
    c       = df["Close"].squeeze()
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

def is_strong_trend_gold(df_h1):
    try:
        c     = df_h1["Close"].squeeze()
        price = float(c.iloc[-2])
        e20   = float(calc_ema(c, 20).iloc[-2])
        e50   = float(calc_ema(c, 50).iloc[-2])
        e200  = float(calc_ema(c, 200).iloc[-2])
        gap   = abs(e20 - e200) / price
        if e20 > e50 > e200 and gap > STRONG_TREND_PCT: return "STRONG_BUY"
        if e20 < e50 < e200 and gap > STRONG_TREND_PCT: return "STRONG_SELL"
        return "NORMAL"
    except: return "NORMAL"

# ============================================================
#  OB + FIBO - identiques v4.3
# ============================================================

def get_sniper_levels(df, direction, atr_val=None, trend_status="NORMAL"):
    try:
        df = df.dropna(subset=["Open","High","Low","Close"])
        if len(df) < 50: return None
        closes = df["Close"].squeeze()
        opens  = df["Open"].squeeze()
        highs  = df["High"].squeeze()
        lows   = df["Low"].squeeze()
        candidates = []
        for i in range(3, 22):
            idx = len(df) - 2 - i
            if idx < 0: break
            c = float(closes.iloc[idx]); o = float(opens.iloc[idx])
            h = float(highs.iloc[idx]);  l = float(lows.iloc[idx])
            if any(v != v for v in [c,o,h,l]): continue
            ob_size = h - l
            if atr_val is not None and ob_size < atr_val * 0.5: continue
            if direction == "BUY" and c < o:
                candidates.append({"high":h,"low":l,"mid":round((h+l)/2,2),
                                   "size":ob_size,"size_pips":round(ob_size/PIP_GOLD,1)})
            elif direction == "SELL" and c > o:
                candidates.append({"high":h,"low":l,"mid":round((h+l)/2,2),
                                   "size":ob_size,"size_pips":round(ob_size/PIP_GOLD,1)})
        if not candidates: return None
        ob = max(candidates, key=lambda x: x["size"])
        swing_high = float(highs.iloc[-12:-2].max())
        swing_low  = float(lows.iloc[-12:-2].min())
        if swing_high <= swing_low: return None
        amp = swing_high - swing_low
        fib_382 = round(swing_low + amp * 0.382, 2)
        fib_500 = round(swing_low + amp * 0.500, 2)
        fib_618 = round(swing_low + amp * 0.618, 2)
        is_strong = (trend_status == "STRONG_BUY" and direction == "BUY") or \
                    (trend_status == "STRONG_SELL" and direction == "SELL")
        limit_market = fib_382 if is_strong else fib_500
        limit_sniper = fib_618
        fib_label    = "0.382 [TENDANCE FORTE]" if is_strong else "0.500"
        tol = round(atr_val * 0.10, 2) if atr_val else 0.0
        swing_sl_buy  = round(float(lows.iloc[-50:-2].min()), 2)
        swing_sl_sell = round(float(highs.iloc[-50:-2].max()), 2)
        return {
            "ob": ob, "fib_382": fib_382, "fib_500": fib_500, "fib_618": fib_618,
            "limit": limit_market, "limit_market": limit_market,
            "limit_sniper": limit_sniper, "fib_label": fib_label,
            "limit_low": round(limit_market - tol, 2),
            "limit_high": round(limit_market + tol, 2),
            "golden_pocket": str(min(fib_500,fib_618)) + "-" + str(max(fib_500,fib_618)),
            "swing_sl_buy": swing_sl_buy, "swing_sl_sell": swing_sl_sell,
            "is_strong": is_strong,
        }
    except Exception as e:
        print("get_sniper_levels : " + str(e), flush=True)
        return None

# ============================================================
#  SESSIONS
# ============================================================

SESSIONS = [
    {"name":"Pre-Londres",  "start":(7,30),  "end":(8,0),   "premarket":True},
    {"name":"Matin",        "start":(8,0),   "end":(13,0),  "premarket":False},
    {"name":"Pre-NewYork",  "start":(13,30), "end":(14,30), "premarket":True},
    {"name":"Apres-midi",   "start":(14,30), "end":(19,0),  "premarket":False},
]

def is_market_open():
    now = datetime.now(timezone.utc)
    wd  = now.weekday()
    if wd == 5: return False
    if wd == 6 and now.hour < 22: return False
    return True

def get_current_session():
    if not is_market_open(): return None
    now = datetime.now(PARIS_TZ)
    hm  = (now.hour, now.minute)
    for s in SESSIONS:
        if s["start"] <= hm < s["end"]: return s
    return None

def get_session_label():
    s = get_current_session()
    if s is None: return "Hors session"
    return s["name"] + (" [PRE-MARKET]" if s["premarket"] else "")

# ============================================================
#  TELEGRAM
# ============================================================

def send_msg(msg):
    if not bot or not TG_CHAT: return
    try:
        bot.send_message(TG_CHAT, msg)
        print("Telegram OK", flush=True)
    except Exception as e:
        print("Telegram erreur : " + str(e), flush=True)

# ============================================================
#  ANALYSE PRINCIPALE (async - donnees MetaApi)
# ============================================================

async def analyse_gold(api, account):
    """
    Analyse complete XAU/USD avec donnees RaiseFX.
    Meme logique que v4.3 mais sur donnees broker reelles.
    """
    try:
        # Recuperation donnees MetaApi
        df_m5  = await get_candles_metaapi(api, account, SYMBOL, "5m",  300)
        df_m15 = await get_candles_metaapi(api, account, SYMBOL, "15m", 200)
        df_h1  = await get_candles_metaapi(api, account, SYMBOL, "1h",  300)

        for nm, df, n in [("M5",df_m5,55),("M15",df_m15,55),("H1",df_h1,200)]:
            if df is None or len(df) < n:
                print("MetaApi " + nm + " insuffisant (" + str(len(df) if df is not None else 0) + ")", flush=True)
                return None

        # H4 resample depuis H1
        df_h4 = df_h1.resample("4h").agg(
            {"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}
        ).dropna()
        if len(df_h4) < 20:
            print("H4 insuffisant", flush=True); return None

        # Prix reel du broker
        bid, ask, spread = await get_current_price_metaapi(api, account, SYMBOL)
        print("Spread RaiseFX : " + str(spread) + " pips", flush=True)

        # ETAPE 1 : EMA 200 H1 direction
        ema200_h1 = float(calc_ema(df_h1["Close"].squeeze(), 200).iloc[-2])
        price_h1  = float(df_h1["Close"].squeeze().iloc[-2])
        if price_h1 > ema200_h1:   direction = "BUY"
        elif price_h1 < ema200_h1: direction = "SELL"
        else:
            print("EMA200 neutre - pas de signal", flush=True); return None
        expected = 1 if direction == "BUY" else -1
        print("EMA200 H1 : " + direction, flush=True)

        # ETAPE 2 : Alignement H4+H1+M5
        for nm, df in [("H4",df_h4),("H1",df_h1),("M15",df_m15),("M5",df_m5)]:
            b = ema_bias(df, nm)
            if b == 0 or b != expected:
                print(nm + " non aligne - stop", flush=True); return None
        print("EMA alignes H4+H1+M15+M5", flush=True)

        # Bougie M5 cloturee (iloc[-2] = derniere cloturee)
        c_m5 = df_m5["Close"].squeeze(); o_m5 = df_m5["Open"].squeeze()
        h_m5 = df_m5["High"].squeeze();  l_m5 = df_m5["Low"].squeeze()
        p = float(c_m5.iloc[-2]); o = float(o_m5.iloc[-2])
        h = float(h_m5.iloc[-2]); l = float(l_m5.iloc[-2])
        if any(v != v for v in [p,o,h,l]):
            print("NaN detecte sur bougie M5 - stop", flush=True); return None

        # ETAPE 3 : ATR + protection zero
        atr = float(calc_atr(df_m5, ATR_PERIOD).iloc[-2])
        if atr != atr or atr <= 0:
            print("ATR invalide - stop", flush=True); return None
        if (h - l) > ATR_SPIKE_MULT * atr:
            print("Panique ATR - stop", flush=True); return None

        # ETAPE 4 : Retest EMA20 M5
        ema20_m5 = float(calc_ema(c_m5, 20).iloc[-2])
        ecart    = abs(p - ema20_m5)
        if ecart > RETEST_THRESH:
            print("Pas de retest EMA20 (" + str(round(ecart,2)) + "$ > " + str(RETEST_THRESH) + "$) - stop", flush=True)
            return None
        print("Retest EMA20 OK : " + str(round(ecart,2)) + "$", flush=True)

        # ETAPE 5 : SL MARKET
        sl_pips = (SL_ATR_MULT * atr) * 10
        if sl_pips < SL_MIN_PIPS: sl_pips = SL_MIN_PIPS
        if sl_pips > SL_MAX_PIPS:
            print("SL trop grand - stop", flush=True); return None
        sl_pts  = sl_pips / 10.0
        sl_mkt  = round(p - sl_pts, 2) if direction == "BUY" else round(p + sl_pts, 2)
        tp_mkt  = round(p + sl_pips/10.0*TP_RR_MARKET, 2) if direction == "BUY" else round(p - sl_pips/10.0*TP_RR_MARKET, 2)

        # ── SCORE DE CONFLUENCE ────────────────────────────────
        score     = 0
        score_log = []

        # Score Volume (30 pts) - utilise tickVolume MetaApi
        vol = df_m5["Volume"].squeeze()
        vol_sig = float(vol.iloc[-2]) if len(vol) > 2 else 0
        vol_avg = float(vol.iloc[-21:-2].mean()) if len(vol) > 21 else 0
        vol_ok = vol_sig > vol_avg * VOL_THRESHOLD if vol_avg > 0 else (h - l) > atr * 0.8
        vol_src = "TickVol RaiseFX"
        if vol_ok:
            score += SCORE_VOL
            score_log.append("Vol+" + str(SCORE_VOL))
        else:
            score_log.append("Vol+0")

        # Score RSI (25 pts) - extremes absolus obligatoires
        rsi = float(calc_rsi(c_m5, 14).iloc[-2])
        if direction == "BUY" and rsi >= 75:
            print("RSI extreme >75 - BLOQUE", flush=True); return None
        if direction == "SELL" and rsi <= 25:
            print("RSI extreme <25 - BLOQUE", flush=True); return None
        if (direction == "BUY" and rsi < 70) or (direction == "SELL" and rsi > 30):
            score += SCORE_RSI
            score_log.append("RSI+" + str(SCORE_RSI))
        else:
            score_log.append("RSI+0")

        # Score DXY Gap (25 pts)
        # Symbole DXY selon broker - sur RaiseFX verifier le nom exact
        # Si non disponible le score DXY est simplement ignore (pas bloquant)
        DXY_SYMBOL = os.getenv("DXY_SYMBOL", "USDX")  # ajuster si besoin
        dxy_k = 0.0; dxy_t = "N/A"
        try:
            df_dxy = await get_candles_metaapi(api, account, DXY_SYMBOL, "1h", 50)
            if df_dxy is not None and len(df_dxy) >= 20:
                k, d = calc_stochastic(df_dxy)
                dxy_k = k
                dir_check = direction
                if k > STOCH_OB or k < STOCH_OS:
                    print("DXY zone extreme - BLOQUE", flush=True); return None
                dxy_ok = not ((dir_check=="BUY" and k < STOCH_GAP_LOW) or
                              (dir_check=="SELL" and k > STOCH_GAP_HIGH) or
                              abs(k-d) < DXY_KD_MIN_GAP)
                if dxy_ok:
                    score += SCORE_DXY_GAP
                    score_log.append("DXY+" + str(SCORE_DXY_GAP))
                    dxy_t = "haussier" if k > d else "baissier"
                else:
                    score_log.append("DXY+0")
            else:
                score_log.append("DXY:N/A")
        except:
            score_log.append("DXY:N/A")

        # Score Momentum M5 (20 pts)
        momentum_ok = False
        if len(h_m5) > 3:
            prev_mid = (float(h_m5.iloc[-3]) + float(l_m5.iloc[-3])) / 2
            momentum_ok = (direction=="BUY" and p >= prev_mid) or \
                          (direction=="SELL" and p <= prev_mid)
        if momentum_ok:
            score += SCORE_MOMENTUM
            score_log.append("Mom+" + str(SCORE_MOMENTUM))
        else:
            score_log.append("Mom+0")

        print("Score : " + str(score) + "/100 [" + " | ".join(score_log) + "] min=" + str(SCORE_MIN), flush=True)
        if score < SCORE_MIN:
            print("Score insuffisant (" + str(score) + " < " + str(SCORE_MIN) + ") - stop", flush=True)
            return None

        # ETAPE 6 : OB + Fibo dual
        trend_status = is_strong_trend_gold(df_h1)
        levels_m5  = get_sniper_levels(df_m5,  direction, atr_val=atr, trend_status=trend_status)
        levels_m15 = get_sniper_levels(df_m15, direction, atr_val=atr, trend_status=trend_status)
        levels_best = levels_m15 if levels_m15 is not None else levels_m5
        ob_tf = "M15" if levels_m15 is not None else "M5"

        gc.collect()

        return {
            "dir": direction, "p": round(p,2), "bid": bid, "ask": ask,
            "spread": spread, "sl_mkt": sl_mkt, "tp_mkt": tp_mkt,
            "sl_pips": round(sl_pips,1), "tp_pips": round(sl_pips*TP_RR_MARKET,1),
            "rsi": round(rsi,1), "ema200": round(ema200_h1,2),
            "ecart": round(ecart,2), "dxy_k": round(dxy_k,1), "dxy_t": dxy_t,
            "atr": round(atr,2), "vol_src": vol_src, "score": score,
            "score_log": " | ".join(score_log), "ob_tf": ob_tf,
            "trend_status": trend_status,
            "levels": levels_best,
            "fib_label": levels_best["fib_label"] if levels_best else "0.500",
            "session": get_session_label(),
        }
    except Exception as e:
        print("analyse_gold ERREUR : " + str(e), flush=True)
        return None

# ============================================================
#  BOUCLE PRINCIPALE ASYNC
# ============================================================

async def wait_next_m5():
    """Attend la prochaine bougie M5 cloturee."""
    now  = datetime.now(PARIS_TZ)
    wait = 300 - (now.second + (now.minute % 5) * 60)
    if wait <= 2: wait += 300
    print("Prochaine M5 dans " + str(wait) + "s", flush=True)
    await asyncio.sleep(wait)

async def main():
    """Boucle principale MetaApi - simulation pure."""
    print("="*60, flush=True)
    print("XAU/USD SNIPER - MODE SIMULATION METAAPI", flush=True)
    print("Broker : RaiseFX | Symbole : " + SYMBOL + " (Gold)", flush=True)
    print("SCORE_MIN=" + str(SCORE_MIN) + " | ADR=" + str(ADR_BLOCK_PCT)
          + " | DXY_GAP=" + str(DXY_KD_MIN_GAP), flush=True)
    print("ZERO ORDRE REEL - LOGS TERMINAL UNIQUEMENT", flush=True)
    print("="*60, flush=True)

    if not META_TOKEN:
        print("ERREUR : META_API_TOKEN non defini dans les variables d'environnement", flush=True)
        print("Railway > Settings > Variables > Ajouter META_API_TOKEN", flush=True)
        return

    # Import MetaApi ici pour garder la compatibilite
    try:
        from metaapi_cloud_sdk import MetaApi
    except ImportError:
        print("ERREUR : metaapi-cloud-sdk non installe", flush=True)
        print("Ajouter 'metaapi-cloud-sdk' dans requirements.txt", flush=True)
        return

    api     = MetaApi(META_TOKEN)
    account = await api.metatrader_account_api.get_account(META_ACCT)

    print("Connexion RaiseFX...", flush=True)
    await account.deploy()
    await account.wait_connected()
    print("Connecte a RaiseFX - compte : " + META_ACCT[:8] + "...", flush=True)

    send_msg(
        "SIMULATION MetaApi XAU/USD\n"
        + "Broker    : RaiseFX\n"
        + "Score min : " + str(SCORE_MIN) + "/100\n"
        + "ADR block : " + str(int(ADR_BLOCK_PCT*100)) + "%\n"
        + "Mode      : ZERO ordre reel - logs terminal"
    )

    while True:
        try:
            await wait_next_m5()
            now_str = datetime.now(PARIS_TZ).strftime("%H:%M")

            if not is_market_open():
                print("[" + now_str + "] Weekend - ferme", flush=True)
                continue
            if get_current_session() is None:
                print("[" + now_str + "] Hors session", flush=True)
                continue

            print("[" + now_str + "] " + get_session_label() + " - analyse...", flush=True)
            s = await analyse_gold(api, account)

            if s:
                d  = "ACHAT" if s["dir"] == "BUY" else "VENTE"
                trend_tag = " [TENDANCE FORTE]" if "STRONG" in s.get("trend_status","") else ""
                lv = s.get("levels")

                # Log terminal detaille
                print("\n" + "="*55, flush=True)
                print("SIGNAL SIMULATION" + trend_tag + " - " + d, flush=True)
                print("Prix broker   : bid=" + str(s["bid"]) + " ask=" + str(s["ask"]), flush=True)
                print("Spread        : " + str(s["spread"]) + " pips", flush=True)
                print("Entree MARKET : " + str(s["p"]), flush=True)
                print("Stop Loss     : " + str(s["sl_mkt"]) + " (" + str(s["sl_pips"]) + " pips)", flush=True)
                print("Take Profit   : " + str(s["tp_mkt"]) + " (" + str(s["tp_pips"]) + " pips)", flush=True)
                if lv:
                    print("Fibo MARKET   : " + s["fib_label"] + " = " + str(lv["limit_market"]), flush=True)
                    print("Fibo SNIPER   : 0.618 = " + str(lv["fib_618"]), flush=True)
                    print("OB " + s["ob_tf"] + "       : " + str(lv["ob"]["low"]) + "-" + str(lv["ob"]["high"]), flush=True)
                print("RSI M5        : " + str(s["rsi"]), flush=True)
                print("DXY Stoch     : K=" + str(s["dxy_k"]) + " (" + s["dxy_t"] + ")", flush=True)
                print("Score         : " + str(s["score"]) + "/100 [" + s["score_log"] + "]", flush=True)
                print("Session       : " + s["session"], flush=True)
                print("="*55 + "\n", flush=True)

                # Alerte Telegram avec tag SIMULATION
                msg = ("[SIMULATION MetaApi] XAU/USD - " + d + trend_tag + "\n"
                       + "Broker : RaiseFX | Spread : " + str(s["spread"]) + " pips\n"
                       + "Fibo MARKET : " + s.get("fib_label","0.500") + "\n"
                       + "\n"
                       + "⚡ OPTION MARKET\n"
                       + "Entree : " + str(s["p"]) + "\n"
                       + "Stop   : " + str(s["sl_mkt"]) + " (" + str(s["sl_pips"]) + " pips)\n"
                       + "Cible  : " + str(s["tp_mkt"]) + " (" + str(s["tp_pips"]) + " pips | RR 1:" + str(TP_RR_MARKET) + ")\n"
                       + "\n")
                if lv:
                    msg += ("🎯 OPTION SNIPER (Fibo 0.618)\n"
                            + "Entree LIMIT : " + str(lv["fib_618"]) + "\n"
                            + "Golden Pocket : " + str(lv.get("golden_pocket","")) + "\n"
                            + "OB " + s["ob_tf"] + " : " + str(lv["ob"]["low"]) + "-" + str(lv["ob"]["high"]) + "\n"
                            + "\n")
                msg += ("Score   : " + str(s["score"]) + "/100 [" + s["score_log"] + "]\n"
                        + "RSI M5  : " + str(s["rsi"]) + "\n"
                        + "DXY     : K=" + str(s["dxy_k"]) + " (" + s["dxy_t"] + ")\n"
                        + "Session : " + s["session"] + "\n"
                        + "** SIMULATION - aucun ordre place **")
                send_msg(msg)
            else:
                print("[" + now_str + "] Pas de signal", flush=True)

            gc.collect()

        except Exception as e:
            print("BOUCLE ERREUR : " + str(e), flush=True)
            await asyncio.sleep(30)

# ============================================================
#  LANCEMENT
# ============================================================

if __name__ == "__main__":
    asyncio.run(main())
