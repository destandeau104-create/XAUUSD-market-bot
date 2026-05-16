"""
XAU/USD SNIPER v4.3 - MODE SIMULATION METAAPI [ENGINE v3]
Source      : RaiseFX via metaapi-cloud-sdk
Symbole     : Gold (CFD RaiseFX, prix ~4551, pip=0.01)
Timeframes  : M1 declencheur + M5/M15/H1/H4 filtres HTF
Mode        : SIMULATION PURE - ZERO ordre reel
Score min   : 75/100  <- v3: releve de 50 (exige momentum)

OPTIMISATIONS v3 :
  1. SCORE_MIN 50 -> 75  : exige Mom+20 actif (+10% WR backtest)
  2. Filtre RSI strict   : zone 45-55 (hors zone = 18% WR)
  3. SL_MAX_PIPS 6953->4500 : coupe les outliers -130/-140 pips
  4. COOLDOWN_MIN 20->30 : reduit sur-trading en series perdantes
  Sessions : inchangees (Pre-Londres + Matin + Pre-NY + Apres-midi)
"""
import asyncio, os, gc
from datetime import datetime, timezone
import pytz, pandas as pd, numpy as np
import telebot

# ============================================================
#  ACCES - exclusivement via variables d'environnement
#  Railway > Settings > Variables
# ============================================================

META_TOKEN  = os.getenv("META_API_TOKEN",  "")
META_ACCT   = os.getenv("META_ACCOUNT_ID", "7fed6592-a20e-4542-8720-52c9618f16e5")
TG_TOKEN    = os.getenv("TELEGRAM_TOKEN",  "8218163213:AAEDXu19mXfeUSM65JIZAiBucxUAxmRHwy4")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID","1432682636")
SYMBOL      = "Gold"
SIMULATION  = True  # TOUJOURS True - jamais d'ordre reel

# ============================================================
#  PARAMETRES STRATEGIE
#  Calibres sur donnees reelles RaiseFX (diagnostic 15/05/2026)
#  Prix ~4551 | PIP=0.01 | ATR M5=4.635 | Spread=0.22
# ============================================================

PARIS_TZ        = pytz.timezone("Europe/Paris")
ATR_PERIOD      = 14
ATR_SPIKE_MULT  = 2.5
SL_ATR_MULT     = 1.5
PIP_GOLD        = 0.01    # confirme : 2 decimales Gold RaiseFX
SL_MIN_PIPS     = 695     # ATR(4.635) x1.5 / 0.01 = 695 pips
SL_MAX_PIPS     = 4500    # v3: reduit de 6953 (coupe outliers -130/-140 pips)
RETEST_THRESH   = 3.64    # P30 ecart EMA20 reel RaiseFX
TP_RR_MARKET    = 1.5
TP_RR_SNIPER    = 3.0
COOLDOWN_MIN    = 30      # v3: etendu de 20->30 min (reduit sur-trading)
VOL_THRESHOLD   = 0.70
STOCH_OB        = 80
STOCH_OS        = 20
STOCH_GAP_HIGH  = 70
STOCH_GAP_LOW   = 30
ADR_BLOCK_PCT   = 0.85
DXY_KD_MIN_GAP  = 3.0
STRONG_TREND_PCT= 0.001

# Score stratifie v3
SCORE_MIN       = 75   # v3: releve de 50 (exige momentum Mom+20 actif)
SCORE_VOL       = 30
SCORE_RSI       = 25
SCORE_DXY_GAP   = 25
SCORE_MOMENTUM  = 20

# ── Filtres v3 ───────────────────────────────────────────────
RSI_MIN              = 45  # v3: zone RSI stricte 45-55 (hors zone = 18% WR)
RSI_MAX              = 55

# Anti-doublon
_last_signal_dir  = None
_last_signal_time = None

try:
    bot = telebot.TeleBot(TG_TOKEN)
    print("Telebot initialise", flush=True)
except Exception as e:
    print("Telebot erreur : " + str(e), flush=True)
    bot = None

# ============================================================
#  SESSIONS ETENDUES AVEC PRE-MARKET
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
        if s["start"] <= hm < s["end"]:
            return s
    return None

def is_in_session(): return get_current_session() is not None

def get_session_label():
    s = get_current_session()
    if s is None: return "Hors session"
    return s["name"] + (" [PRE-MARKET]" if s["premarket"] else "")

# ============================================================
#  ANTI-DOUBLON
# ============================================================

def is_signal_allowed(direction):
    global _last_signal_dir, _last_signal_time
    now = datetime.now(PARIS_TZ)
    if _last_signal_time is not None and _last_signal_dir == direction:
        elapsed = (now - _last_signal_time).total_seconds() / 60
        if elapsed < COOLDOWN_MIN:
            print("Cooldown " + str(round(elapsed,1)) + "min - stop", flush=True)
            return False
    return True

def register_signal(direction):
    global _last_signal_dir, _last_signal_time
    _last_signal_dir  = direction
    _last_signal_time = datetime.now(PARIS_TZ)

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
#  METAAPI - RECUPERATION BOUGIES
# ============================================================

async def get_candles(account, symbol, timeframe, count):
    """Recupere les bougies OHLCV depuis RaiseFX via MetaApi."""
    try:
        now     = datetime.now(timezone.utc)
        candles = await account.get_historical_candles(symbol, timeframe, now, count)
        if not candles:
            print("MetaApi : aucune bougie " + symbol + " " + timeframe, flush=True)
            return pd.DataFrame()
        rows = []
        for c in candles:
            try:
                rows.append({
                    "Open":   float(c.get("open",  0)),
                    "High":   float(c.get("high",  0)),
                    "Low":    float(c.get("low",   0)),
                    "Close":  float(c.get("close", 0)),
                    "Volume": float(c.get("tickVolume", c.get("volume", 0))),
                })
            except Exception: continue
        if not rows: return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.dropna(subset=["Open","High","Low","Close"])
        print("MetaApi : " + str(len(df)) + " bougies " + symbol + " " + timeframe, flush=True)
        return df
    except Exception as e:
        print("get_candles erreur " + timeframe + " : " + str(e), flush=True)
        return pd.DataFrame()

async def get_price(account, symbol):
    """Prix bid/ask temps reel depuis RaiseFX."""
    try:
        conn = account.get_rpc_connection()
        await conn.connect()
        await conn.wait_synchronized()
        price = await conn.get_symbol_price(symbol)
        await conn.close()
        if price:
            bid    = float(price.get("bid", 0))
            ask    = float(price.get("ask", 0))
            spread = round((ask - bid) / PIP_GOLD, 1)
            return bid, ask, spread
        return None, None, None
    except Exception as e:
        print("get_price erreur : " + str(e), flush=True)
        return None, None, None

# ============================================================
#  INDICATEURS
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
    """ATR Median anti-spike - identique Gold Sniper v4.3."""
    h  = df["High"].squeeze()
    l  = df["Low"].squeeze()
    c  = df["Close"].squeeze()
    tr = pd.concat([
        h - l,
        (h - c.shift()).abs(),
        (l - c.shift()).abs()
    ], axis=1).max(axis=1)
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
    """Direction EMA 20/50 sur un timeframe."""
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
    """Tendance forte = EMA 20>50>200 avec ecart > 0.1%."""
    try:
        c     = df_h1["Close"].squeeze()
        price = float(c.iloc[-2])
        e20   = float(calc_ema(c, 20).iloc[-2])
        e50   = float(calc_ema(c, 50).iloc[-2])
        e200  = float(calc_ema(c, 200).iloc[-2])
        gap   = abs(e20 - e200) / price
        if e20 > e50 > e200 and gap > STRONG_TREND_PCT:
            print("Tendance FORTE BUY -> Fibo 0.382 actif", flush=True)
            return "STRONG_BUY"
        if e20 < e50 < e200 and gap > STRONG_TREND_PCT:
            print("Tendance FORTE SELL -> Fibo 0.382 actif", flush=True)
            return "STRONG_SELL"
        return "NORMAL"
    except Exception: return "NORMAL"

# ============================================================
#  OB + FIBO DUAL 0.382/0.618
# ============================================================

def get_sniper_levels(df, direction, atr_val=None, trend_status="NORMAL"):
    """
    OB = plus grand candidat valide (audit Wall Street).
    MARKET LIMIT : Fibo 0.382 si tendance forte, 0.500 sinon.
    SNIPER LIMIT : Fibo 0.618 toujours (Golden Pocket).
    Swing SL sur 48 bougies (4h) comme plafond.
    """
    try:
        df = df.dropna(subset=["Open","High","Low","Close"])
        if len(df) < 50: return None
        closes = df["Close"].squeeze()
        opens  = df["Open"].squeeze()
        highs  = df["High"].squeeze()
        lows   = df["Low"].squeeze()
        # Collecte tous les OB valides
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
                candidates.append({"high":h,"low":l,
                                   "mid":round((h+l)/2,2),
                                   "size":ob_size,
                                   "size_pips":round(ob_size/PIP_GOLD,1)})
            elif direction == "SELL" and c > o:
                candidates.append({"high":h,"low":l,
                                   "mid":round((h+l)/2,2),
                                   "size":ob_size,
                                   "size_pips":round(ob_size/PIP_GOLD,1)})
        if not candidates:
            print("OB : aucun candidat valide", flush=True)
            return None
        # Plus grand OB = empreinte institutionnelle maximale
        ob = max(candidates, key=lambda x: x["size"])
        print("OB : " + str(ob["low"]) + "-" + str(ob["high"])
              + " (" + str(ob["size_pips"]) + " pips)", flush=True)
        # Fibo sur swing 10 bougies
        swing_high = float(highs.iloc[-12:-2].max())
        swing_low  = float(lows.iloc[-12:-2].min())
        if swing_high <= swing_low or swing_high != swing_high or swing_low != swing_low:
            print("Swing invalide", flush=True); return None
        amp     = swing_high - swing_low
        fib_382 = round(swing_low + amp * 0.382, 2)
        fib_500 = round(swing_low + amp * 0.500, 2)
        fib_618 = round(swing_low + amp * 0.618, 2)
        is_strong = (trend_status=="STRONG_BUY" and direction=="BUY") or \
                    (trend_status=="STRONG_SELL" and direction=="SELL")
        limit_market = fib_382 if is_strong else fib_500
        limit_sniper = fib_618
        fib_label    = "0.382 [TENDANCE FORTE]" if is_strong else "0.500"
        tol          = round(atr_val * 0.10, 2) if atr_val else 0.0
        # Swing SL 4h = 48 bougies M5 ou 240 bougies M1
        swing_sl_buy  = round(float(lows.iloc[-50:-2].min()), 2)
        swing_sl_sell = round(float(highs.iloc[-50:-2].max()), 2)
        print("Fibo MARKET=" + fib_label + "=" + str(limit_market)
              + " SNIPER=0.618=" + str(limit_sniper), flush=True)
        return {
            "ob":            ob,
            "fib_382":       fib_382,
            "fib_500":       fib_500,
            "fib_618":       fib_618,
            "limit":         limit_market,
            "limit_market":  limit_market,
            "limit_sniper":  limit_sniper,
            "fib_label":     fib_label,
            "limit_low":     round(limit_market - tol, 2),
            "limit_high":    round(limit_market + tol, 2),
            "golden_pocket": str(min(fib_500,fib_618))+"-"+str(max(fib_500,fib_618)),
            "swing_sl_buy":  swing_sl_buy,
            "swing_sl_sell": swing_sl_sell,
            "is_strong":     is_strong,
        }
    except Exception as e:
        print("get_sniper_levels : " + str(e), flush=True)
        return None

def calc_sniper_option(direction, entry, sl_mkt, levels, atr_val=None):
    """
    SNIPER : Fibo 0.618 + SL hybride OB+Swing.
    Buffer spread = 22 pips (0.22 points confirme RaiseFX).
    Amelioration minimum 80 pips (0.80 points).
    """
    BUFFER   = 22 * PIP_GOLD  # spread reel RaiseFX
    SL_MULT  = 1.2
    if levels is None: return None
    try:
        ob          = levels["ob"]
        limit_price = levels["limit_sniper"]
        fib_618     = levels["fib_618"]
        if direction == "BUY":
            improvement = round((entry - limit_price) / PIP_GOLD, 1)
            if limit_price >= entry or improvement < 80:
                print("SNIPER : amelioration insuffisante (" + str(improvement) + " pips)", flush=True)
                return None
            sl_mkt_adj = sl_mkt
            if sl_mkt_adj >= limit_price - BUFFER:
                sl_mkt_adj = round(limit_price - BUFFER - PIP_GOLD, 2)
            sl_ob     = round(ob["low"] - ob["size"] * SL_MULT * 0.1, 2)
            sl_swing  = levels.get("swing_sl_buy", sl_ob)
            sl_sniper = round(min(sl_ob, sl_swing)
                              if abs(entry-sl_swing)/PIP_GOLD < SL_MAX_PIPS
                              else sl_ob, 2)
        else:
            improvement = round((limit_price - entry) / PIP_GOLD, 1)
            if limit_price <= entry or improvement < 80:
                print("SNIPER : amelioration insuffisante (" + str(improvement) + " pips)", flush=True)
                return None
            sl_mkt_adj = sl_mkt
            if sl_mkt_adj <= limit_price + BUFFER:
                sl_mkt_adj = round(limit_price + BUFFER + PIP_GOLD, 2)
            sl_ob     = round(ob["high"] + ob["size"] * SL_MULT * 0.1, 2)
            sl_swing  = levels.get("swing_sl_sell", sl_ob)
            sl_sniper = round(max(sl_ob, sl_swing)
                              if abs(entry-sl_swing)/PIP_GOLD < SL_MAX_PIPS
                              else sl_ob, 2)
        sl_dist = abs(limit_price - sl_sniper) / PIP_GOLD
        if sl_dist > SL_MAX_PIPS or sl_dist < 3:
            print("SNIPER : SL hors limites", flush=True); return None
        session = get_current_session()
        rr      = 2.0 if (session and session["premarket"]) else TP_RR_SNIPER
        tp_dist = sl_dist * rr
        tp_sn   = round(limit_price + tp_dist*PIP_GOLD, 2) if direction=="BUY" \
                  else round(limit_price - tp_dist*PIP_GOLD, 2)
        sl_mkt_d = abs(entry - sl_mkt_adj) / PIP_GOLD
        tp_mkt   = round(entry + sl_mkt_d*TP_RR_MARKET*PIP_GOLD, 2) if direction=="BUY" \
                   else round(entry - sl_mkt_d*TP_RR_MARKET*PIP_GOLD, 2)
        print("SNIPER VALIDE +" + str(improvement) + " pips RR 1:" + str(rr), flush=True)
        return {
            "limit":        limit_price,
            "limit_low":    levels.get("limit_low", limit_price),
            "limit_high":   levels.get("limit_high", limit_price),
            "golden_pocket":levels.get("golden_pocket",""),
            "fib_618":      fib_618,
            "sl":           sl_sniper,
            "tp":           tp_sn,
            "sl_pips":      round(sl_dist, 1),
            "tp_pips":      round(tp_dist, 1),
            "improvement":  improvement,
            "ob_zone":      str(ob["low"]) + "-" + str(ob["high"]),
            "rr":           rr,
            "sl_mkt_adj":   sl_mkt_adj,
            "tp_mkt_adj":   tp_mkt,
            "sl_mkt_pips":  round(sl_mkt_d, 1),
            "tp_mkt_pips":  round(sl_mkt_d * TP_RR_MARKET, 1),
        }
    except Exception as e:
        print("calc_sniper_option : " + str(e), flush=True)
        return None

# ============================================================
#  ANALYSE PRINCIPALE - ARCHITECTURE M1 + HTF
#  M1  : declencheur d'entree (retest EMA20, score, OB)
#  M5  : OB M5 majeur + volume
#  M15 : OB M15 institutional
#  H1  : EMA200 tendance maitre + tendance forte
#  H4  : alignement macro
# ============================================================

async def analyse_gold(account):
    try:
        # Recuperation donnees toutes timeframes
        df_m1  = await get_candles(account, SYMBOL, "1m",  300)
        df_m5  = await get_candles(account, SYMBOL, "5m",  300)
        df_m15 = await get_candles(account, SYMBOL, "15m", 200)
        df_h1  = await get_candles(account, SYMBOL, "1h",  300)

        # Verification donnees suffisantes
        for nm, df, n in [("M1",df_m1,55),("M5",df_m5,55),
                          ("M15",df_m15,55),("H1",df_h1,200)]:
            if df is None or len(df) < n:
                print(nm + " insuffisant (" + str(len(df) if df is not None else 0) + ")", flush=True)
                return None

        # H4 resample depuis H1
        df_h4 = df_h1.resample("4h", on=df_h1.index.name
                                if df_h1.index.name else None).agg(
            {"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}
        ).dropna() if hasattr(df_h1.index, 'freq') else \
            df_h1.copy().assign(
                idx=pd.RangeIndex(len(df_h1))
            ).groupby(pd.RangeIndex(len(df_h1))//4).agg(
                {"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}
            )
        if len(df_h4) < 20:
            print("H4 insuffisant", flush=True); return None

        # Prix reel broker
        bid, ask, spread = await get_price(account, SYMBOL)
        if spread:
            print("Spread RaiseFX : " + str(spread) + " pips", flush=True)

        # ── ETAPE 1 : EMA200 H1 - tendance maitre ────────────
        ema200_h1 = float(calc_ema(df_h1["Close"].squeeze(), 200).iloc[-2])
        price_h1  = float(df_h1["Close"].squeeze().iloc[-2])
        if price_h1 > ema200_h1:   direction = "BUY"
        elif price_h1 < ema200_h1: direction = "SELL"
        else:
            print("EMA200 neutre - stop", flush=True); return None
        expected = 1 if direction == "BUY" else -1
        print("EMA200 H1 : " + direction, flush=True)

        # ── ETAPE 2 : Cooldown ───────────────────────────────
        if not is_signal_allowed(direction): return None

        # ── ETAPE 3 : Alignement H4+H1+M15+M5 ───────────────
        for nm, df in [("H4",df_h4),("H1",df_h1),("M15",df_m15),("M5",df_m5)]:
            b = ema_bias(df, nm)
            if b == 0 or b != expected:
                print(nm + " non aligne - stop", flush=True); return None
        print("EMA alignes H4+H1+M15+M5", flush=True)

        # ── ETAPE 3b : Tendance forte (Fibo 0.382) ───────────
        trend_status = is_strong_trend_gold(df_h1)

        # ── ETAPE 4 : Bougie M1 cloturee (iloc[-2]) ──────────
        c_m1 = df_m1["Close"].squeeze(); o_m1 = df_m1["Open"].squeeze()
        h_m1 = df_m1["High"].squeeze();  l_m1 = df_m1["Low"].squeeze()
        p = float(c_m1.iloc[-2]); o = float(o_m1.iloc[-2])
        h = float(h_m1.iloc[-2]); l = float(l_m1.iloc[-2])
        if any(v != v for v in [p,o,h,l]):
            print("NaN sur bougie M1 - stop", flush=True); return None

        # ── ETAPE 5 : ATR M1 + anti-panique ─────────────────
        atr = float(calc_atr(df_m1, ATR_PERIOD).iloc[-2])
        if atr != atr or atr <= 0:
            print("ATR invalide - stop", flush=True); return None
        if (h - l) > ATR_SPIKE_MULT * atr:
            print("Panique ATR M1 - stop", flush=True); return None

        # ── ETAPE 6 : Retest EMA20 M1 ───────────────────────
        ema20_m1 = float(calc_ema(c_m1, 20).iloc[-2])
        ecart    = abs(p - ema20_m1)
        if ecart > RETEST_THRESH:
            print("Pas de retest EMA20 M1 (" + str(round(ecart,2))
                  + " > " + str(RETEST_THRESH) + ") - stop", flush=True)
            return None
        print("Retest EMA20 M1 OK : " + str(round(ecart,2)) + " points", flush=True)

        # ── ETAPE 7 : SL MARKET ──────────────────────────────
        sl_pips = (SL_ATR_MULT * atr) / PIP_GOLD
        if sl_pips < SL_MIN_PIPS: sl_pips = SL_MIN_PIPS
        if sl_pips > SL_MAX_PIPS:
            print("SL trop grand - stop", flush=True); return None
        sl_pts = sl_pips * PIP_GOLD
        sl_mkt = round(p - sl_pts, 2) if direction=="BUY" else round(p + sl_pts, 2)
        tp_mkt = round(p + sl_pts*TP_RR_MARKET, 2) if direction=="BUY" \
                 else round(p - sl_pts*TP_RR_MARKET, 2)

        # ── ETAPE 8 : SCORE DE CONFLUENCE STRATIFIE ──────────
        score     = 0
        score_log = []

        # Score Volume M1 (30 pts) - tick volume MetaApi
        vol      = df_m1["Volume"].squeeze()
        vol_sig  = float(vol.iloc[-2]) if len(vol) > 2 else 0
        vol_avg  = float(vol.iloc[-21:-2].mean()) if len(vol) > 21 else 0
        vol_ok   = vol_sig > vol_avg * VOL_THRESHOLD if vol_avg > 0 \
                   else (h - l) > atr * 0.8
        vol_src  = "TickVol M1 RaiseFX"
        if vol_ok:
            score += SCORE_VOL
            score_log.append("Vol+" + str(SCORE_VOL))
        else:
            score_log.append("Vol+0")

        # Score RSI M1 (25 pts) — v3: zone stricte 45-55 + extremes absolus
        rsi = float(calc_rsi(c_m1, 14).iloc[-2])
        print("RSI M1=" + str(round(rsi,1)), flush=True)
        if direction == "BUY" and rsi >= 75:
            print("RSI extreme >75 - OBLIGATOIRE bloque", flush=True); return None
        if direction == "SELL" and rsi <= 25:
            print("RSI extreme <25 - OBLIGATOIRE bloque", flush=True); return None
        # v3: filtre zone neutre uniquement (hors 45-55 = 18% WR en backtest)
        if rsi < RSI_MIN or rsi > RSI_MAX:
            print("RSI hors zone neutre (" + str(round(rsi,1))
                  + " | zone=" + str(RSI_MIN) + "-" + str(RSI_MAX)
                  + ") - stop v3", flush=True)
            return None
        if (direction=="BUY" and rsi < 70) or (direction=="SELL" and rsi > 30):
            score += SCORE_RSI
            score_log.append("RSI+" + str(SCORE_RSI))
        else:
            score_log.append("RSI+0")

        # Score DXY (25 pts) - N/A si non disponible
        dxy_k = 0.0; dxy_t = "N/A"
        DXY_SYMBOL = os.getenv("DXY_SYMBOL", "USDX")
        try:
            df_dxy = await get_candles(account, DXY_SYMBOL, "1h", 50)
            if df_dxy is not None and len(df_dxy) >= 20:
                k, d = calc_stochastic(df_dxy)
                dxy_k = k
                if k > STOCH_OB or k < STOCH_OS:
                    print("DXY zone extreme - OBLIGATOIRE bloque", flush=True)
                    return None
                dxy_ok_flag = not (
                    (direction=="BUY" and k < STOCH_GAP_LOW) or
                    (direction=="SELL" and k > STOCH_GAP_HIGH) or
                    abs(k-d) < DXY_KD_MIN_GAP
                )
                if dxy_ok_flag:
                    score += SCORE_DXY_GAP
                    score_log.append("DXY+" + str(SCORE_DXY_GAP))
                    dxy_t = "haussier" if k > d else "baissier"
                else:
                    score_log.append("DXY+0")
            else:
                score_log.append("DXY:N/A")
        except Exception as e:
            print("DXY erreur : " + str(e), flush=True)
            score_log.append("DXY:N/A")

        # Score Momentum M1 (20 pts)
        momentum_ok = False
        if len(h_m1) > 3:
            prev_mid = (float(h_m1.iloc[-3]) + float(l_m1.iloc[-3])) / 2
            momentum_ok = (direction=="BUY" and p >= prev_mid) or \
                          (direction=="SELL" and p <= prev_mid)
        if momentum_ok:
            score += SCORE_MOMENTUM
            score_log.append("Mom+" + str(SCORE_MOMENTUM))
        else:
            score_log.append("Mom+0")

        # Verdict score
        print("Score : " + str(score) + "/100 ["
              + " | ".join(score_log) + "] min=" + str(SCORE_MIN), flush=True)
        if score < SCORE_MIN:
            print("Score insuffisant - stop", flush=True); return None

        # ── ETAPE 9 : OB M1 + M5 + M15 + Fibo dual ──────────
        levels_m1  = get_sniper_levels(df_m1,  direction, atr_val=atr, trend_status=trend_status)
        levels_m5  = get_sniper_levels(df_m5,  direction, atr_val=atr, trend_status=trend_status)
        levels_m15 = get_sniper_levels(df_m15, direction, atr_val=atr, trend_status=trend_status)

        # Priorite M15 > M5 > M1 (plus fort institutionnellement)
        if levels_m15 is not None:
            levels_best = levels_m15; ob_tf = "M15 MAJEUR"
        elif levels_m5 is not None:
            levels_best = levels_m5;  ob_tf = "M5"
        else:
            levels_best = levels_m1;  ob_tf = "M1"
        print("OB selectionne : " + ob_tf, flush=True)

        sniper = calc_sniper_option(direction, p, sl_mkt, levels_best, atr_val=atr)

        register_signal(direction)
        gc.collect()
        print("SIGNAL VALIDE " + direction + " @ " + str(round(p,2))
              + " [" + trend_status + "] OB " + ob_tf, flush=True)

        return {
            "dir":         direction,
            "p":           round(p, 2),
            "bid":         bid,
            "ask":         ask,
            "spread":      spread,
            "sl_mkt":      sl_mkt,
            "tp_mkt":      tp_mkt,
            "sl_pips":     round(sl_pips, 1),
            "tp_pips":     round(sl_pips * TP_RR_MARKET, 1),
            "rsi":         round(rsi, 1),
            "ema200":      round(ema200_h1, 2),
            "ema20_m1":    round(ema20_m1, 2),
            "ecart":       round(ecart, 2),
            "dxy_k":       round(dxy_k, 1),
            "dxy_t":       dxy_t,
            "atr":         round(atr, 2),
            "vol_src":     vol_src,
            "score":       score,
            "score_log":   " | ".join(score_log),
            "ob_tf":       ob_tf,
            "trend_status":trend_status,
            "fib_label":   levels_best["fib_label"] if levels_best else "0.500",
            "limit_market":levels_best["limit_market"] if levels_best else None,
            "session":     get_session_label(),
            "sniper":      sniper,
        }
    except Exception as e:
        print("analyse_gold ERREUR : " + str(e), flush=True)
        return None

# ============================================================
#  BOUCLE M1 - attend la prochaine bougie M1 cloturee
# ============================================================

async def wait_next_m1():
    """Synchronise sur la prochaine bougie M1."""
    now  = datetime.now(PARIS_TZ)
    wait = 60 - now.second
    if wait <= 2: wait += 60
    print("Prochaine M1 dans " + str(wait) + "s", flush=True)
    await asyncio.sleep(wait)

# ============================================================
#  BOUCLE PRINCIPALE
# ============================================================

async def main():
    print("="*60, flush=True)
    print("XAU/USD SNIPER v4.3 - SIMULATION METAAPI M1 [v3]", flush=True)
    print("Broker  : RaiseFX | Symbole : " + SYMBOL, flush=True)
    print("Score   : " + str(SCORE_MIN) + "/100 | RSI zone : "
          + str(RSI_MIN) + "-" + str(RSI_MAX), flush=True)
    print("Sessions: Pre-Londres + Matin + Pre-NY + Apres-midi", flush=True)
    print("PIP     : " + str(PIP_GOLD) + " | ATR ref : 4.635 pts", flush=True)
    print("ZERO ORDRE REEL - LOGS TERMINAL UNIQUEMENT", flush=True)
    print("="*60, flush=True)

    if not META_TOKEN:
        print("ERREUR : META_API_TOKEN non defini", flush=True)
        print("Railway > Settings > Variables > META_API_TOKEN", flush=True)
        return

    try:
        from metaapi_cloud_sdk import MetaApi
    except ImportError:
        print("ERREUR : metaapi-cloud-sdk non installe", flush=True)
        return

    api     = MetaApi(META_TOKEN)
    account = await api.metatrader_account_api.get_account(META_ACCT)

    print("Connexion RaiseFX...", flush=True)
    await account.wait_connected()
    print("Connecte a RaiseFX - " + META_ACCT[:8] + "...", flush=True)

    send_msg(
        "XAU/USD SNIPER v4.3 - SIMULATION M1 [ENGINE v3]\n"
        + "Broker  : RaiseFX | Gold CFD\n"
        + "Score   : " + str(SCORE_MIN) + "/100 (Mom obligatoire)\n"
        + "RSI     : zone " + str(RSI_MIN) + "-" + str(RSI_MAX) + " stricte\n"
        + "Fibo    : 0.382 tendance forte / 0.618 Sniper\n"
        + "OB      : M15 > M5 > M1 (priorite institutionnelle)\n"
        + "Sessions: Pre-Londres + Matin + Pre-NY + Apres-midi\n"
        + "** SIMULATION - aucun ordre reel **"
    )

    while True:
        try:
            await wait_next_m1()
            now_str = datetime.now(PARIS_TZ).strftime("%H:%M")

            if not is_market_open():
                print("[" + now_str + "] Weekend - ferme", flush=True)
                continue
            if not is_in_session():
                print("[" + now_str + "] Hors session", flush=True)
                continue

            print("[" + now_str + "] " + get_session_label() + " - analyse M1...", flush=True)
            s = await analyse_gold(account)

            if s:
                d         = "ACHAT" if s["dir"]=="BUY" else "VENTE"
                sn        = s["sniper"]
                trend_tag = " [TENDANCE FORTE]" if "STRONG" in s.get("trend_status","") else ""
                fib_mkt   = s.get("fib_label", "0.500")
                lim_mkt   = s.get("limit_market", s["p"])
                sl_show   = sn["sl_mkt_adj"] if sn and "sl_mkt_adj" in sn else s["sl_mkt"]
                tp_show   = sn["tp_mkt_adj"] if sn and "tp_mkt_adj" in sn else s["tp_mkt"]
                slp_show  = sn["sl_mkt_pips"] if sn and "sl_mkt_pips" in sn else s["sl_pips"]
                tpp_show  = sn["tp_mkt_pips"] if sn and "tp_mkt_pips" in sn else s["tp_pips"]

                # Log terminal
                print("\n" + "="*58, flush=True)
                print("SIGNAL SIMULATION" + trend_tag + " - " + d, flush=True)
                print("Bid/Ask : " + str(s["bid"]) + "/" + str(s["ask"])
                      + " Spread=" + str(s["spread"]) + " pips", flush=True)
                print("Fibo MARKET : " + fib_mkt + " = " + str(lim_mkt), flush=True)
                print("Entree : " + str(s["p"]), flush=True)
                print("Stop   : " + str(sl_show) + " (" + str(slp_show) + " pips)", flush=True)
                print("Cible  : " + str(tp_show) + " (" + str(tpp_show) + " pips)", flush=True)
                if sn:
                    print("SNIPER : " + str(sn["limit"]) + " +"
                          + str(sn["improvement"]) + " pips | Fib618=" + str(sn["fib_618"]), flush=True)
                print("Score  : " + str(s["score"]) + "/100 [" + s["score_log"] + "]", flush=True)
                print("OB     : " + s["ob_tf"], flush=True)
                print("="*58 + "\n", flush=True)

                # Telegram
                msg = ("[SIM] XAU/USD SNIPER v4.3" + trend_tag + " - " + d + "\n"
                       + "Broker : RaiseFX | Spread : " + str(s["spread"]) + " pips\n"
                       + "Fibo MARKET : " + fib_mkt + "\n"
                       + "\n"
                       + "⚡ OPTION MARKET\n"
                       + "Entree : " + str(s["p"]) + "\n"
                       + "Stop   : " + str(sl_show)
                       + " (" + str(slp_show) + " pips | SL ATR)\n"
                       + "Cible  : " + str(tp_show)
                       + " (" + str(tpp_show) + " pips | RR 1:" + str(TP_RR_MARKET) + ")\n"
                       + "\n")
                if sn:
                    gp = sn.get("golden_pocket","")
                    msg += ("🎯 OPTION SNIPER (Fibo 0.618)\n"
                            + "Entree : " + str(sn["limit"])
                            + " (+" + str(sn["improvement"]) + " pips)\n"
                            + "Stop   : " + str(sn["sl"])
                            + " (" + str(sn["sl_pips"]) + " pips | SL OB+4h)\n"
                            + "Cible  : " + str(sn["tp"])
                            + " (" + str(sn["tp_pips"]) + " pips | RR 1:" + str(sn["rr"]) + ")\n"
                            + "OB " + s["ob_tf"] + " : " + str(sn["ob_zone"]) + "\n"
                            + ("Golden Pocket : " + str(gp) + "\n" if gp else "")
                            + "Fibo 0.618 : " + str(sn["fib_618"]) + "\n"
                            + "\n")
                else:
                    msg += "🎯 SNIPER : pas de confluence OB/Fibo\n\n"
                msg += ("EMA200 H1 : " + str(s["ema200"]) + "\n"
                        + "Retest M1 : " + str(s["ecart"]) + " pts de EMA20\n"
                        + "RSI M1    : " + str(s["rsi"]) + "\n"
                        + "DXY       : K=" + str(s["dxy_k"]) + " (" + s["dxy_t"] + ")\n"
                        + "Score     : " + str(s["score"]) + "/100 ["
                        + s["score_log"] + "]\n"
                        + "Session   : " + s["session"] + "\n"
                        + "** SIMULATION - aucun ordre place **")
                send_msg(msg)
            else:
                print("[" + now_str + "] Pas de signal", flush=True)

            gc.collect()

        except Exception as e:
            print("BOUCLE ERREUR : " + str(e), flush=True)
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
