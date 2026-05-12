import yfinance as yf
import pandas as pd
import pandas_ta as ta
import pytz
import requests
from datetime import datetime
import warnings

# Suppression des alertes pour un log propre sur GitHub
warnings.filterwarnings('ignore')

# --- TES IDENTIFIANTS FINAUX ---
TOKEN = "8218163213:AAEDXu19mXfeUSM65JIZAiBucxUAxmRHwy4"
CHAT_ID = "1432682636"

# --- CONFIGURATION MARCHÉ ---
SYMBOL_GOLD = "GC=F"
SYMBOL_DXY = "DX-Y.NYB"
TZ_PARIS = pytz.timezone('Europe/Paris')

def send_telegram(message):
    """Envoie la notification au bot Telegram"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
    try:
        requests.get(url, timeout=15)
    except:
        pass

def check_signal():
    # 1. VERROU HORAIRE V4.0 (Session Londres et NY)
    now_paris = datetime.now(TZ_PARIS)
    h_dec = now_paris.hour + (now_paris.minute / 60)
    
    # Matin : 9h00-13h00 | Après-midi : 14h30-19h00
    is_session = (9.0 <= h_dec <= 13.0) or (14.5 <= h_dec <= 19.0)
    if not is_session:
        return "MODE_VEILLE", 0

    # 2. RÉCUPÉRATION DES DONNÉES
    try:
        g_m5 = yf.download(SYMBOL_GOLD, period="2d", interval="5m", progress=False, auto_adjust=True)
        g_h1 = yf.download(SYMBOL_GOLD, period="10d", interval="1h", progress=False, auto_adjust=True)
        d_h1 = yf.download(SYMBOL_DXY, period="10d", interval="1h", progress=False, auto_adjust=True)
        
        for df in [g_m5, g_h1, d_h1]:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
    except:
        return "ERREUR_DATA", 0

    # 3. CALCULS TECHNIQUES (V4.3 SNIPER)
    # Tendance H4 (EMA 200 sur 1H)
    ema_tendance = ta.ema(g_h1['Close'], length=200).iloc[-1]
    
    # Setup M5 (Retest EMA 20)
    prix_actuel = g_m5['Close'].iloc[-1]
    ema20_m5 = ta.ema(g_m5['Close'], length=20).iloc[-1]
    
    # Filtre de Volume
    vol_actuel = g_m5['Volume'].iloc[-1]
    vol_moyen = g_m5['Volume'].rolling(window=20).mean().iloc[-1]
    
    # Filtre Dollar DXY (Stochastique 1H)
    stoch_dxy = ta.stoch(d_h1['High'], d_h1['Low'], d_h1['Close'], k=14, d=3)
    k_dxy = stoch_dxy['STOCHk_14_3_3'].iloc[-1]

    # 4. CONDITIONS DU SIGNAL
    proche_ema = abs(prix_actuel - ema20_m5) < 0.35
    vol_ok = vol_actuel > (vol_moyen * 0.8)
    
    signal = None
    
    if prix_actuel > ema_tendance and k_dxy > 20:
        if proche_ema and vol_ok:
            signal = "ACHAT 🎯 (Retest EMA20)"
            
    elif prix_actuel < ema_tendance and k_dxy < 80:
        if proche_ema and vol_ok:
            signal = "VENTE 📉 (Retest EMA20)"
            
    return signal, prix_actuel

# --- EXÉCUTION DU SCRIPT ---
sig, prix = check_signal()

if sig and sig not in ["MODE_VEILLE", "ERREUR_DATA"]:
    h = datetime.now(TZ_PARIS).strftime("%H:%M")
    message = f"✨ [{h}] SIGNAL GOLD V4.3\n\n💰 Prix: {prix:.2f}\n📝 Type: {sig}"
    send_telegram(message)
