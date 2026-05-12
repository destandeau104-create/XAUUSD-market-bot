import yfinance as yf
import pandas as pd
import pandas_ta as ta
import pytz
import requests
import time
from datetime import datetime
import warnings

# Masquer les alertes inutiles
warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURATION - TES INFOS
# ==========================================
TOKEN = "8218163213:AAEDXu19mXfeUSM65JIZAiBucxUAxmRHwy4"
CHAT_ID = "1432682636"
SYMBOL_GOLD = "GC=F"
SYMBOL_DXY = "DX-Y.NYB"
TZ_PARIS = pytz.timezone('Europe/Paris')

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
    try:
        requests.get(url, timeout=10)
    except:
        pass

def check_signal():
    # 1. Vérification Horaire (Heure de Paris)
    now_paris = datetime.now(TZ_PARIS)
    h_dec = now_paris.hour + (now_paris.minute / 60)
    
    # Sessions : 9h-13h et 14h30-19h
    is_session = (9.0 <= h_dec <= 13.0) or (14.5 <= h_dec <= 19.0)
    if not is_session:
        return "MODE_VEILLE", 0

    # 2. Récupération des données Marché
    try:
        g_m5 = yf.download(SYMBOL_GOLD, period="2d", interval="5m", progress=False, auto_adjust=True)
        g_h1 = yf.download(SYMBOL_GOLD, period="10d", interval="1h", progress=False, auto_adjust=True)
        d_h1 = yf.download(SYMBOL_DXY, period="10d", interval="1h", progress=False, auto_adjust=True)
        
        for df in [g_m5, g_h1, d_h1]:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
    except:
        return "ERREUR_DATA", 0

    # 3. Calculs Stratégie V4.3
    # Tendance H4 (via H1 EMA 200)
    ema_tendance = ta.ema(g_h1['Close'], length=200).iloc[-1]
    
    # Prix actuel et EMA20 M5
    prix_actuel = g_m5['Close'].iloc[-1]
    ema20_m5 = ta.ema(g_m5['Close'], length=20).iloc[-1]
    
    # Volume
    vol_actuel = g_m5['Volume'].iloc[-1]
    vol_moyen = g_m5['Volume'].rolling(window=20).mean().iloc[-1]
    
    # Filtre Dollar (DXY)
    stoch_dxy = ta.stoch(d_h1['High'], d_h1['Low'], d_h1['Close'], k=14, d=3)
    k_dxy = stoch_dxy['STOCHk_14_3_3'].iloc[-1]

    # 4. Conditions de Sniper
    proche_ema = abs(prix_actuel - ema20_m5) < 0.35
    vol_ok = vol_actuel > (vol_moyen * 0.8)
    
    signal = None
    # ACHAT : Prix > Tendance + DXY pas trop haut + Retest EMA + Volume
    if prix_actuel > ema_tendance and k_dxy > 20 and proche_ema and vol_ok:
        signal = "ACHAT 🎯 (Retest EMA20)"
    # VENTE : Prix < Tendance + DXY pas trop bas + Retest EMA + Volume
    elif prix_actuel < ema_tendance and k_dxy < 80 and proche_ema and vol_ok:
        signal = "VENTE 📉 (Retest EMA20)"
            
    return signal, prix_actuel

# ==========================================
# BOUCLE INFINIE (Le Bot reste allumé)
# ==========================================
print("🚀 BOT GOLD V4.3 ACTIVÉ - SCAN EN CONTINU")
print("Appuyez sur 'Stop' pour arrêter")

while True:
    try:
        sig, prix = check_signal()
        heure_actuelle = datetime.now(TZ_PARIS).strftime("%H:%M:%S")
        
        if sig and sig not in ["MODE_VEILLE", "ERREUR_DATA"]:
            msg = f"✨ [{heure_actuelle}] SIGNAL GOLD V4.3\n\n💰 Prix: {prix:.2f}\n📝 Type: {sig}"
            send_telegram(msg)
            print(f"[{heure_actuelle}] ✅ Signal envoyé !")
            time.sleep(300) # Attend 5 min après un signal
        
        elif sig == "MODE_VEILLE":
            print(f"[{heure_actuelle}] 🌙 En sommeil (Hors session)...", end="\r")
        
        else:
            print(f"[{heure_actuelle}] 🔍 Scan en cours... Prix: {prix:.2f}", end="\r")
            
        time.sleep(60) # Scan toutes les minutes
        
    except Exception as e:
        print(f"\n⚠️ Erreur : {e}")
        time.sleep(10)
