import yfinance as yf
import pandas as pd
import pandas_ta as ta
import pytz
import requests
from datetime import datetime
import warnings

# Désactivation des alertes inutiles
warnings.filterwarnings('ignore')

# --- CONFIGURATION (Identifiants vérifiés) ---
TOKEN = "8218163213:AAEDXu19mXfeUSM65JIZAiBucxUAxmRHwy4"
CHAT_ID = "1432682636"
TZ_PARIS = pytz.timezone('Europe/Paris')

def send_telegram(message):
    """Envoi sécurisé vers Telegram"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload, timeout=15)
    except Exception as e:
        print(f"Erreur envoi Telegram : {e}")

def run_sniper():
    # 1. RÉGULATION HORAIRE (Heure de Paris)
    now_paris = datetime.now(TZ_PARIS)
    h_dec = now_paris.hour + (now_paris.minute / 60)
    
    # Sessions V4.0 : 9h-13h et 14h30-19h
    is_session = (9.0 <= h_dec <= 13.0) or (14.5 <= h_dec <= 19.0)
    
    if not is_session:
        print(f"[{now_paris.strftime('%H:%M')}] 🌙 Hors session : Le bot dort.")
        return

    # 2. RÉCUPÉRATION DES DONNÉES (Multi-Timeframe)
    try:
        # Téléchargement Gold (M5 pour entrée, H1 pour tendance)
        g_m5 = yf.download("GC=F", period="2d", interval="5m", progress=False, auto_adjust=True)
        g_h1 = yf.download("GC=F", period="10d", interval="1h", progress=False, auto_adjust=True)
        # Téléchargement Dollar (H1 pour filtre)
        dxy = yf.download("DX-Y.NYB", period="10d", interval="1h", progress=False, auto_adjust=True)

        # Nettoyage des colonnes (Correction bug Multi-Index)
        for df in [g_m5, g_h1, dxy]:
            if df.empty:
                print("Erreur : Un des datasets est vide.")
                return
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
    except Exception as e:
        print(f"⚠️ Erreur Data Yahoo : {e}")
        return

    # 3. CALCUL DES INDICATEURS (Règles V4.3)
    # Tendance H4 (via EMA 200 sur H1)
    ema_200_h1 = ta.ema(g_h1['Close'], length=200).iloc[-1]
    
    # Retest M5 (EMA 20 sur M5)
    prix_actuel = g_m5['Close'].iloc[-1]
    ema_20_m5 = ta.ema(g_m5['Close'], length=20).iloc[-1]
    
    # Volume (Volume actuel vs Moyenne des 20 dernières bougies)
    vol_actuel = g_m5['Volume'].iloc[-1]
    vol_moyen = g_m5['Volume'].rolling(20).mean().iloc[-1]
    
    # Filtre Dollar (DXY Stochastique K)
    stoch_dxy = ta.stoch(dxy['High'], dxy['Low'], dxy['Close'])
    k_dxy = stoch_dxy['STOCHk_14_3_3'].iloc[-1]

    # 4. VÉRIFICATION DES CONDITIONS SNIPER
    # Règle du retest : prix à moins de 35 cents de l'EMA 20
    retest_ok = abs(prix_actuel - ema_20_m5) < 0.35
    # Règle du volume : au moins 80% de la moyenne
    volume_ok = vol_actuel > (vol_moyen * 0.8)
    
    # --- LOGIQUE DE DÉCISION ---
    signal = None
    
    # ACHAT : Prix > Tendance + DXY sain + Retest + Volume
    if prix_actuel > ema_200_h1 and k_dxy > 20 and retest_ok and volume_ok:
        signal = "🎯 ACHAT GOLD (Retest EMA20)"

    # VENTE : Prix < Tendance + DXY sain + Retest + Volume
    elif prix_actuel < ema_200_h1 and k_dxy < 80 and retest_ok and volume_ok:
        signal = "📉 VENTE GOLD (Retest EMA20)"

    # 5. RÉSULTAT
    if signal:
        msg = f"✨ SIGNAL V4.3 - {now_paris.strftime('%H:%M')}\n\n💰 Prix: {prix_actuel:.2f}\n📝 {signal}\n📈 Tendance: {'HAUSSE' if prix_actuel > ema_200_h1 else 'BAISSE'}"
        send_telegram(msg)
        print(f"✅ {signal} envoyé à Telegram !")
    else:
        print(f"[{now_paris.strftime('%H:%M')}] Scan OK. Prix: {prix_actuel:.2f}. Pas de signal (Attente retest ou volume).")

if __name__ == "__main__":
    run_sniper()
