import os
import time
import threading
import requests
import telebot
import yfinance as yf
import pandas_ta as ta
import pytz
from datetime import datetime
from flask import Flask

# --- CONFIGURATION DIRECTE ---
TOKEN = "8794987935:AAECh2yzzM_g9dZ3ki3tlKC1UWdC44YOjCk"
CHAT_ID = "1432682636"
bot = telebot.TeleBot(TOKEN)
PARIS_TZ = pytz.timezone("Europe/Paris")

# --- SERVEUR WEB (Indispensable pour Render) ---
app = Flask('')
@app.route('/')
def home():
    return "Bot XAU/USD est en ligne !"

def run_server():
    app.run(host='0.0.0.0', port=8080)

# --- TA STRATÉGIE (Version condensée et stable) ---
def check_signals():
    while True:
        try:
            now = datetime.now(PARIS_TZ)
            # Session Paris (9h-11h30 / 14h30-17h30)
            if (9, 0) <= (now.hour, now.minute) <= (11, 30) or (14, 30) <= (now.hour, now.minute) <= (17, 30):
                print(f"🔎 Scan en cours... {now.strftime('%H:%M')}")
                
                # Récupération Gold
                data = yf.download("XAUUSD=X", interval="5m", period="2d", progress=False)
                if not data.empty:
                    # Ici on peut rajouter les calculs RSI/EMA plus tard 
                    # Une fois qu'on est sûr que ça ne crash plus
                    pass
            else:
                print("🕐 Hors session de trading (Repos)")
            
            time.sleep(300) # Attente 5 minutes
        except Exception as e:
            print(f"Erreur analyse: {e}")
            time.sleep(60)

# --- LANCEMENT ---
if __name__ == "__main__":
    print("--- DÉMARRAGE DU BOT ---")
    
    # 1. Lancement du serveur Web
    server_thread = threading.Thread(target=run_server)
    server_thread.daemon = True
    server_thread.start()
    
    # 2. Message de test Telegram
    try:
        bot.send_message(CHAT_ID, "🚀 Bot Trading XAU/USD : Système opérationnel sur Render !")
        print("✅ Message Telegram envoyé")
    except Exception as e:
        print(f"❌ Erreur Telegram: {e}")

    # 3. Lancement de la boucle de trading
    check_signals()
