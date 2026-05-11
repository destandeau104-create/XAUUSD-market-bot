import os
import threading
import time
import requests
import telebot
from flask import Flask

# --- CONFIGURATION ---
TOKEN = "8794987935:AAECh2yzzM_g9dZ3ki3tlKC1UWdC44YOjCk"
CHAT_ID = "1432682636"
bot = telebot.TeleBot(TOKEN)

# --- SERVEUR POUR RENDER ---
app = Flask('')
@app.route('/')
def home():
    return "Bot en ligne !"

def run_web():
    app.run(host='0.0.0.0', port=8080)

# --- BOUCLE DE TRADING (VERSION TEST) ---
def trading_loop():
    print("🚀 Boucle de trading lancée...")
    while True:
        # On simule un scan toutes les 5 minutes
        print("🔎 Scan du marché XAU/USD...")
        time.sleep(300)

# --- DÉMARRAGE ---
if __name__ == "__main__":
    print("--- DÉMARRAGE DU SCRIPT ---")
    
    # 1. Lancer le serveur web
    t_web = threading.Thread(target=run_web)
    t_web.daemon = True
    t_web.start()
    print("✅ Serveur Web actif")

    # 2. Envoyer un message Telegram pour confirmer
    try:
        bot.send_message(CHAT_ID, "✅ Bot XAU/USD connecté et opérationnel !")
        print("✅ Message de test envoyé")
    except Exception as e:
        print(f"❌ Erreur Telegram: {e}")

    # 3. Lancer le trading
    trading_loop()
