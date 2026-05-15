"""
DIAGNOSTIC COMPLET GOLD RAISEFX
Calcule les vraies valeurs pour calibrer le bot
Lance une fois, affiche tout, s'arrete
"""
import asyncio, os
import pandas as pd
import numpy as np
from datetime import datetime, timezone

META_TOKEN = os.getenv("META_API_TOKEN", "")
META_ACCT  = os.getenv("META_ACCOUNT_ID", "7fed6592-a20e-4542-8720-52c9618f16e5")
SYMBOL     = "Gold"

async def main():
    print("="*58, flush=True)
    print("DIAGNOSTIC COMPLET GOLD RAISEFX", flush=True)
    print("="*58, flush=True)

    if not META_TOKEN:
        print("ERREUR : META_API_TOKEN manquant", flush=True)
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
    print("Connecte !", flush=True)

    # ── Prix temps reel ──────────────────────────────────────
    print("\n── PRIX TEMPS REEL ──", flush=True)
    try:
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        price = await connection.get_symbol_price(SYMBOL)
        if price:
            bid    = float(price.get("bid", 0))
            ask    = float(price.get("ask", 0))
            spread = round(ask - bid, 5)
            print("Bid    : " + str(bid), flush=True)
            print("Ask    : " + str(ask), flush=True)
            print("Spread : " + str(spread) + " points", flush=True)
        await connection.close()
    except Exception as e:
        print("Prix erreur : " + str(e), flush=True)

    # ── Bougies M5 ──────────────────────────────────────────
    print("\n── ANALYSE BOUGIES M5 (300 bougies) ──", flush=True)
    try:
        now     = datetime.now(timezone.utc)
        candles = await account.get_historical_candles(SYMBOL, "5m", now, 300)
        if candles:
            rows = []
            for c in candles:
                try:
                    rows.append({
                        "Open":  float(c.get("open",  0)),
                        "High":  float(c.get("high",  0)),
                        "Low":   float(c.get("low",   0)),
                        "Close": float(c.get("close", 0)),
                    })
                except Exception: continue
            df = pd.DataFrame(rows)
            if not df.empty:
                h  = df["High"]
                l  = df["Low"]
                c  = df["Close"]
                df["range"] = h - l
                tr = pd.concat([
                    h - l,
                    (h - c.shift()).abs(),
                    (l - c.shift()).abs()
                ], axis=1).max(axis=1)
                atr14 = tr.rolling(14, min_periods=14).median()
                atr_val = float(atr14.iloc[-1])

                # EMA20 ecarts sur 50 dernieres bougies
                ema20  = c.ewm(span=20, min_periods=20, adjust=False).mean()
                ecarts = (c - ema20).abs()
                ecart_moyen = float(ecarts.iloc[-50:].mean())
                ecart_max   = float(ecarts.iloc[-50:].max())
                ecart_min   = float(ecarts.iloc[-50:].min())
                ecart_p30   = float(np.percentile(ecarts.iloc[-50:].dropna(), 30))

                prix_actuel = float(c.iloc[-1])
                decimales   = len(str(prix_actuel).split(".")[1]) if "." in str(prix_actuel) else 2
                pip         = 10 ** (-decimales)

                print("Prix actuel      : " + str(round(prix_actuel, 2)), flush=True)
                print("Decimales        : " + str(decimales), flush=True)
                print("PIP              : " + str(pip), flush=True)
                print("", flush=True)
                print("ATR 14 M5        : " + str(round(atr_val, 4)) + " points", flush=True)
                print("Range moyen M5   : " + str(round(float(df['range'].mean()), 4)) + " points", flush=True)
                print("Range max M5     : " + str(round(float(df['range'].max()), 4)) + " points", flush=True)
                print("", flush=True)
                print("Ecart EMA20 moyen : " + str(round(ecart_moyen, 4)) + " points", flush=True)
                print("Ecart EMA20 max   : " + str(round(ecart_max, 4)) + " points", flush=True)
                print("Ecart EMA20 min   : " + str(round(ecart_min, 4)) + " points", flush=True)
                print("Ecart EMA20 P30   : " + str(round(ecart_p30, 4)) + " points (seuil retest recommande)", flush=True)

                # Calcul parametres recommandes
                sl_min_rec    = round(atr_val * 1.5, 2)
                sl_max_rec    = round(atr_val * 15,  2)
                retest_rec    = round(ecart_p30, 2)
                sl_min_pips   = round(sl_min_rec / pip)
                sl_max_pips   = round(sl_max_rec / pip)

                print("\n" + "="*58, flush=True)
                print("PARAMETRES RECOMMANDES POUR main.py", flush=True)
                print("="*58, flush=True)
                print("PIP_GOLD        = " + str(pip), flush=True)
                print("SL_MIN_PIPS     = " + str(sl_min_pips) + "   # ATR x1.5 = " + str(sl_min_rec) + " points", flush=True)
                print("SL_MAX_PIPS     = " + str(sl_max_pips) + "  # ATR x15  = " + str(sl_max_rec) + " points", flush=True)
                print("RETEST_THRESH   = " + str(retest_rec) + "  # P30 ecart EMA20 = filtre optimal", flush=True)
                print("="*58, flush=True)
    except Exception as e:
        print("Bougies erreur : " + str(e), flush=True)

    print("\nDIAGNOSTIC TERMINE", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
