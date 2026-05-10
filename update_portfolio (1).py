#!/usr/bin/env python3
"""
RAPTOR PORTFOLIO OPTIMIZER v2.0
════════════════════════════════
Universo esteso a 25 ETF con copertura geografica globale.
Nuove categorie: EQUITY_ASIA, EQUITY_EM, GOLD, ENERGY, CASH (XEON rifugio).
Campo fiscal: ETP/ETF (informativo, non influenza ottimizzazione).

score_finale = macro_score × 0.55 + momentum_score × 0.30 + quality_score × 0.15
"""

import json, os, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

def install(pkg):
    os.system(f"pip install {pkg} --break-system-packages -q")

try:
    import numpy as np
except ImportError:
    install("numpy"); import numpy as np

try:
    import yfinance as yf
except ImportError:
    install("yfinance"); import yfinance as yf

# ── UNIVERSO ETF (25) ─────────────────────────────────────────────
UNIVERSE = [
    # ── EQUITY ETP (WisdomTree collateral-backed) ──────────────────
    {"ticker":"WWRD.MI","name":"WT World",              "cat":"EQUITY",     "sub":"GLOBAL",    "risk":1.0,"cur":"USD","fiscal":"ETP"},
    {"ticker":"WSPX.MI","name":"WT S&P 500",            "cat":"EQUITY",     "sub":"US",        "risk":1.0,"cur":"USD","fiscal":"ETP"},
    {"ticker":"WSPE.MI","name":"WT S&P 500 EUR Hed",    "cat":"EQUITY",     "sub":"US_EUR",    "risk":1.0,"cur":"EUR","fiscal":"ETP"},
    {"ticker":"WS5X.MI","name":"WT Euro Stoxx 50",      "cat":"EQUITY",     "sub":"EU",        "risk":1.0,"cur":"EUR","fiscal":"ETP"},
    {"ticker":"WNAS.MI","name":"WT Nasdaq-100",         "cat":"EQUITY",     "sub":"TECH",      "risk":1.0,"cur":"USD","fiscal":"ETP"},
    {"ticker":"SMEA.MI","name":"iShares Europe Small",  "cat":"EQUITY",     "sub":"EU_SMALL",  "risk":1.0,"cur":"EUR","fiscal":"ETF"},
    # ── EQUITY ASIA ────────────────────────────────────────────────
    {"ticker":"WPXJ.MI","name":"WT Japan EUR Hed",      "cat":"EQUITY_ASIA","sub":"JAPAN",     "risk":1.0,"cur":"EUR","fiscal":"ETP"},
    {"ticker":"XCHA.MI","name":"iShares China",         "cat":"EQUITY_ASIA","sub":"CHINA",     "risk":1.1,"cur":"USD","fiscal":"ETF"},
    {"ticker":"XASX.MI","name":"iShares Asia Pacific",  "cat":"EQUITY_ASIA","sub":"ASIA_PAC",  "risk":1.0,"cur":"USD","fiscal":"ETF"},
    # ── EQUITY EM ──────────────────────────────────────────────────
    {"ticker":"VFEM.MI","name":"Vanguard FTSE EM",      "cat":"EQUITY_EM",  "sub":"EM_BROAD",  "risk":1.1,"cur":"USD","fiscal":"ETF"},
    {"ticker":"EIMI.MI","name":"iShares MSCI EM",       "cat":"EQUITY_EM",  "sub":"EM_CORE",   "risk":1.1,"cur":"USD","fiscal":"ETF"},
    # ── EFFICIENT CORE 90/60 (WisdomTree) ─────────────────────────
    {"ticker":"NTSX.MI","name":"WT US Efficient Core",  "cat":"EFF_CORE",   "sub":"US_EC",     "risk":1.5,"cur":"USD","fiscal":"ETP"},
    {"ticker":"NTSG.MI","name":"WT Global Eff. Core",   "cat":"EFF_CORE",   "sub":"GLOBAL_EC", "risk":1.5,"cur":"USD","fiscal":"ETP"},
    {"ticker":"NTSZ.MI","name":"WT EM Efficient Core",  "cat":"EFF_CORE",   "sub":"EM_EC",     "risk":1.5,"cur":"USD","fiscal":"ETP"},
    {"ticker":"WRTY.MI","name":"WT Russell 2000 EC",    "cat":"EFF_CORE",   "sub":"SMALL_EC",  "risk":1.5,"cur":"USD","fiscal":"ETP"},
    # ── BOND HY (PIMCO) ────────────────────────────────────────────
    {"ticker":"STHY.MI","name":"PIMCO US ST HY USD",    "cat":"BOND_HY",    "sub":"US_HY",     "risk":1.0,"cur":"USD","fiscal":"ETF"},
    {"ticker":"STHE.MI","name":"PIMCO US ST HY EUR",    "cat":"BOND_HY",    "sub":"US_HY_EUR", "risk":1.0,"cur":"EUR","fiscal":"ETF"},
    {"ticker":"EUHI.MI","name":"PIMCO Euro ST HY",      "cat":"BOND_HY",    "sub":"EU_HY",     "risk":1.0,"cur":"EUR","fiscal":"ETF"},
    {"ticker":"EUHA.MI","name":"PIMCO Euro HY Acc",     "cat":"BOND_HY",    "sub":"EU_HY_A",   "risk":1.0,"cur":"EUR","fiscal":"ETF"},
    # ── BOND IG ────────────────────────────────────────────────────
    {"ticker":"PJS1.MI","name":"PIMCO Euro Short Mat",  "cat":"BOND_IG",    "sub":"EU_IG",     "risk":0.5,"cur":"EUR","fiscal":"ETF"},
    {"ticker":"XGIU.MI","name":"iShares Euro Govt Bond","cat":"BOND_IG",    "sub":"EU_GOVT",   "risk":0.7,"cur":"EUR","fiscal":"ETF"},
    # ── BOND EM (PIMCO) ────────────────────────────────────────────
    {"ticker":"EMLI.MI","name":"PIMCO EM Local Bond",   "cat":"BOND_EM",    "sub":"EM_LOCAL",  "risk":1.2,"cur":"USD","fiscal":"ETF"},
    # ── GOLD (ETC) ─────────────────────────────────────────────────
    {"ticker":"PHAU.MI","name":"WT Physical Gold",      "cat":"GOLD",       "sub":"GOLD",      "risk":1.0,"cur":"USD","fiscal":"ETP"},
    # ── ENERGY (ETC) ───────────────────────────────────────────────
    {"ticker":"CRUD.MI","name":"WT WTI Crude Oil",      "cat":"ENERGY",     "sub":"CRUDE",     "risk":1.3,"cur":"USD","fiscal":"ETP"},
    # ── CASH / RIFUGIO ─────────────────────────────────────────────
    {"ticker":"XEON.MI","name":"Xtrackers EUR Overnight","cat":"CASH",      "sub":"CASH",      "risk":0.0,"cur":"EUR","fiscal":"ETF"},
]

CATS = ["EQUITY","EQUITY_ASIA","EQUITY_EM","EFF_CORE","BOND_HY","BOND_IG","BOND_EM","GOLD","ENERGY","CASH"]

# ── VINCOLI PER CATEGORIA ─────────────────────────────────────────
MAX_W = {
    "EQUITY":     22,
    "EQUITY_ASIA":16,
    "EQUITY_EM":  14,
    "EFF_CORE":   18,
    "BOND_HY":    16,
    "BOND_IG":    20,
    "BOND_EM":    12,
    "GOLD":       20,
    "ENERGY":     12,
    "CASH":       70,   # XEON può arrivare al 70% in regime di crisi
}
MIN_W_ACTIVE  = 3
MAX_POSITIONS = 12
MIN_POSITIONS = 7

# ── PREFERENZE MACRO PER CATEGORIA ───────────────────────────────
MACRO_PREF = {
    "GOLDILOCKS":    {"EQUITY":90,"EQUITY_ASIA":70,"EQUITY_EM":60,"EFF_CORE":85,"BOND_HY":60,"BOND_IG":15,"BOND_EM":50,"GOLD":30,"ENERGY":40,"CASH":0},
    "REFLAZIONE":    {"EQUITY":75,"EQUITY_ASIA":65,"EQUITY_EM":75,"EFF_CORE":70,"BOND_HY":65,"BOND_IG":25,"BOND_EM":70,"GOLD":70,"ENERGY":80,"CASH":0},
    "DISINFLAZIONE": {"EQUITY":70,"EQUITY_ASIA":55,"EQUITY_EM":45,"EFF_CORE":75,"BOND_HY":55,"BOND_IG":60,"BOND_EM":40,"GOLD":50,"ENERGY":25,"CASH":5},
    "EUFORIA":       {"EQUITY":95,"EQUITY_ASIA":80,"EQUITY_EM":75,"EFF_CORE":80,"BOND_HY":75,"BOND_IG":10,"BOND_EM":55,"GOLD":20,"ENERGY":60,"CASH":0},
    "TIGHTENING":    {"EQUITY":30,"EQUITY_ASIA":25,"EQUITY_EM":15,"EFF_CORE":35,"BOND_HY":25,"BOND_IG":72,"BOND_EM":20,"GOLD":55,"ENERGY":45,"CASH":15},
    "STAGFLAZIONE":  {"EQUITY":20,"EQUITY_ASIA":15,"EQUITY_EM":10,"EFF_CORE":25,"BOND_HY":20,"BOND_IG":48,"BOND_EM":30,"GOLD":80,"ENERGY":75,"CASH":20},
    "RECESSIONE":    {"EQUITY": 5,"EQUITY_ASIA": 5,"EQUITY_EM": 0,"EFF_CORE":10,"BOND_HY": 5,"BOND_IG":85,"BOND_EM":10,"GOLD":70,"ENERGY":10,"CASH":60},
    "RISK_OFF":      {"EQUITY":10,"EQUITY_ASIA": 5,"EQUITY_EM": 0,"EFF_CORE":15,"BOND_HY":10,"BOND_IG":85,"BOND_EM": 5,"GOLD":75,"ENERGY":15,"CASH":65},
    "ZIRP":          {"EQUITY":65,"EQUITY_ASIA":55,"EQUITY_EM":55,"EFF_CORE":70,"BOND_HY":70,"BOND_IG":45,"BOND_EM":55,"GOLD":60,"ENERGY":35,"CASH":0},
    "GEO_SHOCK":     {"EQUITY":15,"EQUITY_ASIA":10,"EQUITY_EM": 5,"EFF_CORE":20,"BOND_HY":15,"BOND_IG":78,"BOND_EM":10,"GOLD":85,"ENERGY":70,"CASH":55},
    "PANDEMIC":      {"EQUITY": 5,"EQUITY_ASIA": 0,"EQUITY_EM": 0,"EFF_CORE": 5,"BOND_HY": 0,"BOND_IG":90,"BOND_EM": 0,"GOLD":60,"ENERGY": 0,"CASH":70},
    "FINANCIAL":     {"EQUITY": 0,"EQUITY_ASIA": 0,"EQUITY_EM": 0,"EFF_CORE": 5,"BOND_HY": 0,"BOND_IG":90,"BOND_EM": 0,"GOLD":65,"ENERGY": 0,"CASH":70},
    "WAR":           {"EQUITY":10,"EQUITY_ASIA": 5,"EQUITY_EM": 0,"EFF_CORE":15,"BOND_HY":10,"BOND_IG":80,"BOND_EM": 5,"GOLD":85,"ENERGY":80,"CASH":60},
    "SOVEREIGN":     {"EQUITY":10,"EQUITY_ASIA": 5,"EQUITY_EM": 0,"EFF_CORE":15,"BOND_HY": 5,"BOND_IG":82,"BOND_EM": 5,"GOLD":70,"ENERGY":20,"CASH":55},
}
MACRO_PREF_DEFAULT = {"EQUITY":50,"EQUITY_ASIA":40,"EQUITY_EM":35,"EFF_CORE":50,"BOND_HY":40,"BOND_IG":50,"BOND_EM":35,"GOLD":40,"ENERGY":30,"CASH":10}

# ── PREFERENZE SUB-TIPO ───────────────────────────────────────────
SUB_PREF = {
    "GOLDILOCKS":    {"GLOBAL":1.0,"US":0.9,"US_EUR":0.7,"EU":0.8,"TECH":0.85,"EU_SMALL":0.7,"JAPAN":0.75,"CHINA":0.6,"ASIA_PAC":0.7,"EM_BROAD":0.6,"EM_CORE":0.65,"GLOBAL_EC":1.0,"US_EC":0.9,"EM_EC":0.6,"SMALL_EC":0.65,"US_HY":0.8,"US_HY_EUR":0.7,"EU_HY":0.9,"EU_HY_A":0.85,"EU_IG":0.3,"EU_GOVT":0.25,"EM_LOCAL":0.7,"GOLD":0.4,"CRUDE":0.5,"CASH":0.0},
    "REFLAZIONE":    {"GLOBAL":0.8,"US":0.7,"US_EUR":0.6,"EU":0.75,"TECH":0.6,"EU_SMALL":0.65,"JAPAN":0.7,"CHINA":0.75,"ASIA_PAC":0.7,"EM_BROAD":0.85,"EM_CORE":0.85,"GLOBAL_EC":0.8,"US_EC":0.7,"EM_EC":0.9,"SMALL_EC":0.75,"US_HY":0.75,"US_HY_EUR":0.7,"EU_HY":0.8,"EU_HY_A":0.8,"EU_IG":0.4,"EU_GOVT":0.35,"EM_LOCAL":1.0,"GOLD":0.85,"CRUDE":1.0,"CASH":0.0},
    "DISINFLAZIONE": {"GLOBAL":0.8,"US":0.85,"US_EUR":0.9,"EU":0.7,"TECH":0.8,"EU_SMALL":0.6,"JAPAN":0.75,"CHINA":0.5,"ASIA_PAC":0.65,"EM_BROAD":0.5,"EM_CORE":0.5,"GLOBAL_EC":0.9,"US_EC":1.0,"EM_EC":0.5,"SMALL_EC":0.6,"US_HY":0.7,"US_HY_EUR":0.8,"EU_HY":0.75,"EU_HY_A":0.7,"EU_IG":0.9,"EU_GOVT":0.85,"EM_LOCAL":0.5,"GOLD":0.6,"CRUDE":0.3,"CASH":0.1},
    "EUFORIA":       {"GLOBAL":0.85,"US":0.95,"US_EUR":0.65,"EU":0.7,"TECH":1.0,"EU_SMALL":0.75,"JAPAN":0.8,"CHINA":0.8,"ASIA_PAC":0.75,"EM_BROAD":0.75,"EM_CORE":0.75,"GLOBAL_EC":0.8,"US_EC":0.9,"EM_EC":0.75,"SMALL_EC":0.85,"US_HY":0.9,"US_HY_EUR":0.8,"EU_HY":0.85,"EU_HY_A":0.8,"EU_IG":0.2,"EU_GOVT":0.15,"EM_LOCAL":0.6,"GOLD":0.25,"CRUDE":0.7,"CASH":0.0},
    "TIGHTENING":    {"GLOBAL":0.5,"US":0.4,"US_EUR":0.8,"EU":0.5,"TECH":0.3,"EU_SMALL":0.4,"JAPAN":0.55,"CHINA":0.3,"ASIA_PAC":0.4,"EM_BROAD":0.3,"EM_CORE":0.3,"GLOBAL_EC":0.6,"US_EC":0.5,"EM_EC":0.3,"SMALL_EC":0.3,"US_HY":0.4,"US_HY_EUR":0.6,"EU_HY":0.5,"EU_HY_A":0.5,"EU_IG":1.0,"EU_GOVT":0.9,"EM_LOCAL":0.3,"GOLD":0.7,"CRUDE":0.6,"CASH":0.3},
    "STAGFLAZIONE":  {"GLOBAL":0.4,"US":0.3,"US_EUR":0.6,"EU":0.45,"TECH":0.2,"EU_SMALL":0.35,"JAPAN":0.4,"CHINA":0.25,"ASIA_PAC":0.35,"EM_BROAD":0.3,"EM_CORE":0.3,"GLOBAL_EC":0.4,"US_EC":0.3,"EM_EC":0.3,"SMALL_EC":0.25,"US_HY":0.4,"US_HY_EUR":0.5,"EU_HY":0.4,"EU_HY_A":0.4,"EU_IG":0.9,"EU_GOVT":0.8,"EM_LOCAL":0.5,"GOLD":1.0,"CRUDE":0.9,"CASH":0.35},
    "RECESSIONE":    {"GLOBAL":0.3,"US":0.3,"US_EUR":0.7,"EU":0.4,"TECH":0.2,"EU_SMALL":0.2,"JAPAN":0.4,"CHINA":0.15,"ASIA_PAC":0.25,"EM_BROAD":0.1,"EM_CORE":0.1,"GLOBAL_EC":0.3,"US_EC":0.3,"EM_EC":0.1,"SMALL_EC":0.1,"US_HY":0.2,"US_HY_EUR":0.3,"EU_HY":0.3,"EU_HY_A":0.3,"EU_IG":1.0,"EU_GOVT":0.95,"EM_LOCAL":0.2,"GOLD":0.9,"CRUDE":0.15,"CASH":1.0},
    "RISK_OFF":      {"GLOBAL":0.3,"US":0.3,"US_EUR":0.8,"EU":0.3,"TECH":0.2,"EU_SMALL":0.2,"JAPAN":0.45,"CHINA":0.1,"ASIA_PAC":0.2,"EM_BROAD":0.1,"EM_CORE":0.1,"GLOBAL_EC":0.3,"US_EC":0.3,"EM_EC":0.1,"SMALL_EC":0.1,"US_HY":0.2,"US_HY_EUR":0.3,"EU_HY":0.3,"EU_HY_A":0.3,"EU_IG":1.0,"EU_GOVT":0.95,"EM_LOCAL":0.1,"GOLD":1.0,"CRUDE":0.2,"CASH":1.0},
    "ZIRP":          {"GLOBAL":0.8,"US":0.75,"US_EUR":0.6,"EU":0.7,"TECH":0.8,"EU_SMALL":0.65,"JAPAN":0.7,"CHINA":0.7,"ASIA_PAC":0.7,"EM_BROAD":0.7,"EM_CORE":0.7,"GLOBAL_EC":0.85,"US_EC":0.8,"EM_EC":0.7,"SMALL_EC":0.65,"US_HY":0.9,"US_HY_EUR":0.85,"EU_HY":0.9,"EU_HY_A":0.9,"EU_IG":0.6,"EU_GOVT":0.5,"EM_LOCAL":0.8,"GOLD":0.7,"CRUDE":0.4,"CASH":0.0},
    "GEO_SHOCK":     {"GLOBAL":0.3,"US":0.35,"US_EUR":0.8,"EU":0.2,"TECH":0.25,"EU_SMALL":0.2,"JAPAN":0.4,"CHINA":0.1,"ASIA_PAC":0.2,"EM_BROAD":0.1,"EM_CORE":0.1,"GLOBAL_EC":0.3,"US_EC":0.35,"EM_EC":0.1,"SMALL_EC":0.1,"US_HY":0.3,"US_HY_EUR":0.4,"EU_HY":0.2,"EU_HY_A":0.2,"EU_IG":1.0,"EU_GOVT":0.9,"EM_LOCAL":0.1,"GOLD":1.0,"CRUDE":0.9,"CASH":0.85},
    "PANDEMIC":      {"GLOBAL":0.2,"US":0.2,"US_EUR":0.7,"EU":0.15,"TECH":0.4,"EU_SMALL":0.1,"JAPAN":0.3,"CHINA":0.1,"ASIA_PAC":0.15,"EM_BROAD":0.05,"EM_CORE":0.05,"GLOBAL_EC":0.2,"US_EC":0.2,"EM_EC":0.05,"SMALL_EC":0.05,"US_HY":0.1,"US_HY_EUR":0.2,"EU_HY":0.1,"EU_HY_A":0.1,"EU_IG":1.0,"EU_GOVT":0.95,"EM_LOCAL":0.05,"GOLD":0.8,"CRUDE":0.05,"CASH":1.0},
    "FINANCIAL":     {"GLOBAL":0.1,"US":0.1,"US_EUR":0.7,"EU":0.1,"TECH":0.1,"EU_SMALL":0.05,"JAPAN":0.2,"CHINA":0.05,"ASIA_PAC":0.1,"EM_BROAD":0.0,"EM_CORE":0.0,"GLOBAL_EC":0.1,"US_EC":0.1,"EM_EC":0.0,"SMALL_EC":0.0,"US_HY":0.0,"US_HY_EUR":0.1,"EU_HY":0.0,"EU_HY_A":0.0,"EU_IG":1.0,"EU_GOVT":0.95,"EM_LOCAL":0.0,"GOLD":0.85,"CRUDE":0.0,"CASH":1.0},
    "WAR":           {"GLOBAL":0.3,"US":0.4,"US_EUR":0.8,"EU":0.1,"TECH":0.3,"EU_SMALL":0.15,"JAPAN":0.35,"CHINA":0.05,"ASIA_PAC":0.15,"EM_BROAD":0.1,"EM_CORE":0.1,"GLOBAL_EC":0.3,"US_EC":0.4,"EM_EC":0.1,"SMALL_EC":0.1,"US_HY":0.3,"US_HY_EUR":0.4,"EU_HY":0.2,"EU_HY_A":0.2,"EU_IG":1.0,"EU_GOVT":0.9,"EM_LOCAL":0.05,"GOLD":1.0,"CRUDE":1.0,"CASH":0.9},
    "SOVEREIGN":     {"GLOBAL":0.3,"US":0.4,"US_EUR":0.8,"EU":0.2,"TECH":0.25,"EU_SMALL":0.15,"JAPAN":0.4,"CHINA":0.1,"ASIA_PAC":0.2,"EM_BROAD":0.1,"EM_CORE":0.1,"GLOBAL_EC":0.3,"US_EC":0.4,"EM_EC":0.1,"SMALL_EC":0.1,"US_HY":0.2,"US_HY_EUR":0.3,"EU_HY":0.2,"EU_HY_A":0.2,"EU_IG":1.0,"EU_GOVT":0.95,"EM_LOCAL":0.1,"GOLD":0.9,"CRUDE":0.25,"CASH":0.85},
}
SUB_PREF_DEFAULT = {s["sub"]: 0.5 for s in UNIVERSE}

# ── FETCH PREZZI ──────────────────────────────────────────────────
def fetch_prices(tickers: list) -> dict:
    result = {}
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=95)
    for tk in tickers:
        for suffix in [tk, tk.replace(".MI",".L"), tk.replace(".MI",".PA")]:
            try:
                hist = yf.Ticker(suffix).history(
                    start=start.strftime("%Y-%m-%d"),
                    end=end.strftime("%Y-%m-%d"),
                    auto_adjust=True)
                if len(hist) < 10:
                    continue
                closes = hist["Close"].dropna()
                p    = float(closes.iloc[-1])
                r1w  = (closes.iloc[-1]/closes.iloc[-6]-1)*100  if len(closes)>6  else None
                r4w  = (closes.iloc[-1]/closes.iloc[-21]-1)*100 if len(closes)>21 else None
                r12w = (closes.iloc[-1]/closes.iloc[-61]-1)*100 if len(closes)>61 else None
                result[tk] = {"p": round(p,4), "r1w": r1w, "r4w": r4w, "r12w": r12w}
                print(f"  ✓ {tk} p={p:.2f}" + (f" r1w={r1w:.1f}%" if r1w else ""))
                break
            except Exception:
                continue
        if tk not in result:
            print(f"  ⚠  {tk} — non disponibile")
            result[tk] = {"p": None, "r1w": None, "r4w": None, "r12w": None}
        time.sleep(0.2)
    return result

# ── MOMENTUM SCORE ────────────────────────────────────────────────
def momentum_score(prices: dict) -> dict:
    composites = {}
    for tk, d in prices.items():
        vals  = [d["r1w"], d["r4w"], d["r12w"]]
        valid = [v for v in vals if v is not None]
        if not valid:
            composites[tk] = None
            continue
        keys  = ["r1w","r4w","r12w"]
        w     = [0.30, 0.40, 0.30]
        c     = sum(d[k]*w[i] for i,k in enumerate(keys) if d[k] is not None)
        wsum  = sum(w[i] for i,k in enumerate(keys) if d[k] is not None)
        composites[tk] = c / wsum if wsum else None

    known = {tk: v for tk,v in composites.items() if v is not None}
    if not known:
        return {tk: 50 for tk in prices}
    vmin, vmax = min(known.values()), max(known.values())
    scores = {}
    for tk, c in composites.items():
        if c is None:
            scores[tk] = 50
        elif vmax == vmin:
            scores[tk] = 50
        else:
            scores[tk] = round((c - vmin) / (vmax - vmin) * 100)
    return scores

# ── MACRO SCORE ───────────────────────────────────────────────────
def macro_scores(scenarios: dict) -> dict:
    tot_w = sum(scenarios.values()) or 1
    scores = {}
    for etf in UNIVERSE:
        tk  = etf["ticker"]
        cat = etf["cat"]
        sub = etf["sub"]
        sc  = 0.0
        for code, pct in scenarios.items():
            w   = pct / tot_w
            cp  = MACRO_PREF.get(code, MACRO_PREF_DEFAULT).get(cat, 50)
            sp  = SUB_PREF.get(code, SUB_PREF_DEFAULT).get(sub, 0.5)
            sc += w * cp * sp
        scores[tk] = min(100, round(sc))
    return scores

# ── QUALITY SCORE ─────────────────────────────────────────────────
def quality_scores() -> dict:
    return {
        "WWRD.MI":80,"WSPX.MI":90,"WSPE.MI":85,"WS5X.MI":82,"WNAS.MI":88,"SMEA.MI":75,
        "WPXJ.MI":78,"XCHA.MI":72,"XASX.MI":74,
        "VFEM.MI":80,"EIMI.MI":82,
        "NTSX.MI":87,"NTSG.MI":86,"NTSZ.MI":78,"WRTY.MI":75,
        "STHY.MI":84,"STHE.MI":83,"EUHI.MI":82,"EUHA.MI":81,
        "PJS1.MI":90,"XGIU.MI":88,
        "EMLI.MI":78,
        "PHAU.MI":88,
        "CRUD.MI":75,
        "XEON.MI":95,
    }

# ── SCORE FINALE ──────────────────────────────────────────────────
def final_scores(macro: dict, mom: dict, qual: dict) -> dict:
    scores = {}
    for etf in UNIVERSE:
        tk = etf["ticker"]
        m  = macro.get(tk, 50)
        mo = mom.get(tk, 50)
        q  = qual.get(tk, 75)
        scores[tk] = round(m * 0.55 + mo * 0.30 + q * 0.15)
    return scores

# ── OTTIMIZZAZIONE PESI ───────────────────────────────────────────
def optimize_weights(scores: dict, prev_weights: dict, scenarios: dict) -> dict:
    etf_map = {e["ticker"]: e for e in UNIVERSE}

    # Determina regime dominante per logica XEON
    dominant = max(scenarios, key=lambda k: scenarios.get(k, 0)) if scenarios else ""
    crisis_regimes = {"RISK_OFF","RECESSIONE","PANDEMIC","FINANCIAL","WAR","SOVEREIGN","GEO_SHOCK"}
    is_crisis = dominant in crisis_regimes
    crisis_intensity = scenarios.get(dominant, 0) / 100 if is_crisis else 0

    sorted_etf = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    selected   = [tk for tk, _ in sorted_etf[:MAX_POSITIONS]]

    # Assicura XEON sia sempre candidato in crisi
    xeon = "XEON.MI"
    if is_crisis and xeon not in selected:
        selected = selected[:-1] + [xeon]

    # Score grezzo con risk scaling
    raw = {}
    for tk in selected:
        e        = etf_map[tk]
        sc       = scores[tk]
        risk     = e["risk"]
        # CASH (XEON) non ha penalità risk
        if e["cat"] == "CASH":
            raw[tk] = max(sc, 1)
        else:
            raw[tk] = max(sc / max(risk, 0.1), 1)

    # Normalizzazione iniziale
    tot     = sum(raw.values())
    weights = {tk: raw[tk] / tot * 100 for tk in selected}

    # In crisi: boost XEON proporzionale all'intensità
    if is_crisis and xeon in weights:
        boost        = crisis_intensity * MAX_W["CASH"]
        weights[xeon] = min(MAX_W["CASH"], weights.get(xeon, 0) + boost)
        # Ridistribuisci il resto proporzionalmente
        others = {tk: w for tk, w in weights.items() if tk != xeon}
        tot_others = sum(others.values())
        remaining  = 100 - weights[xeon]
        if tot_others > 0:
            weights.update({tk: w / tot_others * remaining for tk, w in others.items()})

    def apply_constraints(w: dict) -> dict:
        for tk in list(w.keys()):
            cat  = etf_map[tk]["cat"]
            wmax = MAX_W.get(cat, 25)
            w[tk] = max(MIN_W_ACTIVE, min(wmax, w[tk]))
        s = sum(w.values())
        return {tk: v / s * 100 for tk, v in w.items()}

    for _ in range(4):
        weights = apply_constraints(weights)

    # Arrotonda a interi
    rounded = {tk: int(v) for tk, v in weights.items()}
    diff    = 100 - sum(rounded.values())
    if diff != 0:
        keys = sorted(rounded, key=lambda k: weights[k], reverse=True)
        for i in range(abs(diff)):
            rounded[keys[i % len(keys)]] += 1 if diff > 0 else -1

    return {tk: w for tk, w in rounded.items() if w > 0}

# ── SEGNALE RIBILANCIAMENTO ───────────────────────────────────────
def rebalance_signal(new_w: dict, prev_w: dict, prev_regime: str, curr_regime: str) -> tuple:
    if not prev_w:
        return "INIT", "Prima configurazione del portafoglio"
    all_tickers = set(new_w.keys()) | set(prev_w.keys())
    deviations  = {tk: abs(new_w.get(tk,0) - prev_w.get(tk,0)) for tk in all_tickers}
    max_dev     = max(deviations.values()) if deviations else 0
    avg_dev     = sum(deviations.values()) / len(deviations) if deviations else 0
    entered     = [tk for tk in new_w if tk not in prev_w]
    exited      = [tk for tk in prev_w if tk not in new_w]
    regime_changed = prev_regime != curr_regime
    if max_dev > 8 or regime_changed:
        if regime_changed:
            reason = f"Cambio regime: {prev_regime} → {curr_regime}"
        else:
            tk_max = max(deviations, key=deviations.get)
            reason = f"Deviazione massima {max_dev:.0f}% su {tk_max}"
        if entered or exited:
            reason += f" · {', '.join(e.replace('.MI','') for e in entered)} in · {', '.join(e.replace('.MI','') for e in exited)} out"
        return "REBALANCE", reason
    if max_dev > 3 or avg_dev > 2:
        return "PARTIAL", f"Deviazione {max_dev:.0f}% (avg {avg_dev:.1f}%)"
    return "HOLD", f"Portafoglio stabile (dev max {max_dev:.1f}%)"

def dominant(scenarios: dict) -> str:
    return max(scenarios, key=lambda k: scenarios.get(k,0)) if scenarios else "NEUTRO"

# ── MAIN ──────────────────────────────────────────────────────────
def run():
    BASE   = Path(__file__).parent
    LATEST = BASE / "data" / "latest.json"
    PF_OUT = BASE / "data" / "portfolio.json"

    print("="*60)
    print(f"RAPTOR PORTFOLIO OPTIMIZER v2.0 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)

    with open(LATEST, encoding="utf-8") as f:
        latest = json.load(f)
    sw = latest.get("scenario_weights", [])
    if not sw:
        print("⚠  scenario_weights vuoto"); return

    current      = sw[-1]
    scenarios    = current.get("scenarios", {})
    curr_regime  = dominant(scenarios)
    today_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"📅 {today_str}  |  Regime: {curr_regime} ({scenarios.get(curr_regime,0)}%)")

    # Storico
    history      = []
    prev_weights = {}
    prev_regime  = ""
    if PF_OUT.exists():
        try:
            with open(PF_OUT, encoding="utf-8") as f:
                pf_data = json.load(f)
            history = pf_data.get("history", [])
            if history:
                prev_week    = history[0]
                prev_weights = {e["ticker_full"]: e["weight"] for e in prev_week.get("portfolio",[])}
                prev_regime  = prev_week.get("regime","")
            print(f"📊 Storico: {len(history)} settimane")
        except Exception as e:
            print(f"⚠  Errore storico: {e}")

    print("\n📡 Download prezzi...")
    tickers = [e["ticker"] for e in UNIVERSE]
    prices  = fetch_prices(tickers)

    print("\n🧮 Calcolo scores...")
    m_sc  = macro_scores(scenarios)
    mo_sc = momentum_score(prices)
    q_sc  = quality_scores()
    f_sc  = final_scores(m_sc, mo_sc, q_sc)

    print("\n⚖️  Ottimizzazione pesi...")
    weights = optimize_weights(f_sc, prev_weights, scenarios)

    signal, reason = rebalance_signal(weights, prev_weights, prev_regime, curr_regime)
    print(f"\n🚦 Segnale: {signal} — {reason}")

    etf_map = {e["ticker"]: e for e in UNIVERSE}
    portfolio_list = []
    for tk, w in sorted(weights.items(), key=lambda x: x[1], reverse=True):
        e   = etf_map[tk]
        pd_ = prices.get(tk, {})
        portfolio_list.append({
            "ticker":         tk.replace(".MI",""),
            "ticker_full":    tk,
            "name":           e["name"],
            "cat":            e["cat"],
            "sub":            e["sub"],
            "risk_factor":    e["risk"],
            "currency":       e["cur"],
            "fiscal":         e["fiscal"],
            "weight":         w,
            "weight_prev":    prev_weights.get(tk, 0),
            "weight_delta":   w - prev_weights.get(tk, 0),
            "macro_score":    m_sc.get(tk, 50),
            "momentum_score": mo_sc.get(tk, 50),
            "quality_score":  q_sc.get(tk, 75),
            "final_score":    f_sc.get(tk, 50),
            "price":          pd_.get("p"),
            "ret_1w":         round(pd_.get("r1w") or 0, 2),
            "ret_4w":         round(pd_.get("r4w") or 0, 2),
            "ret_12w":        round(pd_.get("r12w") or 0, 2),
        })

    macro_bd = {}
    for cat in CATS:
        v = sum(e["weight"] for e in portfolio_list if e["cat"]==cat)
        if v: macro_bd[cat] = v

    week_entry = {
        "date":             today_str,
        "regime":           curr_regime,
        "regime_probs":     {k:v for k,v in scenarios.items() if v>0},
        "rebalance":        signal,
        "rebalance_reason": reason,
        "portfolio":        portfolio_list,
        "macro_breakdown":  macro_bd,
        "generated_at":     datetime.now(timezone.utc).isoformat(),
    }

    print("\n📐 PORTAFOGLIO:")
    print(f"   {'Ticker':<10} {'Nome':<26} {'Cat':<12} {'Peso':>5}  {'Δ':>5}  {'Score':>5}  {'Fiscal'}")
    print(f"   {'-'*76}")
    for e in portfolio_list:
        delta_str = f"{e['weight_delta']:+.0f}%" if e['weight_prev'] else " NEW"
        print(f"   {e['ticker']:<10} {e['name']:<26} {e['cat']:<12} {e['weight']:>4}%  {delta_str:>5}  {e['final_score']:>5}  {e['fiscal']}")
    print(f"\n   BREAKDOWN: " + " | ".join(f"{k}: {v}%" for k,v in macro_bd.items()))

    idx = next((i for i,h in enumerate(history) if h.get("date")==today_str), None)
    if idx is not None:
        history[idx] = week_entry
    else:
        history.insert(0, week_entry)
    history = history[:52]

    PF_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(PF_OUT, "w", encoding="utf-8") as f:
        json.dump({"history": history}, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Salvato {PF_OUT} ({len(history)} settimane)")

if __name__ == "__main__":
    run()
