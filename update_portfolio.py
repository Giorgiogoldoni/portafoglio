#!/usr/bin/env python3
"""
RAPTOR PORTFOLIO OPTIMIZER v1.0
════════════════════════════════
Ottimizzazione ibrida Macro × Momentum con risk scaling per Efficient Core.

Input:  data/latest.json        (scenari Macro Mover)
        data/portfolio.json     (storico settimane precedenti, se esiste)
Output: data/portfolio.json     (portafoglio corrente + storico)

Logica:
  score_finale = macro_score × 0.55 + momentum_score × 0.30 + quality_score × 0.15

  Rebalancing:
    HOLD         → deviazione massima < 3%
    PARTIAL      → deviazione 3-8% oppure shift regime > 10pp
    REBALANCE    → deviazione > 8% oppure cambio regime dominante
    FORCED       → posizione fuori range (>28% o <2%)
"""

import json, os, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── DIPENDENZE ────────────────────────────────────────────────────
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

# ── UNIVERSO ETF ──────────────────────────────────────────────────
UNIVERSE = [
    # ── EQUITY ETP (WisdomTree collateral-backed) ──────────────────
    {"ticker":"WWRD.MI","name":"WT World",             "cat":"EQUITY","sub":"GLOBAL",    "risk":1.0,"cur":"USD"},
    {"ticker":"WSPX.MI","name":"WT S&P 500",           "cat":"EQUITY","sub":"US",         "risk":1.0,"cur":"USD"},
    {"ticker":"WSPE.MI","name":"WT S&P 500 EUR Hed",   "cat":"EQUITY","sub":"US_EUR",     "risk":1.0,"cur":"EUR"},
    {"ticker":"WS5X.MI","name":"WT Euro Stoxx 50",     "cat":"EQUITY","sub":"EU",         "risk":1.0,"cur":"EUR"},
    {"ticker":"WNAS.MI","name":"WT Nasdaq-100",        "cat":"EQUITY","sub":"TECH",       "risk":1.0,"cur":"USD"},
    # ── EFFICIENT CORE 90/60 (WisdomTree UCITS) ────────────────────
    {"ticker":"NTSX.MI","name":"WT US Efficient Core", "cat":"EFF_CORE","sub":"US_EC",    "risk":1.5,"cur":"USD"},
    {"ticker":"NTSG.MI","name":"WT Global Eff. Core",  "cat":"EFF_CORE","sub":"GLOBAL_EC","risk":1.5,"cur":"USD"},
    {"ticker":"NTSZ.MI","name":"WT EM Efficient Core", "cat":"EFF_CORE","sub":"EM_EC",    "risk":1.5,"cur":"USD"},
    {"ticker":"WRTY.MI","name":"WT Russell 2000 EC",   "cat":"EFF_CORE","sub":"SMALL_EC", "risk":1.5,"cur":"USD"},
    # ── BOND HY (PIMCO) ────────────────────────────────────────────
    {"ticker":"STHY.MI","name":"PIMCO US ST HY USD",   "cat":"BOND_HY","sub":"US_HY",    "risk":1.0,"cur":"USD"},
    {"ticker":"STHE.MI","name":"PIMCO US ST HY EUR",   "cat":"BOND_HY","sub":"US_HY_EUR","risk":1.0,"cur":"EUR"},
    {"ticker":"EUHI.MI","name":"PIMCO Euro ST HY",     "cat":"BOND_HY","sub":"EU_HY",    "risk":1.0,"cur":"EUR"},
    {"ticker":"EUHA.MI","name":"PIMCO Euro HY Acc",    "cat":"BOND_HY","sub":"EU_HY_A",  "risk":1.0,"cur":"EUR"},
    # ── BOND IG SHORT (PIMCO) ──────────────────────────────────────
    {"ticker":"PJS1.MI","name":"PIMCO Euro Short Mat", "cat":"BOND_IG","sub":"EU_IG",    "risk":0.5,"cur":"EUR"},
    # ── BOND EM LOCAL (PIMCO) ──────────────────────────────────────
    {"ticker":"EMLI.MI","name":"PIMCO EM Local Bond",  "cat":"BOND_EM","sub":"EM_LOCAL", "risk":1.2,"cur":"USD"},
]

CATS = ["EQUITY","EFF_CORE","BOND_HY","BOND_IG","BOND_EM"]

# ── PREFERENZE MACRO PER CATEGORIA (0-100) ────────────────────────
MACRO_PREF = {
    "GOLDILOCKS":    {"EQUITY":90,"EFF_CORE":85,"BOND_HY":60,"BOND_IG":15,"BOND_EM":50},
    "REFLAZIONE":    {"EQUITY":75,"EFF_CORE":70,"BOND_HY":65,"BOND_IG":25,"BOND_EM":70},
    "DISINFLAZIONE": {"EQUITY":70,"EFF_CORE":75,"BOND_HY":55,"BOND_IG":60,"BOND_EM":40},
    "EUFORIA":       {"EQUITY":95,"EFF_CORE":80,"BOND_HY":75,"BOND_IG":10,"BOND_EM":55},
    "TIGHTENING":    {"EQUITY":30,"EFF_CORE":35,"BOND_HY":25,"BOND_IG":72,"BOND_EM":20},
    "STAGFLAZIONE":  {"EQUITY":20,"EFF_CORE":25,"BOND_HY":20,"BOND_IG":48,"BOND_EM":30},
    "RECESSIONE":    {"EQUITY": 5,"EFF_CORE":10,"BOND_HY": 5,"BOND_IG":92,"BOND_EM":10},
    "RISK_OFF":      {"EQUITY":10,"EFF_CORE":15,"BOND_HY":10,"BOND_IG":92,"BOND_EM": 5},
    "ZIRP":          {"EQUITY":65,"EFF_CORE":70,"BOND_HY":70,"BOND_IG":45,"BOND_EM":55},
    "GEO_SHOCK":     {"EQUITY":15,"EFF_CORE":20,"BOND_HY":15,"BOND_IG":78,"BOND_EM":10},
    "PANDEMIC":      {"EQUITY": 5,"EFF_CORE": 5,"BOND_HY": 0,"BOND_IG":95,"BOND_EM": 0},
    "FINANCIAL":     {"EQUITY": 0,"EFF_CORE": 5,"BOND_HY": 0,"BOND_IG":95,"BOND_EM": 0},
    "WAR":           {"EQUITY":10,"EFF_CORE":15,"BOND_HY":10,"BOND_IG":82,"BOND_EM": 5},
    "SOVEREIGN":     {"EQUITY":10,"EFF_CORE":15,"BOND_HY": 5,"BOND_IG":87,"BOND_EM": 5},
}
MACRO_PREF_DEFAULT = {"EQUITY":50,"EFF_CORE":50,"BOND_HY":40,"BOND_IG":50,"BOND_EM":35}

# ── PREFERENZE WITHIN-CATEGORY PER SOTTO-TIPO ────────────────────
# Multiplier 0-1 per ogni sub-tipo a seconda dello scenario
SUB_PREF = {
    "GOLDILOCKS": {
        "GLOBAL":1.0,"US":0.9,"US_EUR":0.7,"EU":0.8,"TECH":0.85,
        "GLOBAL_EC":1.0,"US_EC":0.9,"EM_EC":0.6,"SMALL_EC":0.65,
        "US_HY":0.8,"US_HY_EUR":0.7,"EU_HY":0.9,"EU_HY_A":0.85,"EU_IG":0.3,"EM_LOCAL":0.7,
    },
    "REFLAZIONE": {
        "GLOBAL":0.8,"US":0.7,"US_EUR":0.6,"EU":0.75,"TECH":0.6,
        "GLOBAL_EC":0.8,"US_EC":0.7,"EM_EC":0.9,"SMALL_EC":0.75,
        "US_HY":0.75,"US_HY_EUR":0.7,"EU_HY":0.8,"EU_HY_A":0.8,"EU_IG":0.4,"EM_LOCAL":1.0,
    },
    "DISINFLAZIONE": {
        "GLOBAL":0.8,"US":0.85,"US_EUR":0.9,"EU":0.7,"TECH":0.8,
        "GLOBAL_EC":0.9,"US_EC":1.0,"EM_EC":0.5,"SMALL_EC":0.6,
        "US_HY":0.7,"US_HY_EUR":0.8,"EU_HY":0.75,"EU_HY_A":0.7,"EU_IG":0.9,"EM_LOCAL":0.5,
    },
    "EUFORIA": {
        "GLOBAL":0.85,"US":0.95,"US_EUR":0.65,"EU":0.7,"TECH":1.0,
        "GLOBAL_EC":0.8,"US_EC":0.9,"EM_EC":0.75,"SMALL_EC":0.85,
        "US_HY":0.9,"US_HY_EUR":0.8,"EU_HY":0.85,"EU_HY_A":0.8,"EU_IG":0.2,"EM_LOCAL":0.6,
    },
    "TIGHTENING": {
        "GLOBAL":0.5,"US":0.4,"US_EUR":0.8,"EU":0.5,"TECH":0.3,
        "GLOBAL_EC":0.6,"US_EC":0.5,"EM_EC":0.3,"SMALL_EC":0.3,
        "US_HY":0.4,"US_HY_EUR":0.6,"EU_HY":0.5,"EU_HY_A":0.5,"EU_IG":1.0,"EM_LOCAL":0.3,
    },
    "STAGFLAZIONE": {
        "GLOBAL":0.4,"US":0.3,"US_EUR":0.6,"EU":0.45,"TECH":0.2,
        "GLOBAL_EC":0.4,"US_EC":0.3,"EM_EC":0.3,"SMALL_EC":0.25,
        "US_HY":0.4,"US_HY_EUR":0.5,"EU_HY":0.4,"EU_HY_A":0.4,"EU_IG":0.9,"EM_LOCAL":0.5,
    },
    "RECESSIONE": {
        "GLOBAL":0.3,"US":0.3,"US_EUR":0.7,"EU":0.4,"TECH":0.2,
        "GLOBAL_EC":0.3,"US_EC":0.3,"EM_EC":0.1,"SMALL_EC":0.1,
        "US_HY":0.2,"US_HY_EUR":0.3,"EU_HY":0.3,"EU_HY_A":0.3,"EU_IG":1.0,"EM_LOCAL":0.2,
    },
    "RISK_OFF": {
        "GLOBAL":0.3,"US":0.3,"US_EUR":0.8,"EU":0.3,"TECH":0.2,
        "GLOBAL_EC":0.3,"US_EC":0.3,"EM_EC":0.1,"SMALL_EC":0.1,
        "US_HY":0.2,"US_HY_EUR":0.3,"EU_HY":0.3,"EU_HY_A":0.3,"EU_IG":1.0,"EM_LOCAL":0.1,
    },
    "ZIRP": {
        "GLOBAL":0.8,"US":0.75,"US_EUR":0.6,"EU":0.7,"TECH":0.8,
        "GLOBAL_EC":0.85,"US_EC":0.8,"EM_EC":0.7,"SMALL_EC":0.65,
        "US_HY":0.9,"US_HY_EUR":0.85,"EU_HY":0.9,"EU_HY_A":0.9,"EU_IG":0.6,"EM_LOCAL":0.8,
    },
    "GEO_SHOCK": {
        "GLOBAL":0.3,"US":0.35,"US_EUR":0.8,"EU":0.2,"TECH":0.25,
        "GLOBAL_EC":0.3,"US_EC":0.35,"EM_EC":0.1,"SMALL_EC":0.1,
        "US_HY":0.3,"US_HY_EUR":0.4,"EU_HY":0.2,"EU_HY_A":0.2,"EU_IG":1.0,"EM_LOCAL":0.1,
    },
    "PANDEMIC": {
        "GLOBAL":0.2,"US":0.2,"US_EUR":0.7,"EU":0.15,"TECH":0.4,
        "GLOBAL_EC":0.2,"US_EC":0.2,"EM_EC":0.05,"SMALL_EC":0.05,
        "US_HY":0.1,"US_HY_EUR":0.2,"EU_HY":0.1,"EU_HY_A":0.1,"EU_IG":1.0,"EM_LOCAL":0.05,
    },
    "FINANCIAL": {
        "GLOBAL":0.1,"US":0.1,"US_EUR":0.7,"EU":0.1,"TECH":0.1,
        "GLOBAL_EC":0.1,"US_EC":0.1,"EM_EC":0.0,"SMALL_EC":0.0,
        "US_HY":0.0,"US_HY_EUR":0.1,"EU_HY":0.0,"EU_HY_A":0.0,"EU_IG":1.0,"EM_LOCAL":0.0,
    },
    "WAR": {
        "GLOBAL":0.3,"US":0.4,"US_EUR":0.8,"EU":0.1,"TECH":0.3,
        "GLOBAL_EC":0.3,"US_EC":0.4,"EM_EC":0.1,"SMALL_EC":0.1,
        "US_HY":0.3,"US_HY_EUR":0.4,"EU_HY":0.2,"EU_HY_A":0.2,"EU_IG":1.0,"EM_LOCAL":0.05,
    },
    "SOVEREIGN": {
        "GLOBAL":0.3,"US":0.4,"US_EUR":0.8,"EU":0.2,"TECH":0.25,
        "GLOBAL_EC":0.3,"US_EC":0.4,"EM_EC":0.1,"SMALL_EC":0.1,
        "US_HY":0.2,"US_HY_EUR":0.3,"EU_HY":0.2,"EU_HY_A":0.2,"EU_IG":1.0,"EM_LOCAL":0.1,
    },
}
SUB_PREF_DEFAULT = {s["sub"]: 0.5 for s in UNIVERSE}

# ── VINCOLI PORTAFOGLIO ────────────────────────────────────────────
MAX_W = {
    "EQUITY":   24,   # max peso singolo ETP equity
    "EFF_CORE": 18,   # max peso EC (nozionale effettivo 27%)
    "BOND_HY":  18,   # max singolo bond HY
    "BOND_IG":  20,   # PJS1 — può essere ancora più difensivo
    "BOND_EM":  15,   # EMLI
}
MIN_W_ACTIVE  = 3    # minimo peso se ETF è in portafoglio
MAX_POSITIONS = 12   # max ETF selezionati (su 15)
MIN_POSITIONS = 7    # minimo ETF attivi

# ── FETCH PREZZI (yfinance) ───────────────────────────────────────
def fetch_prices(tickers: list[str]) -> dict:
    """
    Scarica 90 giorni di prezzi per calcolare momentum.
    Ritorna {ticker: {"p": price, "r1w": %, "r4w": %, "r12w": %}}
    """
    result = {}
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=95)

    for tk in tickers:
        for suffix in [tk, tk.replace(".MI",".L"), tk.replace(".MI",".PA")]:
            try:
                hist = yf.Ticker(suffix).history(start=start.strftime("%Y-%m-%d"),
                                                  end=end.strftime("%Y-%m-%d"),
                                                  auto_adjust=True)
                if len(hist) < 10:
                    continue
                closes = hist["Close"].dropna()
                p   = float(closes.iloc[-1])
                r1w = (closes.iloc[-1]/closes.iloc[-6]-1)*100  if len(closes)>6  else None
                r4w = (closes.iloc[-1]/closes.iloc[-21]-1)*100 if len(closes)>21 else None
                r12w= (closes.iloc[-1]/closes.iloc[-61]-1)*100 if len(closes)>61 else None
                result[tk] = {"p": round(p,4), "r1w": r1w, "r4w": r4w, "r12w": r12w}
                print(f"  ✓ {tk} [{suffix}] p={p:.2f} r1w={r1w:.1f}%" if r1w else f"  ✓ {tk}")
                break
            except Exception:
                continue
        if tk not in result:
            print(f"  ⚠  {tk} — prezzi non disponibili (solo macro score)")
            result[tk] = {"p": None, "r1w": None, "r4w": None, "r12w": None}
        time.sleep(0.2)   # rate limit

    return result

# ── MOMENTUM SCORE (0-100) ────────────────────────────────────────
def momentum_score(prices: dict) -> dict:
    """Normalizza i ritorni in score 0-100 per tutti gli ETF."""
    # Raccoglie composito = 0.30*r1w + 0.40*r4w + 0.30*r12w
    composites = {}
    for tk, d in prices.items():
        vals = [d["r1w"], d["r4w"], d["r12w"]]
        valid = [v for v in vals if v is not None]
        if not valid:
            composites[tk] = None
        else:
            w = [0.30, 0.40, 0.30]
            keys = ["r1w","r4w","r12w"]
            c = sum(d[k]*w[i] for i,k in enumerate(keys) if d[k] is not None)
            # ri-normalizza i pesi
            wsum = sum(w[i] for i,k in enumerate(keys) if d[k] is not None)
            composites[tk] = c / wsum if wsum else None

    known = {tk: v for tk,v in composites.items() if v is not None}
    if not known:
        return {tk: 50 for tk in prices}

    vmin, vmax = min(known.values()), max(known.values())
    scores = {}
    for tk, c in composites.items():
        if c is None:
            scores[tk] = 50   # neutro se dati mancanti
        elif vmax == vmin:
            scores[tk] = 50
        else:
            scores[tk] = round((c - vmin) / (vmax - vmin) * 100)
    return scores

# ── MACRO SCORE (0-100) ───────────────────────────────────────────
def macro_scores(scenarios: dict) -> dict:
    """
    Calcola macro_score per ogni ETF dal vettore scenari corrente.
    = somma pesata su scenari di (cat_pref × sub_mult)
    """
    tot_w = sum(scenarios.values()) or 1
    scores = {}
    for etf in UNIVERSE:
        tk   = etf["ticker"]
        cat  = etf["cat"]
        sub  = etf["sub"]
        sc   = 0.0
        for code, pct in scenarios.items():
            w    = pct / tot_w
            cp   = MACRO_PREF.get(code, MACRO_PREF_DEFAULT).get(cat, 50)
            sp   = SUB_PREF.get(code, SUB_PREF_DEFAULT).get(sub, 0.5)
            sc  += w * cp * sp
        scores[tk] = min(100, round(sc))
    return scores

# ── QUALITY SCORE (0-100) ─────────────────────────────────────────
def quality_scores() -> dict:
    """
    Score fisso di qualità/liquidità (TER, AUM, liquidità).
    Stabile nel tempo.
    """
    Q = {
        "WWRD.MI":80,"WSPX.MI":90,"WSPE.MI":85,"WS5X.MI":82,"WNAS.MI":88,
        "NTSX.MI":87,"NTSG.MI":86,"NTSZ.MI":78,"WRTY.MI":75,
        "STHY.MI":84,"STHE.MI":83,"EUHI.MI":82,"EUHA.MI":81,
        "PJS1.MI":90,"EMLI.MI":78,
    }
    return Q

# ── SCORE FINALE ──────────────────────────────────────────────────
def final_scores(macro: dict, mom: dict, qual: dict) -> dict:
    scores = {}
    for etf in UNIVERSE:
        tk = etf["ticker"]
        m  = macro.get(tk, 50)
        mo = mom.get(tk, 50)
        q  = qual.get(tk, 75)
        # Peso: macro 55%, momentum 30%, quality 15%
        scores[tk] = round(m * 0.55 + mo * 0.30 + q * 0.15)
    return scores

# ── OTTIMIZZAZIONE PESI ───────────────────────────────────────────
def optimize_weights(scores: dict, prev_weights: dict) -> dict:
    """
    1. Seleziona i top MAX_POSITIONS ETF per score
    2. Assegna peso proporzionale allo score con vincoli min/max
    3. Aggiustamento momentum: piccolo boost a chi ha migliorato
    4. Risk-scaling per Efficient Core (fattore 1.5)
    5. Normalizza a 100%
    """
    etf_map = {e["ticker"]: e for e in UNIVERSE}
    sorted_etf = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Seleziona top ETF
    selected = [tk for tk, _ in sorted_etf[:MAX_POSITIONS]]

    # Score grezzo → peso raw (con risk scaling)
    raw = {}
    for tk in selected:
        e     = etf_map[tk]
        sc    = scores[tk]
        risk  = e["risk"]
        # Penalizza gli Efficient Core per il rischio implicito
        effective = sc / risk
        raw[tk] = max(effective, 1)

    # Prima normalizzazione
    tot = sum(raw.values())
    weights = {tk: raw[tk] / tot * 100 for tk in selected}

    # Applica vincoli min/max per categoria
    def apply_constraints(w: dict) -> dict:
        for tk in list(w.keys()):
            cat   = etf_map[tk]["cat"]
            wmax  = MAX_W.get(cat, 25)
            w[tk] = max(MIN_W_ACTIVE, min(wmax, w[tk]))
        # Ri-normalizza
        s = sum(w.values())
        return {tk: v/s*100 for tk,v in w.items()}

    # 3 iterazioni per convergenza
    for _ in range(3):
        weights = apply_constraints(weights)

    # Arrotonda a interi (preserva 100%)
    rounded = {tk: int(v) for tk, v in weights.items()}
    diff = 100 - sum(rounded.values())
    if diff != 0:
        # Distribuisce il resto ai più alti score
        keys = sorted(rounded, key=lambda k: weights[k], reverse=True)
        for i in range(abs(diff)):
            rounded[keys[i % len(keys)]] += 1 if diff > 0 else -1

    # Rimuovi ETF con peso 0
    return {tk: w for tk, w in rounded.items() if w > 0}

# ── SEGNALE RIBILANCIAMENTO ───────────────────────────────────────
def rebalance_signal(new_w: dict, prev_w: dict, prev_regime: str, curr_regime: str) -> tuple[str, str]:
    if not prev_w:
        return "INIT", "Prima configurazione del portafoglio"

    # Calcola deviazioni
    all_tickers = set(new_w.keys()) | set(prev_w.keys())
    deviations  = {tk: abs(new_w.get(tk,0) - prev_w.get(tk,0)) for tk in all_tickers}
    max_dev     = max(deviations.values()) if deviations else 0
    avg_dev     = sum(deviations.values()) / len(deviations) if deviations else 0

    # ETF entrati/usciti
    entered = [tk for tk in new_w if tk not in prev_w]
    exited  = [tk for tk in prev_w if tk not in new_w]

    regime_changed = prev_regime != curr_regime

    # Decisione
    if max_dev > 8 or regime_changed:
        if regime_changed:
            reason = f"Cambio regime: {prev_regime} → {curr_regime}"
        else:
            tk_max = max(deviations, key=deviations.get)
            reason = f"Deviazione massima {max_dev:.0f}% su {tk_max}"
        if entered or exited:
            reason += f" · {', '.join(entered)} in · {', '.join(exited)} out" if entered or exited else ""
        return "REBALANCE", reason

    if max_dev > 3 or avg_dev > 2:
        reason = f"Deviazione {max_dev:.0f}% (avg {avg_dev:.1f}%)"
        return "PARTIAL", reason

    return "HOLD", f"Portafoglio stabile (dev max {max_dev:.1f}%)"

# ── DOMINANT SCENARIO ─────────────────────────────────────────────
def dominant(scenarios: dict) -> str:
    return max(scenarios, key=lambda k: scenarios.get(k,0)) if scenarios else "NEUTRO"

# ── MAIN ──────────────────────────────────────────────────────────
def run():
    BASE   = Path(__file__).parent
    LATEST = BASE / "data" / "latest.json"
    PF_OUT = BASE / "data" / "portfolio.json"

    # 1. Carica latest.json
    print("📂 Caricamento latest.json...")
    with open(LATEST, encoding="utf-8") as f:
        latest = json.load(f)

    sw = latest.get("scenario_weights", [])
    if not sw:
        print("⚠  scenario_weights vuoto — uscita")
        return

    current = sw[-1]
    scenarios = current.get("scenarios", {})
    curr_regime = dominant(scenarios)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"📅 Data: {today_str}  |  Regime dominante: {curr_regime}")

    # 2. Carica storico portafoglio
    history = []
    prev_weights = {}
    prev_regime  = ""
    if PF_OUT.exists():
        try:
            with open(PF_OUT, encoding="utf-8") as f:
                pf_data = json.load(f)
            history = pf_data.get("history", [])
            if history:
                prev_week = history[0]
                prev_weights = {e["ticker"]: e["weight"] for e in prev_week.get("portfolio",[])}
                prev_regime  = prev_week.get("regime","")
            print(f"📊 Storico: {len(history)} settimane")
        except Exception as e:
            print(f"⚠  Errore lettura storico: {e}")

    # 3. Fetch prezzi
    print("\n📡 Download prezzi...")
    tickers = [e["ticker"] for e in UNIVERSE]
    prices  = fetch_prices(tickers)

    # 4. Calcola scores
    print("\n🧮 Calcolo scores...")
    m_sc  = macro_scores(scenarios)
    mo_sc = momentum_score(prices)
    q_sc  = quality_scores()
    f_sc  = final_scores(m_sc, mo_sc, q_sc)

    # 5. Ottimizza pesi
    print("\n⚖️  Ottimizzazione pesi...")
    weights = optimize_weights(f_sc, prev_weights)

    # 6. Segnale ribilanciamento
    signal, reason = rebalance_signal(weights, prev_weights, prev_regime, curr_regime)
    print(f"\n🚦 Segnale: {signal} — {reason}")

    # 7. Costruisce entry portafoglio
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

    # Macro breakdown
    macro_bd = {}
    for cat in CATS:
        macro_bd[cat] = sum(e["weight"] for e in portfolio_list if e["cat"]==cat)

    # 8. Entry settimana
    week_entry = {
        "date":         today_str,
        "regime":       curr_regime,
        "regime_probs": {k:v for k,v in scenarios.items() if v>0},
        "rebalance":    signal,
        "rebalance_reason": reason,
        "portfolio":    portfolio_list,
        "macro_breakdown": macro_bd,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Stampa riepilogo
    print("\n📐 PORTAFOGLIO OTTIMIZZATO:")
    print(f"   {'Ticker':<10} {'Nome':<26} {'Cat':<10} {'Peso':>5}  {'Δ':>5}  {'Score':>5}")
    print(f"   {'-'*68}")
    for e in portfolio_list:
        delta_str = f"{e['weight_delta']:+.0f}%" if e['weight_prev'] else " NEW"
        print(f"   {e['ticker']:<10} {e['name']:<26} {e['cat']:<10} {e['weight']:>4}%  {delta_str:>5}  {e['final_score']:>5}")
    print(f"\n   MACRO BREAKDOWN: " + " | ".join(f"{k}: {v}%" for k,v in macro_bd.items()))

    # 9. Upsert history
    idx = next((i for i,h in enumerate(history) if h.get("date")==today_str), None)
    if idx is not None:
        history[idx] = week_entry
        print(f"\n🔄 Aggiornato record {today_str}")
    else:
        history.insert(0, week_entry)
        print(f"\n➕ Nuovo record {today_str}")
    history = history[:52]  # max 1 anno

    # 10. Salva
    PF_OUT.parent.mkdir(parents=True, exist_ok=True)
    out = {"history": history}
    with open(PF_OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Salvato {PF_OUT} ({len(history)} settimane)")

if __name__ == "__main__":
    run()
