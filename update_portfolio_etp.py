#!/usr/bin/env python3
"""
RAPTOR PORTFOLIO ETP — motore dedicato
════════════════════════════════════════
Universo ristretto a 14 strumenti (12 ETP "puri" + XMME/XCHA come eccezione EM/Asia).
PHAU e CRUD limitati al 25% ciascuno. XEON entra solo nei regimi di crisi.

DIFFERENZA CHIAVE rispetto al motore principale:
  Niente Quality Score. Il bilanciamento Macro/Momentum è DINAMICO:
  - regime dominante >=70% probabilita' -> Momentum 80% / Macro 20%
  - regime dominante <=30% probabilita' -> Momentum 30% / Macro 70%
  - in mezzo -> interpolazione lineare

Riusa fetch_prices, fetch_benchmark_prices, update_nav_history, rebalance_signal
dal modulo condiviso raptor_common.py.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import raptor_common as rc

# ── UNIVERSO ETP (14) ──────────────────────────────────────────────
UNIVERSE = [
    {"ticker":"WWRD.MI","name":"WT World",              "cat":"EQUITY",   "area":"GLOBAL", "risk":1.0,"cur":"USD","fiscal":"ETP"},
    {"ticker":"WSPX.MI","name":"WT S&P 500",             "cat":"EQUITY",   "area":"USA",    "risk":1.0,"cur":"USD","fiscal":"ETP"},
    {"ticker":"WSPE.MI","name":"WT S&P 500 EUR Hed",     "cat":"EQUITY",   "area":"USA",    "risk":1.0,"cur":"EUR","fiscal":"ETP"},
    {"ticker":"WNAS.MI","name":"WT Nasdaq-100",          "cat":"EQUITY",   "area":"USA",    "risk":1.0,"cur":"USD","fiscal":"ETP"},
    {"ticker":"WRTY.MI","name":"WT Russell 2000 EC",     "cat":"EFF_CORE", "area":"USA",    "risk":1.5,"cur":"USD","fiscal":"ETP"},
    {"ticker":"NTSX.MI","name":"WT US Efficient Core",   "cat":"EFF_CORE", "area":"USA",    "risk":1.5,"cur":"USD","fiscal":"ETP"},
    {"ticker":"WS5X.MI","name":"WT Euro Stoxx 50",       "cat":"EQUITY",   "area":"EUROPA", "risk":1.0,"cur":"EUR","fiscal":"ETP"},
    {"ticker":"NTSZ.MI","name":"WT Eurozone Eff. Core",  "cat":"EFF_CORE", "area":"EUROPA", "risk":1.5,"cur":"EUR","fiscal":"ETP"},
    {"ticker":"NTSG.MI","name":"WT Global Eff. Core",    "cat":"EFF_CORE", "area":"GLOBAL", "risk":1.5,"cur":"USD","fiscal":"ETP"},
    {"ticker":"XMME.MI","name":"Xtrackers MSCI EM",      "cat":"EQUITY_EM","area":"EM_ASIA","risk":1.1,"cur":"USD","fiscal":"ETF"},
    {"ticker":"XCHA.MI","name":"iShares China",          "cat":"EQUITY_EM","area":"EM_ASIA","risk":1.1,"cur":"USD","fiscal":"ETF"},
    {"ticker":"PHAU.MI","name":"WT Physical Gold",       "cat":"GOLD",     "area":"COMMODITY","risk":1.0,"cur":"USD","fiscal":"ETP"},
    {"ticker":"CRUD.MI","name":"WT WTI Crude Oil",       "cat":"ENERGY",   "area":"COMMODITY","risk":1.3,"cur":"USD","fiscal":"ETP"},
    {"ticker":"XEON.MI","name":"Xtrackers EUR Overnight","cat":"CASH",     "area":"CASH",   "risk":0.0,"cur":"EUR","fiscal":"ETF"},
]

CATS = ["EQUITY","EQUITY_EM","EFF_CORE","GOLD","ENERGY","CASH"]
AREAS = ["USA","EUROPA","GLOBAL","EM_ASIA","COMMODITY","CASH"]
AREA_LABELS = {"USA":"USA","EUROPA":"Europa","GLOBAL":"Global","EM_ASIA":"EM/Asia","COMMODITY":"Materie prime","CASH":"Cash"}

# ── VINCOLI ─────────────────────────────────────────────────────────
MAX_W = {
    "EQUITY":    30,
    "EQUITY_EM": 20,
    "EFF_CORE":  25,
    "GOLD":      25,   # limite richiesto
    "ENERGY":    25,   # limite richiesto
    "CASH":      70,   # solo in crisi
}
MIN_W_ACTIVE  = 4
MAX_POSITIONS = 10
MIN_POSITIONS = 6

# ── PREFERENZE MACRO PER CATEGORIA (riusa logica del motore principale) ──
MACRO_PREF = {
    "GOLDILOCKS":    {"EQUITY":90,"EQUITY_EM":60,"EFF_CORE":85,"GOLD":30,"ENERGY":40,"CASH":0},
    "REFLAZIONE":    {"EQUITY":75,"EQUITY_EM":75,"EFF_CORE":70,"GOLD":70,"ENERGY":80,"CASH":0},
    "DISINFLAZIONE": {"EQUITY":70,"EQUITY_EM":45,"EFF_CORE":75,"GOLD":50,"ENERGY":25,"CASH":5},
    "EUFORIA":       {"EQUITY":95,"EQUITY_EM":75,"EFF_CORE":80,"GOLD":20,"ENERGY":60,"CASH":0},
    "TIGHTENING":    {"EQUITY":30,"EQUITY_EM":15,"EFF_CORE":35,"GOLD":55,"ENERGY":45,"CASH":15},
    "STAGFLAZIONE":  {"EQUITY":20,"EQUITY_EM":10,"EFF_CORE":25,"GOLD":80,"ENERGY":75,"CASH":20},
    "RECESSIONE":    {"EQUITY": 5,"EQUITY_EM": 0,"EFF_CORE":10,"GOLD":70,"ENERGY":10,"CASH":60},
    "RISK_OFF":      {"EQUITY":10,"EQUITY_EM": 0,"EFF_CORE":15,"GOLD":75,"ENERGY":15,"CASH":65},
    "ZIRP":          {"EQUITY":65,"EQUITY_EM":55,"EFF_CORE":70,"GOLD":60,"ENERGY":35,"CASH":0},
    "GEO_SHOCK":     {"EQUITY":15,"EQUITY_EM": 5,"EFF_CORE":20,"GOLD":85,"ENERGY":70,"CASH":55},
    "PANDEMIC":      {"EQUITY": 5,"EQUITY_EM": 0,"EFF_CORE": 5,"GOLD":60,"ENERGY": 0,"CASH":70},
    "FINANCIAL":     {"EQUITY": 0,"EQUITY_EM": 0,"EFF_CORE": 5,"GOLD":65,"ENERGY": 0,"CASH":70},
    "WAR":           {"EQUITY":10,"EQUITY_EM": 0,"EFF_CORE":15,"GOLD":85,"ENERGY":80,"CASH":60},
    "SOVEREIGN":     {"EQUITY":10,"EQUITY_EM": 0,"EFF_CORE":15,"GOLD":70,"ENERGY":20,"CASH":55},
}
MACRO_PREF_DEFAULT = {"EQUITY":50,"EQUITY_EM":35,"EFF_CORE":50,"GOLD":40,"ENERGY":30,"CASH":10}

CRISIS_REGIMES = {"RISK_OFF","RECESSIONE","PANDEMIC","FINANCIAL","WAR","SOVEREIGN","GEO_SHOCK"}


# ── MACRO SCORE ───────────────────────────────────────────────────
def macro_scores(scenarios: dict) -> dict:
    tot_w = sum(scenarios.values()) or 1
    scores = {}
    for etf in UNIVERSE:
        tk  = etf["ticker"]
        cat = etf["cat"]
        sc  = 0.0
        for code, pct in scenarios.items():
            w  = pct / tot_w
            cp = MACRO_PREF.get(code, MACRO_PREF_DEFAULT).get(cat, 50)
            sc += w * cp
        scores[tk] = min(100, round(sc))
    return scores


# ── PESO DINAMICO MOMENTUM/MACRO ───────────────────────────────────
def dynamic_weights(regime_prob: float) -> tuple:
    """
    Interpola tra (Momentum 30%/Macro 70%) e (Momentum 80%/Macro 20%)
    in base alla probabilita' del regime dominante (0-100).
    """
    lo, hi = 30.0, 70.0
    mom_lo, mom_hi = 30.0, 80.0
    if regime_prob <= lo:
        mom = mom_lo
    elif regime_prob >= hi:
        mom = mom_hi
    else:
        t = (regime_prob - lo) / (hi - lo)
        mom = mom_lo + t * (mom_hi - mom_lo)
    macro = 100.0 - mom
    return round(mom, 1), round(macro, 1)


# ── SCORE FINALE (dinamico, no Quality) ────────────────────────────
def final_scores(macro: dict, mom: dict, mom_weight: float, macro_weight: float) -> dict:
    scores = {}
    for etf in UNIVERSE:
        tk = etf["ticker"]
        m  = macro.get(tk, 50)
        mo = mom.get(tk, 50)
        scores[tk] = round(m * (macro_weight/100) + mo * (mom_weight/100))
    return scores


# ── OTTIMIZZAZIONE PESI ───────────────────────────────────────────
def optimize_weights(scores: dict, prev_weights: dict, scenarios: dict) -> dict:
    etf_map = {e["ticker"]: e for e in UNIVERSE}

    dominant_regime = max(scenarios, key=lambda k: scenarios.get(k, 0)) if scenarios else ""
    is_crisis = dominant_regime in CRISIS_REGIMES
    crisis_intensity = scenarios.get(dominant_regime, 0) / 100 if is_crisis else 0

    # XEON entra in selezione SOLO se in crisi
    candidates = [e for e in UNIVERSE if e["cat"] != "CASH"]
    sorted_etf = sorted(
        [(e["ticker"], scores[e["ticker"]]) for e in candidates],
        key=lambda x: x[1], reverse=True
    )
    selected = [tk for tk, _ in sorted_etf[:MAX_POSITIONS]]

    xeon = "XEON.MI"
    if is_crisis:
        if xeon not in selected:
            selected = selected[:-1] + [xeon]
    # fuori crisi: XEON resta sempre escluso

    raw = {}
    for tk in selected:
        e    = etf_map[tk]
        sc   = scores[tk]
        risk = e["risk"]
        if e["cat"] == "CASH":
            raw[tk] = max(sc, 1)
        else:
            raw[tk] = max(sc / max(risk, 0.1), 1)

    tot     = sum(raw.values())
    weights = {tk: raw[tk] / tot * 100 for tk in selected}

    if is_crisis and xeon in weights:
        boost        = crisis_intensity * MAX_W["CASH"]
        weights[xeon] = min(MAX_W["CASH"], weights.get(xeon, 0) + boost)
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

    rounded = {tk: int(v) for tk, v in weights.items()}
    diff    = 100 - sum(rounded.values())
    if diff != 0:
        keys = sorted(rounded, key=lambda k: weights[k], reverse=True)
        for i in range(abs(diff)):
            rounded[keys[i % len(keys)]] += 1 if diff > 0 else -1

    return {tk: w for tk, w in rounded.items() if w > 0}


# ── COPERTURA GEOGRAFICA ───────────────────────────────────────────
def geo_breakdown(portfolio_list: list) -> dict:
    bd = {a: 0 for a in AREAS}
    for e in portfolio_list:
        bd[e["area"]] = bd.get(e["area"], 0) + e["weight"]
    return {a: v for a, v in bd.items() if v > 0}


def geo_balance_status(bd: dict) -> tuple:
    if not bd:
        return "—", "—"
    max_area = max(bd, key=bd.get)
    max_val  = bd[max_area]
    if max_val < 45:
        status = "BILANCIATO"
    elif max_val < 60:
        status = "SBILANCIATO"
    else:
        status = "MOLTO_SBILANCIATO"
    return status, max_area


# ── MAIN ──────────────────────────────────────────────────────────
def run():
    BASE     = Path(__file__).parent
    LATEST   = BASE / "data" / "latest.json"
    PF_OUT   = BASE / "data" / "portfolio_etp.json"
    NAV_FILE = BASE / "nav_history_etp.json"

    print("="*60)
    print(f"RAPTOR PORTFOLIO ETP — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)

    with open(LATEST, encoding="utf-8") as f:
        latest = json.load(f)
    sw = latest.get("scenario_weights", [])
    if not sw:
        print("⚠  scenario_weights vuoto"); return

    current      = sw[-1]
    scenarios    = current.get("scenarios", {})
    curr_regime  = rc.dominant(scenarios)
    curr_prob    = scenarios.get(curr_regime, 0)
    today_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    mom_w, macro_w = dynamic_weights(curr_prob)
    print(f"📅 {today_str}  |  Regime: {curr_regime} ({curr_prob}%)")
    print(f"⚖️  Bilanciamento dinamico: Momentum {mom_w}% / Macro {macro_w}%")

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

    print("\n📡 Download prezzi ETP...")
    tickers = [e["ticker"] for e in UNIVERSE]
    prices  = rc.fetch_prices(tickers)

    print("\n📡 Download benchmark (IWMO, VNGA80)...")
    prices_bench = rc.fetch_benchmark_prices(["IWMO.MI", "VNGA80.MI"])

    print("\n🧮 Calcolo scores...")
    m_sc  = macro_scores(scenarios)
    mo_sc = rc.momentum_score(prices)
    f_sc  = final_scores(m_sc, mo_sc, mom_w, macro_w)

    print("\n⚖️  Ottimizzazione pesi...")
    weights = optimize_weights(f_sc, prev_weights, scenarios)

    signal, reason = rc.rebalance_signal(weights, prev_weights, prev_regime, curr_regime)
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
            "area":           e["area"],
            "risk_factor":    e["risk"],
            "currency":       e["cur"],
            "fiscal":         e["fiscal"],
            "weight":         w,
            "weight_prev":    prev_weights.get(tk, 0),
            "weight_delta":   w - prev_weights.get(tk, 0),
            "macro_score":    m_sc.get(tk, 50),
            "momentum_score": mo_sc.get(tk, 50),
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

    geo_bd = geo_breakdown(portfolio_list)
    geo_status, geo_max_area = geo_balance_status(geo_bd)

    week_entry = {
        "date":             today_str,
        "regime":           curr_regime,
        "regime_prob":      curr_prob,
        "regime_probs":     {k:v for k,v in scenarios.items() if v>0},
        "mom_weight":       mom_w,
        "macro_weight":     macro_w,
        "rebalance":        signal,
        "rebalance_reason": reason,
        "portfolio":        portfolio_list,
        "macro_breakdown":  macro_bd,
        "geo_breakdown":    geo_bd,
        "geo_status":       geo_status,
        "geo_max_area":     geo_max_area,
        "generated_at":     datetime.now(timezone.utc).isoformat(),
    }

    print("\n📐 PORTAFOGLIO ETP:")
    print(f"   {'Ticker':<10} {'Nome':<26} {'Area':<10} {'Peso':>5}  {'Δ':>5}  {'Score':>5}  {'Fiscal'}")
    print(f"   {'-'*76}")
    for e in portfolio_list:
        delta_str = f"{e['weight_delta']:+.0f}%" if e['weight_prev'] else " NEW"
        print(f"   {e['ticker']:<10} {e['name']:<26} {e['area']:<10} {e['weight']:>4}%  {delta_str:>5}  {e['final_score']:>5}  {e['fiscal']}")
    print(f"\n   GEO BREAKDOWN: " + " | ".join(f"{AREA_LABELS.get(k,k)}: {v}%" for k,v in geo_bd.items()))
    print(f"   STATUS: {geo_status} ({geo_max_area})")

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

    print("\n💹 Aggiornamento NAV ETP...")
    rc.update_nav_history(NAV_FILE, portfolio_list, prices_bench, today_str)

if __name__ == "__main__":
    run()
