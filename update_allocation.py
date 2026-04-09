#!/usr/bin/env python3
"""
RAPTOR MACRO MOVER — update_allocation.py
══════════════════════════════════════════
Legge data/latest.json, calcola l'allocazione blended del giorno
(Opzione C: media pesata sul vettore scenari corrente) e aggiunge
una riga a data/allocation_history.json.

Viene chiamato dal workflow GitHub Actions dopo che latest.json
è già stato aggiornato dallo script principale.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

# ─────────────────────────────────────────────
#  CONFIGURAZIONE — deve corrispondere 1:1 al JS
# ─────────────────────────────────────────────

SUBCLASSES = [
    {"key": "eq_growth",  "label": "Equity Growth",      "class": "EQUITY",  "tickers": ["SWRD.MI", "CSPX.MI", "EQQQ.MI"]},
    {"key": "eq_value",   "label": "Equity Value",       "class": "EQUITY",  "tickers": ["IWVL.MI", "XDEV.MI"]},
    {"key": "eq_em",      "label": "Equity EM",          "class": "EQUITY",  "tickers": ["EIMI.MI", "IEMA.MI"]},
    {"key": "bond_ct",    "label": "Bond Corti",         "class": "BOND",    "tickers": ["XEON.MI", "IS3N.MI", "IB01.SW"]},
    {"key": "bond_inf",   "label": "Bond Inflation",     "class": "BOND",    "tickers": ["XTIP.MI", "TIP1A.MI"]},
    {"key": "bond_hy",    "label": "Bond HY",            "class": "BOND",    "tickers": ["IHYG.MI", "EHYA.MI"]},
    {"key": "oro",        "label": "Oro",                "class": "MATERIE", "tickers": ["PHAG.MI", "PHAU.MI", "SGBS.MI"]},
    {"key": "energia",    "label": "Energia",            "class": "MATERIE", "tickers": ["WTI.MI", "BRENT.MI", "CRUD.MI"]},
    {"key": "mat_ind",    "label": "Mat. Industriali",   "class": "MATERIE", "tickers": ["COPA.MI", "NICK.MI", "ALUM.MI"]},
]

# Allocazione base % per regime (identica al frontend)
ALLOC_BASE = {
    "GOLDILOCKS":    {"eq_growth": 35, "eq_value": 15, "eq_em": 10, "bond_ct":  5, "bond_inf":  5, "bond_hy": 10, "oro":  8, "energia":  7, "mat_ind":  5},
    "REFLAZIONE":    {"eq_growth": 30, "eq_value": 15, "eq_em": 15, "bond_ct":  5, "bond_inf":  5, "bond_hy": 12, "oro":  8, "energia":  5, "mat_ind":  5},
    "DISINFLAZIONE": {"eq_growth": 28, "eq_value": 14, "eq_em": 10, "bond_ct": 10, "bond_inf":  8, "bond_hy": 10, "oro":  8, "energia":  6, "mat_ind":  6},
    "EUFORIA":       {"eq_growth": 35, "eq_value": 10, "eq_em": 15, "bond_ct":  5, "bond_inf":  3, "bond_hy": 15, "oro":  5, "energia":  7, "mat_ind":  5},
    "TIGHTENING":    {"eq_growth": 15, "eq_value": 15, "eq_em":  5, "bond_ct": 20, "bond_inf": 15, "bond_hy":  5, "oro": 10, "energia": 10, "mat_ind":  5},
    "STAGFLAZIONE":  {"eq_growth":  8, "eq_value": 12, "eq_em":  5, "bond_ct": 10, "bond_inf": 25, "bond_hy":  5, "oro": 20, "energia": 10, "mat_ind":  5},
    "RECESSIONE":    {"eq_growth":  5, "eq_value": 10, "eq_em":  0, "bond_ct": 30, "bond_inf": 20, "bond_hy":  2, "oro": 20, "energia":  8, "mat_ind":  5},
    "RISK_OFF":      {"eq_growth":  5, "eq_value":  8, "eq_em":  0, "bond_ct": 30, "bond_inf": 20, "bond_hy":  2, "oro": 22, "energia":  8, "mat_ind":  5},
    "ZIRP":          {"eq_growth": 20, "eq_value": 12, "eq_em":  8, "bond_ct": 20, "bond_inf": 12, "bond_hy":  8, "oro": 10, "energia":  5, "mat_ind":  5},
    "GEO_SHOCK":     {"eq_growth":  8, "eq_value": 10, "eq_em":  0, "bond_ct": 25, "bond_inf": 18, "bond_hy":  2, "oro": 22, "energia": 10, "mat_ind":  5},
    "PANDEMIC":      {"eq_growth":  3, "eq_value":  5, "eq_em":  0, "bond_ct": 35, "bond_inf": 20, "bond_hy":  0, "oro": 27, "energia":  5, "mat_ind":  5},
    "FINANCIAL":     {"eq_growth":  3, "eq_value":  5, "eq_em":  0, "bond_ct": 35, "bond_inf": 20, "bond_hy":  0, "oro": 27, "energia":  5, "mat_ind":  5},
    "WAR":           {"eq_growth":  5, "eq_value":  8, "eq_em":  0, "bond_ct": 25, "bond_inf": 18, "bond_hy":  2, "oro": 25, "energia": 12, "mat_ind":  5},
    "SOVEREIGN":     {"eq_growth":  5, "eq_value":  8, "eq_em":  0, "bond_ct": 30, "bond_inf": 20, "bond_hy":  0, "oro": 22, "energia": 10, "mat_ind":  5},
}

# Rendimenti attesi base % a 4 settimane per regime
RET_BASE = {
    "GOLDILOCKS":    {"eq_growth": 2.2, "eq_value": 1.5, "eq_em": 1.8, "bond_ct": 0.1, "bond_inf": 0.3, "bond_hy": 0.7, "oro": 0.5, "energia": 1.2, "mat_ind": 1.0},
    "REFLAZIONE":    {"eq_growth": 1.8, "eq_value": 1.2, "eq_em": 2.0, "bond_ct": 0.1, "bond_inf": 0.2, "bond_hy": 0.9, "oro": 0.3, "energia": 0.8, "mat_ind": 0.8},
    "DISINFLAZIONE": {"eq_growth": 1.5, "eq_value": 1.0, "eq_em": 0.8, "bond_ct": 0.4, "bond_inf": 0.5, "bond_hy": 0.5, "oro": 0.6, "energia": 0.2, "mat_ind": 0.3},
    "EUFORIA":       {"eq_growth": 3.0, "eq_value": 1.8, "eq_em": 2.5, "bond_ct": 0.0, "bond_inf": 0.1, "bond_hy": 1.2, "oro": 0.2, "energia": 1.5, "mat_ind": 1.2},
    "TIGHTENING":    {"eq_growth":-0.5, "eq_value": 0.2, "eq_em":-1.0, "bond_ct": 0.0, "bond_inf": 0.8, "bond_hy":-0.5, "oro": 0.8, "energia": 1.0, "mat_ind": 0.5},
    "STAGFLAZIONE":  {"eq_growth":-0.5, "eq_value": 0.2, "eq_em":-0.8, "bond_ct": 0.1, "bond_inf": 1.2, "bond_hy":-0.2, "oro": 1.5, "energia": 2.0, "mat_ind": 1.5},
    "RECESSIONE":    {"eq_growth":-2.5, "eq_value":-1.5, "eq_em":-3.0, "bond_ct": 0.5, "bond_inf": 1.0, "bond_hy":-1.5, "oro": 2.5, "energia":-0.3, "mat_ind":-1.0},
    "RISK_OFF":      {"eq_growth":-1.8, "eq_value":-0.8, "eq_em":-2.5, "bond_ct": 0.4, "bond_inf": 0.9, "bond_hy":-1.0, "oro": 2.0, "energia": 0.5, "mat_ind":-0.5},
    "ZIRP":          {"eq_growth": 0.8, "eq_value": 0.5, "eq_em": 0.5, "bond_ct": 0.3, "bond_inf": 0.4, "bond_hy": 0.4, "oro": 0.5, "energia": 0.3, "mat_ind": 0.2},
    "GEO_SHOCK":     {"eq_growth":-1.0, "eq_value":-0.5, "eq_em":-1.5, "bond_ct": 0.3, "bond_inf": 0.7, "bond_hy":-0.8, "oro": 1.8, "energia": 1.5, "mat_ind":-0.3},
    "PANDEMIC":      {"eq_growth":-4.0, "eq_value":-2.5, "eq_em":-4.5, "bond_ct": 0.8, "bond_inf": 1.5, "bond_hy":-2.5, "oro": 3.5, "energia":-1.0, "mat_ind":-2.0},
    "FINANCIAL":     {"eq_growth":-4.5, "eq_value":-3.0, "eq_em":-5.0, "bond_ct": 1.0, "bond_inf": 1.8, "bond_hy":-3.0, "oro": 4.0, "energia":-1.5, "mat_ind":-2.5},
    "WAR":           {"eq_growth":-2.0, "eq_value":-1.0, "eq_em":-2.5, "bond_ct": 0.5, "bond_inf": 1.0, "bond_hy":-1.2, "oro": 2.5, "energia": 2.5, "mat_ind":-0.5},
    "SOVEREIGN":     {"eq_growth":-1.5, "eq_value":-0.8, "eq_em":-2.0, "bond_ct": 0.5, "bond_inf": 1.2, "bond_hy":-1.5, "oro": 2.0, "energia": 0.8, "mat_ind":-0.3},
}

# Fallback neutro se il codice scenario non è mappato
ALLOC_NEUTRO = {"eq_growth": 20, "eq_value": 15, "eq_em": 10, "bond_ct": 15, "bond_inf": 10, "bond_hy": 8, "oro": 12, "energia": 5, "mat_ind": 5}
RET_NEUTRO   = {"eq_growth": 0.8, "eq_value": 0.6, "eq_em": 0.5, "bond_ct": 0.2, "bond_inf": 0.3, "bond_hy": 0.3, "oro": 0.4, "energia": 0.3, "mat_ind": 0.2}


def get_dominant(scenarios: dict) -> str:
    """Restituisce il codice scenario con probabilità più alta."""
    if not scenarios:
        return "NEUTRO"
    return max(scenarios, key=lambda k: scenarios.get(k, 0))


def compute_allocation(scenarios: dict) -> tuple[dict, dict, float]:
    """
    Calcola allocazione blended (Opzione C) pesata sul vettore scenari.
    Restituisce (alloc_pct, ret_pct, portfolio_ret_4w).
    """
    alloc_raw = {sc["key"]: 0.0 for sc in SUBCLASSES}
    ret_raw   = {sc["key"]: 0.0 for sc in SUBCLASSES}
    tot_weight = sum(scenarios.values())

    if tot_weight == 0:
        tot_weight = 1.0

    for code, pct in scenarios.items():
        if pct <= 0:
            continue
        w      = pct / tot_weight
        a_base = ALLOC_BASE.get(code, ALLOC_NEUTRO)
        r_base = RET_BASE.get(code, RET_NEUTRO)
        for sc in SUBCLASSES:
            alloc_raw[sc["key"]] += w * a_base.get(sc["key"], 0)
            ret_raw[sc["key"]]   += w * r_base.get(sc["key"], 0)

    # Normalizza allocazione a 100
    tot_alloc = sum(alloc_raw.values())
    alloc_pct = {}
    if tot_alloc > 0:
        running = 0
        keys = list(alloc_raw.keys())
        for i, k in enumerate(keys):
            if i < len(keys) - 1:
                v = round(alloc_raw[k] / tot_alloc * 100)
            else:
                v = 100 - running  # l'ultimo aggiusta il rounding
            alloc_pct[k] = v
            running += v
    else:
        alloc_pct = {sc["key"]: 0 for sc in SUBCLASSES}

    # Rendimento portafoglio atteso 4w
    port_ret = sum(
        (alloc_pct[sc["key"]] / 100) * ret_raw[sc["key"]]
        for sc in SUBCLASSES
    )

    return alloc_pct, ret_raw, round(port_ret, 3)


def build_entry(scenarios: dict, date_str: str) -> dict:
    """Costruisce una riga di storico allocazione."""
    alloc, ret, port_ret = compute_allocation(scenarios)
    dominant = get_dominant(scenarios)

    # Macro-classi
    macro = {"EQUITY": 0, "BOND": 0, "MATERIE": 0}
    for sc in SUBCLASSES:
        macro[sc["class"]] = macro.get(sc["class"], 0) + alloc.get(sc["key"], 0)

    entry = {
        "date":    date_str,
        "regime":  dominant,
        "regime_scenarios": {k: v for k, v in scenarios.items() if v > 0},
        # Allocazioni per sotto-classe
        **{sc["key"]: alloc.get(sc["key"], 0) for sc in SUBCLASSES},
        # Rendimenti attesi per sotto-classe
        **{f"ret_{sc['key']}": round(ret.get(sc["key"], 0), 3) for sc in SUBCLASSES},
        # Macro-classi
        "macro_equity":   macro["EQUITY"],
        "macro_bond":     macro["BOND"],
        "macro_materie":  macro["MATERIE"],
        # Rendimento portafoglio
        "ret_att_4w":     port_ret,
        "generated_at":  datetime.now(timezone.utc).isoformat(),
    }
    return entry


def update_history(latest_path: str, history_path: str) -> None:
    """Carica latest.json, calcola allocazione, aggiorna history."""

    # 1. Carica latest.json
    print(f"📂 Caricamento {latest_path}...")
    with open(latest_path, "r", encoding="utf-8") as f:
        latest = json.load(f)

    sw = latest.get("scenario_weights", [])
    if not sw:
        print("⚠  scenario_weights vuoto — salto aggiornamento storico.")
        return

    # Data corrente e scenario corrente (ultima settimana)
    current_week = latest.get("current_week") or sw[-1]["date"]
    current_entry = sw[-1]
    scenarios = current_entry.get("scenarios", {})

    # Usa la data di OGGI per il record storico
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"📅 Data: {today_str} · Settimana corrente: {current_week}")
    print(f"🎯 Regime dominante: {get_dominant(scenarios)}")

    # 2. Carica storico esistente
    history = []
    if os.path.exists(history_path):
        try:
            with open(history_path, "r", encoding="utf-8") as f:
                history = json.load(f)
            print(f"📊 Storico esistente: {len(history)} record")
        except (json.JSONDecodeError, Exception) as e:
            print(f"⚠  Errore lettura storico: {e} — creo nuovo file")
            history = []
    else:
        print("📊 Storico non esistente — creo nuovo file")

    # 3. Calcola entry odierna
    entry = build_entry(scenarios, today_str)

    # Stampa riepilogo
    alloc, _, port_ret = compute_allocation(scenarios)
    print("\n📐 Allocazione calcolata:")
    print(f"   {'Sotto-classe':<25} {'Alloc%':>7}  {'Tickers'}")
    print(f"   {'-'*55}")
    for sc in SUBCLASSES:
        print(f"   {sc['label']:<25} {alloc[sc['key']]:>6}%  {', '.join(sc['tickers'])}")
    macro_e = sum(alloc[sc["key"]] for sc in SUBCLASSES if sc["class"] == "EQUITY")
    macro_b = sum(alloc[sc["key"]] for sc in SUBCLASSES if sc["class"] == "BOND")
    macro_m = sum(alloc[sc["key"]] for sc in SUBCLASSES if sc["class"] == "MATERIE")
    print(f"\n   Macro → Equity: {macro_e}%  Bond: {macro_b}%  Materie: {macro_m}%")
    print(f"   Rendimento portafoglio atteso 4w: {port_ret:+.2f}%")

    # 4. Upsert sul record di oggi
    idx = next((i for i, h in enumerate(history) if h.get("date") == today_str), None)
    if idx is not None:
        print(f"\n🔄 Aggiornamento record {today_str} (già esistente)")
        history[idx] = entry
    else:
        print(f"\n➕ Nuovo record per {today_str}")
        history.insert(0, entry)  # più recente in cima

    # Mantieni max 365 record (~ 1 anno)
    history = history[:365]

    # 5. Salva
    Path(history_path).parent.mkdir(parents=True, exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Salvato {history_path} ({len(history)} record)")


if __name__ == "__main__":
    BASE_DIR    = Path(__file__).parent
    LATEST_PATH = str(BASE_DIR / "data" / "latest.json")
    HIST_PATH   = str(BASE_DIR / "data" / "allocation_history.json")
    update_history(LATEST_PATH, HIST_PATH)
