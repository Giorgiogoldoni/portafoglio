#!/usr/bin/env python3
"""
RAPTOR COMMON — funzioni condivise tra i motori di portafoglio
════════════════════════════════════════════════════════════════
Usato da update_portfolio.py (portafoglio principale, 25 ETF)
e da update_portfolio_etp.py (Portfolio ETP, 14 strumenti).

Contiene: fetch prezzi, fetch benchmark, NAV tracking.
"""

import json, os, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

def install(pkg):
    os.system(f"pip install {pkg} --break-system-packages -q")

try:
    import yfinance as yf
except ImportError:
    install("yfinance"); import yfinance as yf


# ── FETCH PREZZI UNIVERSO ──────────────────────────────────────────
def fetch_prices(tickers: list) -> dict:
    """
    Scarica prezzi e rendimenti (1W/4W/12W) per una lista di ticker.
    Prova suffissi alternativi (.L, .PA) se .MI non disponibile.
    """
    result = {}
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=95)
    for tk in tickers:
        for suffix in [tk, tk.replace(".MI", ".L"), tk.replace(".MI", ".PA")]:
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


# ── FETCH PREZZI BENCHMARK (solo ultimo prezzo) ────────────────────
def fetch_benchmark_prices(tickers: list) -> dict:
    """Scarica solo l'ultimo prezzo disponibile per i benchmark (IWMO, VNGA80)."""
    result = {}
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=10)
    for tk in tickers:
        try:
            hist = yf.Ticker(tk).history(
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True)
            if len(hist) >= 1:
                result[tk] = float(hist["Close"].dropna().iloc[-1])
                print(f"  ✓ {tk} = {result[tk]:.4f}")
        except Exception as e:
            print(f"  ⚠  {tk}: {e}")
        time.sleep(0.2)
    return result


# ── MOMENTUM SCORE (ranking relativo 0-100) ─────────────────────────
def momentum_score(prices: dict) -> dict:
    """
    Combina r1w/r4w/r12w (pesi 30/40/30) in un composito,
    poi normalizza 0-100 relativamente all'universo passato.
    """
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


# ── NAV TRACKING ────────────────────────────────────────────────────
def update_nav_history(nav_file: Path, portfolio_list: list, prices_bench: dict, today_str: str):
    """
    Calcola NAV portafoglio (prezzi correnti vs precedenti) e benchmark IWMO/VNGA80.
    Appende un punto al file nav_file passato (path specifico per ogni portafoglio).
    """
    nav_history = []
    if nav_file.exists():
        try:
            with open(nav_file, encoding="utf-8") as f:
                nav_history = json.load(f)
        except Exception as e:
            print(f"⚠  Errore lettura {nav_file.name}: {e}")

    if not nav_history:
        # ── INIZIALIZZAZIONE: NAV = 100 ──────────────────────────
        entry = {
            "date":       today_str,
            "nav":        100.0,
            "iwmo":       100.0,
            "vnga80":     100.0,
            "prices":     {e["ticker_full"]: e["price"] for e in portfolio_list if e["price"]},
            "bench_px":   prices_bench,
            "weights":    {e["ticker_full"]: e["weight"] for e in portfolio_list},
            "ret_port":   0.0,
            "ret_iwmo":   0.0,
            "ret_vnga80": 0.0,
        }
        nav_history.append(entry)
        print(f"\n💹 NAV inizializzata a 100.00 — {today_str}")
    else:
        prev       = nav_history[-1]
        prev_px    = prev.get("prices", {})
        prev_bench = prev.get("bench_px", {})
        prev_nav   = prev.get("nav",    100.0)
        prev_iwmo  = prev.get("iwmo",   100.0)
        prev_vnga  = prev.get("vnga80", 100.0)

        # Rendimento portafoglio pesato
        ret_port = 0.0
        for etf in portfolio_list:
            tk     = etf["ticker_full"]
            w      = etf["weight"] / 100.0
            px_new = etf.get("price")
            px_old = prev_px.get(tk)
            if px_new and px_old and px_old > 0:
                ret_port += w * (px_new / px_old - 1)

        # Rendimento benchmark
        ret_iwmo = ret_vnga = 0.0
        if prices_bench.get("IWMO.MI") and prev_bench.get("IWMO.MI") and prev_bench["IWMO.MI"] > 0:
            ret_iwmo = prices_bench["IWMO.MI"] / prev_bench["IWMO.MI"] - 1
        if prices_bench.get("VNGA80.MI") and prev_bench.get("VNGA80.MI") and prev_bench["VNGA80.MI"] > 0:
            ret_vnga = prices_bench["VNGA80.MI"] / prev_bench["VNGA80.MI"] - 1

        new_nav  = round(prev_nav  * (1 + ret_port), 4)
        new_iwmo = round(prev_iwmo * (1 + ret_iwmo), 4)
        new_vnga = round(prev_vnga * (1 + ret_vnga), 4)

        entry = {
            "date":       today_str,
            "nav":        new_nav,
            "iwmo":       new_iwmo,
            "vnga80":     new_vnga,
            "prices":     {e["ticker_full"]: e["price"] for e in portfolio_list if e["price"]},
            "bench_px":   prices_bench,
            "weights":    {e["ticker_full"]: e["weight"] for e in portfolio_list},
            "ret_port":   round(ret_port * 100, 4),
            "ret_iwmo":   round(ret_iwmo * 100, 4),
            "ret_vnga80": round(ret_vnga * 100, 4),
        }

        if nav_history[-1].get("date") == today_str:
            nav_history[-1] = entry   # aggiorna stesso giorno (run multipli)
        else:
            nav_history.append(entry)

        print(f"\n💹 NAV {prev_nav:.2f} → {new_nav:.2f} ({ret_port*100:+.3f}%)")
        print(f"   IWMO   {prev_iwmo:.2f} → {new_iwmo:.2f} ({ret_iwmo*100:+.3f}%)")
        print(f"   VNGA80 {prev_vnga:.2f} → {new_vnga:.2f} ({ret_vnga*100:+.3f}%)")

    nav_history = nav_history[-500:]  # max ~2 anni (4 punti/gg × 250gg)
    nav_file.parent.mkdir(parents=True, exist_ok=True)
    with open(nav_file, "w", encoding="utf-8") as f:
        json.dump(nav_history, f, ensure_ascii=False, indent=2)
    print(f"✅ {nav_file.name} — {len(nav_history)} punti")


# ── SEGNALE RIBILANCIAMENTO (generico) ─────────────────────────────
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
