# ═══════════════════════════════════════════════════════════════════
# PATCH update_portfolio.py — aggiungere NAV tracking
# ═══════════════════════════════════════════════════════════════════
#
# 1) INCOLLA questa funzione PRIMA della funzione run()
# 2) AGGIUNGI la chiamata in fondo a run() (vedi sezione 2)
# 3) AGGIORNA il workflow update.yml (vedi sezione 3)
# ═══════════════════════════════════════════════════════════════════

# ── SEZIONE 1: nuova funzione da aggiungere prima di run() ────────

def update_nav_history(base_path, portfolio_list, prices_bench, today_str):
    """
    Calcola NAV portafoglio (prezzi correnti vs precedenti) e benchmark IWMO/VNGA80.
    Appende un punto a data/nav_history.json.
    prices_bench: dict con chiavi 'IWMO.MI' e 'VNGA80.MI' → prezzo corrente
    """
    NAV_FILE = base_path / "data" / "nav_history.json"

    nav_history = []
    if NAV_FILE.exists():
        try:
            with open(NAV_FILE, encoding="utf-8") as f:
                nav_history = json.load(f)
        except Exception as e:
            print(f"⚠  Errore lettura nav_history: {e}")

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
            nav_history[-1] = entry   # aggiorna stesso giorno
        else:
            nav_history.append(entry)

        print(f"\n💹 NAV {prev_nav:.2f} → {new_nav:.2f} ({ret_port*100:+.3f}%)")
        print(f"   IWMO   {prev_iwmo:.2f} → {new_iwmo:.2f} ({ret_iwmo*100:+.3f}%)")
        print(f"   VNGA80 {prev_vnga:.2f} → {new_vnga:.2f} ({ret_vnga*100:+.3f}%)")

    nav_history = nav_history[-500:]  # max ~2 anni (4 punti/gg × 250gg)
    NAV_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(NAV_FILE, "w", encoding="utf-8") as f:
        json.dump(nav_history, f, ensure_ascii=False, indent=2)
    print(f"✅ nav_history.json — {len(nav_history)} punti")


# ── SEZIONE 2: modifiche alla funzione run() ──────────────────────
#
# Subito DOPO il blocco "print(f"\n📡 Download prezzi...")"
# e DOPO la riga "prices  = fetch_prices(tickers)"
# AGGIUNGI queste righe per scaricare i benchmark:
#
#     print("  → benchmark IWMO, VNGA80...")
#     bench_tickers = ["IWMO.MI", "VNGA80.MI"]
#     prices_bench  = {}
#     _end   = datetime.now(timezone.utc)
#     _start = _end - timedelta(days=10)
#     for _tk in bench_tickers:
#         try:
#             _hist = yf.Ticker(_tk).history(
#                 start=_start.strftime("%Y-%m-%d"),
#                 end=_end.strftime("%Y-%m-%d"),
#                 auto_adjust=True)
#             if len(_hist) >= 1:
#                 prices_bench[_tk] = float(_hist["Close"].dropna().iloc[-1])
#                 print(f"  ✓ {_tk} = {prices_bench[_tk]:.4f}")
#         except Exception as _e:
#             print(f"  ⚠  {_tk}: {_e}")
#         time.sleep(0.2)
#
# Poi, DOPO il blocco di salvataggio di portfolio.json (il print "✅ Salvato"):
#
#     print("\n💹 Aggiornamento NAV...")
#     update_nav_history(BASE, portfolio_list, prices_bench, today_str)


# ── SEZIONE 3: modifica update.yml ───────────────────────────────
#
# Nel job "portfolio", step "Commit & push", cambia:
#
#   git add data/portfolio.json
#
# in:
#
#   git add data/portfolio.json data/nav_history.json
