#!/usr/bin/env python3
"""
RAPTOR MACRO CLASSIFIER v3.0
══════════════════════════════
Classificatore di regime basato su ETF USA come proxy di mercato.
Nessuna API key richiesta — solo Yahoo Finance.

Logica:
  1. Scarica 50+ ETF USA su periodi 1W/4W/12W/1Y
  2. Calcola momentum relativo e confronto cross-asset
  3. Determina score per ogni scenario dal comportamento degli ETF
  4. Mantiene dati macro reali (CPI, yield curve) come conferma
  5. Aggiorna 4x/giorno — scenari sempre freschi dal mercato

Output: data/latest.json
"""

import os, json, time, requests
import yfinance as yf
from datetime import datetime, timedelta, date, timezone
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────
GROQ_KEY     = os.environ.get('GROQ_API_KEY', '')
GMAIL_USER   = os.environ.get('GMAIL_USER', '')
GMAIL_PASS   = os.environ.get('GMAIL_APP_PASSWORD', '')
NOTIFY_EMAIL = os.environ.get('NOTIFY_EMAIL', '')
PAGES_URL    = os.environ.get('PAGES_URL', 'https://giorgiogoldoni.github.io/portafoglio')
DATA_FILE    = os.path.join(os.path.dirname(__file__), '..', 'data', 'latest.json')
START_DATE   = '2010-01-01'

# ── SCENARI ───────────────────────────────────────────────────────
SCENARIOS = [
    {'id':1,  'code':'GOLDILOCKS',   'name':'Goldilocks',             'color':'#00D4FF','tag':'CICLICO'},
    {'id':2,  'code':'REFLAZIONE',   'name':'Reflazione',             'color':'#7FFF00','tag':'CICLICO'},
    {'id':3,  'code':'STAGFLAZIONE', 'name':'Stagflazione',           'color':'#FF6B35','tag':'CICLICO'},
    {'id':4,  'code':'RISK_OFF',     'name':'Risk-Off Acuto',         'color':'#FF3355','tag':'CICLICO'},
    {'id':5,  'code':'DISINFLAZIONE','name':'Disinflazione Morbida',  'color':'#00BFFF','tag':'CICLICO'},
    {'id':6,  'code':'RECESSIONE',   'name':'Recessione Conclamata',  'color':'#8B0000','tag':'CICLICO'},
    {'id':7,  'code':'ZIRP',         'name':'ZIRP / Rep. Finanziaria','color':'#9B59B6','tag':'CICLICO'},
    {'id':8,  'code':'TIGHTENING',   'name':'Tightening Aggressivo',  'color':'#E74C3C','tag':'CICLICO'},
    {'id':9,  'code':'GEO_SHOCK',    'name':'Geopolitical Shock',     'color':'#F39C12','tag':'CICLICO'},
    {'id':10, 'code':'EUFORIA',      'name':'Boom Tech / Euforia',    'color':'#FFD700','tag':'CICLICO'},
    {'id':11, 'code':'PANDEMIC',     'name':'Pandemic Shock',         'color':'#FF69B4','tag':'SHOCK'},
    {'id':12, 'code':'FINANCIAL',    'name':'Financial Crisis',       'color':'#DC143C','tag':'SHOCK'},
    {'id':13, 'code':'WAR',          'name':'War / Geo Rupture',      'color':'#8B4513','tag':'SHOCK'},
    {'id':14, 'code':'SOVEREIGN',    'name':'Sovereign / Debt Crisis','color':'#4B0082','tag':'SHOCK'},
]
CODES = [s['code'] for s in SCENARIOS]

SHOCK_PERIODS = [
    {'id':12,'start':'2007-08-01','end':'2008-09-14','intensity':0.5},
    {'id':12,'start':'2008-09-15','end':'2009-03-09','intensity':1.0},
    {'id':14,'start':'2010-05-01','end':'2012-07-26','intensity':0.8},
    {'id':11,'start':'2020-02-20','end':'2020-03-23','intensity':1.0},
    {'id':11,'start':'2020-03-24','end':'2020-12-31','intensity':0.6},
    {'id':13,'start':'2022-02-24','end':'2022-03-08','intensity':1.0},
    {'id':13,'start':'2022-03-09','end':'2022-12-31','intensity':0.5},
    {'id':12,'start':'2023-03-08','end':'2023-03-31','intensity':0.4},
    {'id':9, 'start':'2026-04-02','end':None,        'intensity':0.7},
]

# ── ETF PROXY (classificatore di regime) ─────────────────────────
# Ogni ETF ha un vettore di "affinità" per ogni scenario (0-1)
ETF_PROXY = {
    # Equity USA
    'SPY':  {'GOLDILOCKS':0.9,'REFLAZIONE':0.7,'EUFORIA':0.95,'DISINFLAZIONE':0.7,'TIGHTENING':0.2,'STAGFLAZIONE':0.1,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.6,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    'QQQ':  {'GOLDILOCKS':0.9,'REFLAZIONE':0.6,'EUFORIA':1.0,'DISINFLAZIONE':0.8,'TIGHTENING':0.1,'STAGFLAZIONE':0.0,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.7,'GEO_SHOCK':0.1,'PANDEMIC':0.1,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    'IWM':  {'GOLDILOCKS':0.85,'REFLAZIONE':0.8,'EUFORIA':0.9,'DISINFLAZIONE':0.6,'TIGHTENING':0.2,'STAGFLAZIONE':0.1,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.5,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    # Equity Europa/Asia
    'VGK':  {'GOLDILOCKS':0.8,'REFLAZIONE':0.75,'EUFORIA':0.8,'DISINFLAZIONE':0.6,'TIGHTENING':0.2,'STAGFLAZIONE':0.15,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.5,'GEO_SHOCK':0.0,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.0,'SOVEREIGN':0.0},
    'EEM':  {'GOLDILOCKS':0.7,'REFLAZIONE':0.9,'EUFORIA':0.75,'DISINFLAZIONE':0.5,'TIGHTENING':0.1,'STAGFLAZIONE':0.2,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.6,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    'EWJ':  {'GOLDILOCKS':0.7,'REFLAZIONE':0.65,'EUFORIA':0.7,'DISINFLAZIONE':0.7,'TIGHTENING':0.3,'STAGFLAZIONE':0.2,'RECESSIONE':0.1,'RISK_OFF':0.2,'ZIRP':0.8,'GEO_SHOCK':0.2,'PANDEMIC':0.1,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    # Bond
    'TLT':  {'GOLDILOCKS':0.4,'REFLAZIONE':0.1,'EUFORIA':0.0,'DISINFLAZIONE':0.9,'TIGHTENING':0.0,'STAGFLAZIONE':0.1,'RECESSIONE':0.9,'RISK_OFF':0.8,'ZIRP':0.9,'GEO_SHOCK':0.6,'PANDEMIC':0.9,'FINANCIAL':0.9,'WAR':0.6,'SOVEREIGN':0.5},
    'IEF':  {'GOLDILOCKS':0.5,'REFLAZIONE':0.2,'EUFORIA':0.1,'DISINFLAZIONE':0.8,'TIGHTENING':0.1,'STAGFLAZIONE':0.2,'RECESSIONE':0.8,'RISK_OFF':0.7,'ZIRP':0.85,'GEO_SHOCK':0.5,'PANDEMIC':0.8,'FINANCIAL':0.8,'WAR':0.5,'SOVEREIGN':0.4},
    'HYG':  {'GOLDILOCKS':0.85,'REFLAZIONE':0.7,'EUFORIA':0.9,'DISINFLAZIONE':0.65,'TIGHTENING':0.1,'STAGFLAZIONE':0.1,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.8,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    'LQD':  {'GOLDILOCKS':0.6,'REFLAZIONE':0.4,'EUFORIA':0.5,'DISINFLAZIONE':0.75,'TIGHTENING':0.2,'STAGFLAZIONE':0.2,'RECESSIONE':0.4,'RISK_OFF':0.3,'ZIRP':0.8,'GEO_SHOCK':0.3,'PANDEMIC':0.5,'FINANCIAL':0.2,'WAR':0.3,'SOVEREIGN':0.2},
    'TIP':  {'GOLDILOCKS':0.5,'REFLAZIONE':0.85,'EUFORIA':0.4,'DISINFLAZIONE':0.4,'TIGHTENING':0.5,'STAGFLAZIONE':0.9,'RECESSIONE':0.3,'RISK_OFF':0.4,'ZIRP':0.3,'GEO_SHOCK':0.4,'PANDEMIC':0.3,'FINANCIAL':0.2,'WAR':0.5,'SOVEREIGN':0.3},
    'EMB':  {'GOLDILOCKS':0.7,'REFLAZIONE':0.8,'EUFORIA':0.6,'DISINFLAZIONE':0.5,'TIGHTENING':0.1,'STAGFLAZIONE':0.2,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.65,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.0},
    # Commodity
    'GLD':  {'GOLDILOCKS':0.4,'REFLAZIONE':0.8,'EUFORIA':0.3,'DISINFLAZIONE':0.3,'TIGHTENING':0.4,'STAGFLAZIONE':0.9,'RECESSIONE':0.7,'RISK_OFF':0.9,'ZIRP':0.5,'GEO_SHOCK':0.9,'PANDEMIC':0.7,'FINANCIAL':0.8,'WAR':0.95,'SOVEREIGN':0.8},
    'USO':  {'GOLDILOCKS':0.5,'REFLAZIONE':0.9,'EUFORIA':0.6,'DISINFLAZIONE':0.2,'TIGHTENING':0.4,'STAGFLAZIONE':0.85,'RECESSIONE':0.1,'RISK_OFF':0.2,'ZIRP':0.3,'GEO_SHOCK':0.85,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.9,'SOVEREIGN':0.3},
    'PDBC': {'GOLDILOCKS':0.5,'REFLAZIONE':0.85,'EUFORIA':0.5,'DISINFLAZIONE':0.2,'TIGHTENING':0.3,'STAGFLAZIONE':0.8,'RECESSIONE':0.1,'RISK_OFF':0.2,'ZIRP':0.3,'GEO_SHOCK':0.7,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.8,'SOVEREIGN':0.3},
    # Difensivi/Volatilità
    'VXX':  {'GOLDILOCKS':0.0,'REFLAZIONE':0.0,'EUFORIA':0.0,'DISINFLAZIONE':0.1,'TIGHTENING':0.3,'STAGFLAZIONE':0.4,'RECESSIONE':0.8,'RISK_OFF':1.0,'ZIRP':0.0,'GEO_SHOCK':0.8,'PANDEMIC':1.0,'FINANCIAL':1.0,'WAR':0.9,'SOVEREIGN':0.7},
    'XLU':  {'GOLDILOCKS':0.4,'REFLAZIONE':0.3,'EUFORIA':0.2,'DISINFLAZIONE':0.7,'TIGHTENING':0.3,'STAGFLAZIONE':0.5,'RECESSIONE':0.6,'RISK_OFF':0.5,'ZIRP':0.8,'GEO_SHOCK':0.4,'PANDEMIC':0.5,'FINANCIAL':0.3,'WAR':0.3,'SOVEREIGN':0.3},
    'XLP':  {'GOLDILOCKS':0.4,'REFLAZIONE':0.3,'EUFORIA':0.2,'DISINFLAZIONE':0.6,'TIGHTENING':0.3,'STAGFLAZIONE':0.5,'RECESSIONE':0.6,'RISK_OFF':0.5,'ZIRP':0.6,'GEO_SHOCK':0.4,'PANDEMIC':0.5,'FINANCIAL':0.3,'WAR':0.3,'SOVEREIGN':0.3},
    # Settoriali
    'XLE':  {'GOLDILOCKS':0.6,'REFLAZIONE':0.85,'EUFORIA':0.6,'DISINFLAZIONE':0.3,'TIGHTENING':0.4,'STAGFLAZIONE':0.8,'RECESSIONE':0.1,'RISK_OFF':0.2,'ZIRP':0.3,'GEO_SHOCK':0.8,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.9,'SOVEREIGN':0.3},
    'XLF':  {'GOLDILOCKS':0.85,'REFLAZIONE':0.7,'EUFORIA':0.9,'DISINFLAZIONE':0.6,'TIGHTENING':0.3,'STAGFLAZIONE':0.2,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.4,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.0},
    'SOXX': {'GOLDILOCKS':0.85,'REFLAZIONE':0.6,'EUFORIA':1.0,'DISINFLAZIONE':0.8,'TIGHTENING':0.1,'STAGFLAZIONE':0.0,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.7,'GEO_SHOCK':0.1,'PANDEMIC':0.1,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    # Valute
    'UUP':  {'GOLDILOCKS':0.3,'REFLAZIONE':0.2,'EUFORIA':0.2,'DISINFLAZIONE':0.4,'TIGHTENING':0.8,'STAGFLAZIONE':0.6,'RECESSIONE':0.5,'RISK_OFF':0.7,'ZIRP':0.1,'GEO_SHOCK':0.6,'PANDEMIC':0.7,'FINANCIAL':0.8,'WAR':0.6,'SOVEREIGN':0.5},
    # EM regionali
    'EWZ':  {'GOLDILOCKS':0.7,'REFLAZIONE':0.9,'EUFORIA':0.7,'DISINFLAZIONE':0.4,'TIGHTENING':0.1,'STAGFLAZIONE':0.3,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.6,'GEO_SHOCK':0.2,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    'INDA': {'GOLDILOCKS':0.75,'REFLAZIONE':0.85,'EUFORIA':0.8,'DISINFLAZIONE':0.5,'TIGHTENING':0.1,'STAGFLAZIONE':0.2,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.6,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
    'FXI':  {'GOLDILOCKS':0.65,'REFLAZIONE':0.8,'EUFORIA':0.7,'DISINFLAZIONE':0.5,'TIGHTENING':0.2,'STAGFLAZIONE':0.2,'RECESSIONE':0.0,'RISK_OFF':0.0,'ZIRP':0.6,'GEO_SHOCK':0.1,'PANDEMIC':0.0,'FINANCIAL':0.0,'WAR':0.1,'SOVEREIGN':0.1},
}

ETF_LIST = [
    {'t':'SPY', 'name':'S&P 500',         'area':'USA'},
    {'t':'QQQ', 'name':'Nasdaq 100',       'area':'USA'},
    {'t':'IWM', 'name':'Russell 2000',     'area':'USA'},
    {'t':'VGK', 'name':'Europa Broad',     'area':'Europa'},
    {'t':'EWG', 'name':'Germania',         'area':'Europa'},
    {'t':'EWI', 'name':'Italia',           'area':'Europa'},
    {'t':'EWP', 'name':'Spagna',           'area':'Europa'},
    {'t':'EWQ', 'name':'Francia',          'area':'Europa'},
    {'t':'EWU', 'name':'UK',               'area':'Europa'},
    {'t':'HEZU','name':'Eurozona Hedged',   'area':'Europa'},
    {'t':'EWJ', 'name':'Giappone',          'area':'Asia'},
    {'t':'DXJ', 'name':'Giappone Hedged',   'area':'Asia'},
    {'t':'EWH', 'name':'Hong Kong',         'area':'Asia'},
    {'t':'FXI', 'name':'Cina Large Cap',    'area':'Asia'},
    {'t':'MCHI','name':'Cina Broad',        'area':'Asia'},
    {'t':'INDA','name':'India',             'area':'Asia'},
    {'t':'EWY', 'name':'Korea',             'area':'Asia'},
    {'t':'EWT', 'name':'Taiwan',            'area':'Asia'},
    {'t':'EWA', 'name':'Australia',         'area':'Asia'},
    {'t':'EEM', 'name':'Emerging Markets',  'area':'EM'},
    {'t':'VWO', 'name':'EM Vanguard',       'area':'EM'},
    {'t':'EWZ', 'name':'Brasile',           'area':'EM'},
    {'t':'ILF', 'name':'Latin America',     'area':'EM'},
    {'t':'KSA', 'name':'Arabia Saudita',    'area':'EM'},
    {'t':'TLT', 'name':'Treasury 20Y+',     'area':'Bond'},
    {'t':'IEF', 'name':'Treasury 7-10Y',    'area':'Bond'},
    {'t':'HYG', 'name':'High Yield USA',    'area':'Bond'},
    {'t':'LQD', 'name':'Investment Grade',  'area':'Bond'},
    {'t':'TIP', 'name':'TIPS',              'area':'Bond'},
    {'t':'BNDX','name':'Bond Globali',      'area':'Bond'},
    {'t':'EMB', 'name':'EM Bond',           'area':'Bond'},
    {'t':'GLD', 'name':'Oro',               'area':'Commodity'},
    {'t':'USO', 'name':'Petrolio',          'area':'Commodity'},
    {'t':'PDBC','name':'Commodity Broad',   'area':'Commodity'},
    {'t':'DBA', 'name':'Agricoltura',       'area':'Commodity'},
    {'t':'COPX','name':'Rame',              'area':'Commodity'},
    {'t':'SLV', 'name':'Argento',           'area':'Commodity'},
    {'t':'VXX', 'name':'VIX',              'area':'Vol'},
    {'t':'XLU', 'name':'Utilities',         'area':'Settoriali'},
    {'t':'XLP', 'name':'Consumer Staples',  'area':'Settoriali'},
    {'t':'XLE', 'name':'Energy',            'area':'Settoriali'},
    {'t':'XLF', 'name':'Financials',        'area':'Settoriali'},
    {'t':'SOXX','name':'Semiconduttori',    'area':'Settoriali'},
    {'t':'IBB', 'name':'Biotech',           'area':'Settoriali'},
    {'t':'UUP', 'name':'Dollaro USA',       'area':'Valute'},
    # Nuovi geografici
    {'t':'EWL', 'name':'Svizzera',          'area':'Europa'},
    {'t':'EWN', 'name':'Olanda',            'area':'Europa'},
    {'t':'EWD', 'name':'Svezia',            'area':'Europa'},
    {'t':'EWC', 'name':'Canada',            'area':'Americas'},
    {'t':'ECH', 'name':'Cile',              'area':'EM'},
    {'t':'GXG', 'name':'Colombia',          'area':'EM'},
]

# ── FETCH ETF DATA ────────────────────────────────────────────────
def fetch_etf_data(tickers: list) -> dict:
    result = {}
    print(f"  Download {len(tickers)} ETF...")
    try:
        data = yf.download(
            tickers, period='1y', interval='1d',
            group_by='ticker', auto_adjust=True, progress=False
        )
    except Exception as e:
        print(f"  err yfinance: {e}")
        return result

    today = date.today()
    for t in tickers:
        try:
            closes = data[t]['Close'] if len(tickers)>1 else data['Close']
            closes = closes.dropna()
            if len(closes) < 5:
                continue
            price = float(closes.iloc[-1])
            def ret(days):
                fd  = today - timedelta(days=days)
                idx = closes.index.searchsorted(str(fd))
                if idx >= len(closes): return None
                old = float(closes.iloc[idx])
                return round((price-old)/old*100, 2) if old else None
            result[t] = {
                'price':   round(price, 2),
                'ret_1w':  ret(7),
                'ret_4w':  ret(30),
                'ret_12w': ret(90),
                'ret_1y':  ret(365),
                'closes':  closes,
            }
        except Exception as e:
            print(f"  err {t}: {e}")
    print(f"  Caricati: {len(result)}/{len(tickers)}")
    return result

# ── CLASSIFICA SCENARIO DA ETF ────────────────────────────────────
def classify_from_etf(etf_data: dict, date_str: str) -> dict:
    """
    Per ogni scenario calcola uno score basato su:
    1. Momentum degli ETF proxy pesato per affinità scenario
    2. Confronto cross-asset (ratio tra asset class)
    3. Shock storici
    """
    scores = {c: 0.0 for c in CODES}

    # Shock storici
    today = date.today().isoformat()
    for sp in SHOCK_PERIODS:
        end = sp['end'] or today
        if sp['start'] <= date_str <= end:
            shock_code = {11:'PANDEMIC',12:'FINANCIAL',13:'WAR',14:'SOVEREIGN',9:'GEO_SHOCK'}.get(sp['id'])
            if shock_code:
                scores[shock_code] += sp['intensity'] * 80

    # Momentum ETF proxy
    available = {t: d for t, d in etf_data.items() if t in ETF_PROXY}
    if available:
        # Normalizza momentum 4W in range -1/+1
        mom4w = {}
        for t, d in available.items():
            r = d.get('ret_4w')
            if r is not None:
                mom4w[t] = r

        if mom4w:
            vals  = list(mom4w.values())
            vmin, vmax = min(vals), max(vals)
            rng = vmax - vmin if vmax != vmin else 1

            for t, r in mom4w.items():
                norm = (r - vmin) / rng * 2 - 1  # -1 a +1
                proxy = ETF_PROXY[t]
                for code in CODES:
                    affinity = proxy.get(code, 0)
                    scores[code] += norm * affinity * 15

    # Cross-asset signals
    def get_ret(t, period='ret_4w'):
        d = etf_data.get(t, {})
        return d.get(period)

    # Equity forte vs Bond → risk-on
    spy4w = get_ret('SPY')
    tlt4w = get_ret('TLT')
    if spy4w is not None and tlt4w is not None:
        equity_vs_bond = spy4w - tlt4w
        if equity_vs_bond > 5:
            scores['GOLDILOCKS'] += 20
            scores['EUFORIA']    += 15
            scores['REFLAZIONE'] += 10
        elif equity_vs_bond < -5:
            scores['RISK_OFF']   += 20
            scores['RECESSIONE'] += 15
            scores['TIGHTENING'] += 10

    # Oro forte → inflazione/paura
    gld4w = get_ret('GLD')
    if gld4w is not None:
        if gld4w > 5:
            scores['REFLAZIONE']  += 15
            scores['STAGFLAZIONE']+= 10
            scores['GEO_SHOCK']   += 10
            scores['RISK_OFF']    += 10
        elif gld4w < -3:
            scores['GOLDILOCKS']  += 10
            scores['DISINFLAZIONE']+= 10

    # Petrolio forte → reflazione/guerra
    uso4w = get_ret('USO')
    if uso4w is not None:
        if uso4w > 8:
            scores['REFLAZIONE']  += 15
            scores['STAGFLAZIONE']+= 10
            scores['WAR']         += 8
        elif uso4w < -8:
            scores['RECESSIONE']  += 10
            scores['RISK_OFF']    += 8

    # VIX/VXX forte → risk off
    vxx4w = get_ret('VXX')
    if vxx4w is not None:
        if vxx4w > 15:
            scores['RISK_OFF']   += 25
            scores['PANDEMIC']   += 10
            scores['FINANCIAL']  += 10
        elif vxx4w < -10:
            scores['GOLDILOCKS'] += 15
            scores['EUFORIA']    += 10

    # HY spread proxy: HYG vs LQD
    hyg4w = get_ret('HYG')
    lqd4w = get_ret('LQD')
    if hyg4w is not None and lqd4w is not None:
        hy_vs_ig = hyg4w - lqd4w
        if hy_vs_ig > 3:
            scores['GOLDILOCKS'] += 15
            scores['EUFORIA']    += 10
        elif hy_vs_ig < -3:
            scores['RISK_OFF']   += 15
            scores['RECESSIONE'] += 10

    # Dollar forte → tightening/risk-off
    uup4w = get_ret('UUP')
    if uup4w is not None:
        if uup4w > 3:
            scores['TIGHTENING'] += 15
            scores['RISK_OFF']   += 10
            scores['GEO_SHOCK']  += 8
        elif uup4w < -2:
            scores['REFLAZIONE'] += 10
            scores['ZIRP']       += 8

    # EM forte vs USA → reflazione/crescita globale
    eem4w = get_ret('EEM')
    if eem4w is not None and spy4w is not None:
        em_vs_us = eem4w - spy4w
        if em_vs_us > 3:
            scores['REFLAZIONE'] += 15
        elif em_vs_us < -5:
            scores['TIGHTENING'] += 8
            scores['RISK_OFF']   += 8

    # TIPS forte → inflazione attesa in salita
    tip4w = get_ret('TIP')
    if tip4w is not None and tlt4w is not None:
        tips_vs_tlt = tip4w - tlt4w
        if tips_vs_tlt > 2:
            scores['REFLAZIONE']  += 12
            scores['STAGFLAZIONE']+= 8
        elif tips_vs_tlt < -2:
            scores['DISINFLAZIONE']+= 12
            scores['RECESSIONE']  += 8

    # Normalizza a 100%
    total = sum(scores.values())
    if total <= 0:
        return {c: round(100/len(CODES)) for c in CODES}
    return {c: round(scores[c]/total*100) for c in CODES}

# ── STORICO SETTIMANALE ───────────────────────────────────────────
def build_weekly_history(etf_data: dict, prev_data: dict) -> list:
    """
    Mantiene storico settimanale dal 2010.
    Per le settimane passate usa i dati storici ETF.
    Per la settimana corrente usa i dati freschi.
    """
    if prev_data:
        prev_weights = prev_data.get('scenario_weights', [])
        if len(prev_weights) > 10:
            # Aggiorna solo l'ultima settimana
            today     = date.today()
            curr_week = (today - timedelta(days=today.weekday())).isoformat()
            current_scenarios = classify_from_etf(etf_data, curr_week)
            # Trova e aggiorna settimana corrente
            updated = False
            for i, w in enumerate(prev_weights):
                if w['date'] >= curr_week:
                    prev_weights[i]['scenarios'] = current_scenarios
                    prev_weights[i]['indicators'] = build_indicators(etf_data)
                    updated = True
                    break
            if not updated:
                prev_weights.append({
                    'date':       curr_week,
                    'scenarios':  current_scenarios,
                    'indicators': build_indicators(etf_data),
                })
            return prev_weights[-800:]  # max ~15 anni

    # Prima run: genera storico completo usando closes storiche
    print("  Generazione storico completo (prima run)...")
    tickers_with_history = [t for t in ETF_PROXY if t in etf_data and etf_data[t].get('closes') is not None]
    weekly = []
    start  = date(2010, 1, 4)
    today  = date.today()
    curr   = start
    while curr <= today:
        week_str = curr.isoformat()
        # Calcola returns storici per questa settimana
        hist_etf = {}
        for t in tickers_with_history:
            closes = etf_data[t]['closes']
            try:
                idx_now  = closes.index.searchsorted(str(curr))
                idx_4w   = closes.index.searchsorted(str(curr - timedelta(days=30)))
                idx_12w  = closes.index.searchsorted(str(curr - timedelta(days=90)))
                if idx_now >= len(closes): idx_now = len(closes)-1
                if idx_4w  >= len(closes): idx_4w  = 0
                if idx_12w >= len(closes): idx_12w = 0
                p_now = float(closes.iloc[idx_now])
                p_4w  = float(closes.iloc[idx_4w])
                p_12w = float(closes.iloc[idx_12w])
                hist_etf[t] = {
                    'ret_4w':  round((p_now-p_4w)/p_4w*100, 2)  if p_4w  else None,
                    'ret_12w': round((p_now-p_12w)/p_12w*100, 2) if p_12w else None,
                }
            except Exception:
                pass
        scenarios = classify_from_etf(hist_etf, week_str)
        weekly.append({'date': week_str, 'scenarios': scenarios, 'indicators': {}})
        curr += timedelta(days=7)
    return weekly

def build_indicators(etf_data: dict) -> dict:
    """Costruisce indicatori macro proxy dagli ETF."""
    def ret(t, p='ret_4w'):
        return etf_data.get(t,{}).get(p)
    return {
        'yield10y':      None,
        'yield2y':       None,
        'yield_curve':   None,
        'cpi_yoy':       None,
        'cpi_mom':       None,
        'fed_funds':     None,
        'real_yield':    None,
        'hy_spread':     None,
        'unemployment':  None,
        'unemp_delta':   None,
        'gdp':           None,
        'm2_yoy':        None,
        'fed_delta12m':  None,
        'btp_spread':    None,
        # ETF proxy (sempre disponibili)
        'spy_ret_4w':    ret('SPY'),
        'tlt_ret_4w':    ret('TLT'),
        'gld_ret_4w':    ret('GLD'),
        'hyg_ret_4w':    ret('HYG'),
        'vxx_ret_4w':    ret('VXX'),
        'uup_ret_4w':    ret('UUP'),
        'eem_ret_4w':    ret('EEM'),
        'uso_ret_4w':    ret('USO'),
        'tip_ret_4w':    ret('TIP'),
    }

def compute_forecast(scenario_weights, horizons=(1,2,4,8)):
    if len(scenario_weights) < 20: return {}
    current_dom = max(scenario_weights[-1]['scenarios'], key=scenario_weights[-1]['scenarios'].get)
    n = len(scenario_weights); forecast = {}
    for h in horizons:
        counts={c:0.0 for c in CODES}; total_w=0.0
        for i in range(n-h):
            dom_i = max(scenario_weights[i]['scenarios'], key=scenario_weights[i]['scenarios'].get)
            if dom_i != current_dom: continue
            dom_f = max(scenario_weights[i+h]['scenarios'], key=scenario_weights[i+h]['scenarios'].get)
            w = 0.99**(n-i-h); counts[dom_f]+=w; total_w+=w
        forecast[f'{h}w'] = {c:round(counts[c]/total_w*100) for c in CODES} if total_w>0 else {c:0 for c in CODES}
    return forecast

def get_active_shocks(date_str):
    labels={12:'FINANCIAL',14:'SOVEREIGN',11:'PANDEMIC',13:'WAR',9:'GEO_SHOCK'}
    today=date.today().isoformat(); active=[]
    for sp in SHOCK_PERIODS:
        end=sp['end'] or today
        if sp['start']<=date_str<=end:
            label=labels.get(sp['id'],'')
            if label and label not in active: active.append(label)
    return active

def generate_oracle_comment(current, forecast, etf_data, alerts, active_shocks):
    if not GROQ_KEY: return None
    dom_code = max(current['scenarios'], key=current['scenarios'].get)
    dom_pct  = current['scenarios'][dom_code]
    dom_name = next(s['name'] for s in SCENARIOS if s['code']==dom_code)
    top3     = sorted(current['scenarios'].items(), key=lambda x: x[1], reverse=True)[:3]
    top3_str = ' | '.join(f"{c}: {p}%" for c,p in top3)
    f4w      = forecast.get('4w',{})
    fcast_dom= sorted(f4w.items(), key=lambda x: x[1], reverse=True)[0] if f4w else ('?','?')
    fcast_str= ' | '.join(f"{c}: {p}%" for c,p in sorted(f4w.items(), key=lambda x: x[1], reverse=True)[:3])
    ind      = current.get('indicators',{})
    # ETF top/bot
    etf_rets = [(t,d.get('ret_4w',0) or 0) for t,d in etf_data.items() if d.get('ret_4w') is not None]
    etf_rets.sort(key=lambda x: x[1], reverse=True)
    top_str  = ' | '.join(f"{t}: {r:+.1f}%" for t,r in etf_rets[:4])
    bot_str  = ' | '.join(f"{t}: {r:+.1f}%" for t,r in etf_rets[-4:])
    alert_ctx= ' | '.join(a['msg'] for a in alerts) if alerts else 'nessuno'
    shock_ctx= ', '.join(active_shocks) if active_shocks else 'nessuno'
    prompt = (
        "Sei RAPTOR, un hedge fund manager con 30 anni di esperienza. "
        "Parli in prima persona, tono assertivo e operativo. "
        "Scrivi ESATTAMENTE 4 frasi: "
        "1) Stato mercato reale. 2) COMPRA 2-3 asset (ticker). "
        "3) VENDI/EVITA 2-3 asset (ticker). 4) Risk factor della settimana.\n\n"
        f"Regime: {dom_name} ({dom_pct}%) | Mix: {top3_str}\n"
        f"ETF top 4W: {top_str}\nETF peggiori 4W: {bot_str}\n"
        f"Shock: {shock_ctx} | Alert: {alert_ctx}\n"
        f"Forecast 4W: {fcast_str} → {fcast_dom[0]} {fcast_dom[1]}%\n"
        "ORACOLO:"
    )
    try:
        r = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization':f'Bearer {GROQ_KEY}','Content-Type':'application/json'},
            json={'model':'llama-3.3-70b-versatile','messages':[
                {'role':'system','content':'Sei un hedge fund manager. Rispondi con ESATTAMENTE 4 frasi in italiano. Solo ordini operativi.'},
                {'role':'user','content':prompt}
            ],'max_tokens':350,'temperature':0.85},
            timeout=30
        )
        resp = r.json()
        if 'choices' not in resp: return None
        return resp['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"  err Groq: {e}"); return None

def check_alerts(current, prev_data, etf_data):
    alerts=[]; curr_dom=max(current['scenarios'],key=current['scenarios'].get)
    if prev_data:
        prev_sw=prev_data.get('scenario_weights',[])
        if prev_sw:
            prev_dom=max(prev_sw[-1]['scenarios'],key=prev_sw[-1]['scenarios'].get)
            if prev_dom!=curr_dom:
                alerts.append({'type':'scenario_change','severity':'HIGH','msg':f'Cambio regime: {prev_dom} → {curr_dom}'})
    vxx = etf_data.get('VXX',{}).get('ret_4w')
    if vxx and vxx>20:
        alerts.append({'type':'vix_spike','severity':'HIGH','msg':f'VXX +{vxx:.1f}% 4w — volatilità estrema'})
    hyg = etf_data.get('HYG',{}).get('ret_4w')
    lqd = etf_data.get('LQD',{}).get('ret_4w')
    if hyg is not None and lqd is not None and hyg-lqd < -5:
        alerts.append({'type':'credit_stress','severity':'HIGH','msg':f'HYG-LQD: {hyg-lqd:.1f}% — stress creditizio'})
    return alerts

def compute_etf_divergence(dominant_code, etf_data):
    bias = {
        'GOLDILOCKS':{'SPY':1,'QQQ':1,'HYG':1,'TLT':-1,'GLD':-1,'VXX':-1},
        'REFLAZIONE':{'GLD':1,'USO':1,'TIP':1,'TLT':-1,'UUP':-1,'EEM':1},
        'RISK_OFF':  {'TLT':1,'GLD':1,'VXX':1,'SPY':-1,'HYG':-1,'EEM':-1},
        'TIGHTENING':{'UUP':1,'TIP':1,'TLT':-1,'HYG':-1,'SPY':-1,'GLD':-1},
        'EUFORIA':   {'QQQ':1,'SPY':1,'HYG':1,'VXX':-1,'TLT':-1,'SOXX':1},
    }.get(dominant_code,{})
    divs=[]
    for ticker,expected_dir in bias.items():
        ret=etf_data.get(ticker,{}).get('ret_4w')
        if ret is None: continue
        actual_dir=1 if ret>0.5 else (-1 if ret<-0.5 else 0)
        if actual_dir!=0 and actual_dir==-expected_dir:
            divs.append({'ticker':ticker,'name':next((e['name'] for e in ETF_LIST if e['t']==ticker),ticker),
                         'expected':'rialzo' if expected_dir>0 else 'ribasso','actual_ret':ret,'severity':abs(ret)})
    return sorted(divs,key=lambda x:x['severity'],reverse=True)[:5]

def main():
    print("="*60)
    print(f"RAPTOR MACRO CLASSIFIER v3.0 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)

    prev_data = None
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f: prev_data = json.load(f)
            print(f"  Prev: {prev_data.get('generated','?')}")
        except: pass

    # 1. Scarica ETF
    print("\n[1/3] Download ETF...")
    all_tickers = list(set([e['t'] for e in ETF_LIST] + list(ETF_PROXY.keys())))
    etf_raw = fetch_etf_data(all_tickers)

    # Rimuovi closes dal dict ETF (troppo grande per JSON)
    etf_data_clean = {}
    etf_data_full  = {}
    for t, d in etf_raw.items():
        etf_data_full[t]  = d
        etf_data_clean[t] = {k:v for k,v in d.items() if k!='closes'}

    # 2. Classifica regime
    print("\n[2/3] Classificazione regime...")
    today     = date.today()
    curr_week = (today - timedelta(days=today.weekday())).isoformat()
    current_scenarios = classify_from_etf(etf_data_full, curr_week)
    curr_dom  = max(current_scenarios, key=current_scenarios.get)
    curr_pct  = current_scenarios[curr_dom]
    print(f"  Regime: {curr_dom} ({curr_pct}%)")
    top3 = sorted(current_scenarios.items(), key=lambda x: x[1], reverse=True)[:3]
    print(f"  Top3: {' | '.join(f'{c}: {p}%' for c,p in top3)}")

    # 3. Storico + output
    print("\n[3/3] Costruzione storico + output...")
    indicators = build_indicators(etf_data_full)
    scenario_weights = build_weekly_history(etf_data_full, prev_data)

    # Assicura che l'ultima entry sia quella corrente
    current_entry = {'date':curr_week,'scenarios':current_scenarios,'indicators':indicators}
    if scenario_weights and scenario_weights[-1]['date'] >= curr_week:
        scenario_weights[-1] = current_entry
    else:
        scenario_weights.append(current_entry)

    forecast      = compute_forecast(scenario_weights)
    active_shocks = get_active_shocks(curr_week)
    alerts        = check_alerts(current_entry, prev_data, etf_data_full)
    divergences   = compute_etf_divergence(curr_dom, etf_data_full)
    oracle        = generate_oracle_comment(current_entry, forecast, etf_data_full, alerts, active_shocks)

    output = {
        'generated':       datetime.now(timezone.utc).isoformat(),
        'version':         '3.0',
        'current_week':    curr_week,
        'macro_indicators':indicators,
        'scenario_weights':scenario_weights,
        'scenarios_meta':  SCENARIOS,
        'forecast':        forecast,
        'etf_data':        etf_data_clean,
        'etf_list':        ETF_LIST,
        'etf_divergences': divergences,
        'active_shocks':   active_shocks,
        'alerts':          alerts,
        'oracle_comment':  oracle,
    }

    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE,'w') as f:
        json.dump(output, f, separators=(',',':'))
    size = os.path.getsize(DATA_FILE)/1024
    print(f"\n✅ Salvato {DATA_FILE} ({size:.0f} KB)")
    print(f"   Regime: {curr_dom} ({curr_pct}%) | Storico: {len(scenario_weights)} settimane")

if __name__=='__main__':
    main()
