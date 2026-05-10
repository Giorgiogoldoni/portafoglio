#!/usr/bin/env python3
"""
RAPTOR MACRO CLASSIFIER v3.1
══════════════════════════════
Classificatore a 3 livelli:
  L1 — Shock detector (priorità assoluta)
  L2 — Cross-asset matrix (5 segnali binari)
  L3 — Regime persistence (inerzia 30%)

Nessuna API key. Solo Yahoo Finance.
Output: data/latest.json
"""

import os, json, time, requests
import yfinance as yf
from datetime import datetime, timedelta, date, timezone
from pathlib import Path

GROQ_KEY     = os.environ.get('GROQ_API_KEY', '')
GMAIL_USER   = os.environ.get('GMAIL_USER', '')
GMAIL_PASS   = os.environ.get('GMAIL_APP_PASSWORD', '')
NOTIFY_EMAIL = os.environ.get('NOTIFY_EMAIL', '')
PAGES_URL    = os.environ.get('PAGES_URL', 'https://giorgiogoldoni.github.io/portafoglio')
DATA_FILE    = os.path.join(os.path.dirname(__file__), '..', 'data', 'latest.json')

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
    {'id':12,'start':'2007-08-01','end':'2008-09-14','intensity':0.5,'code':'FINANCIAL'},
    {'id':12,'start':'2008-09-15','end':'2009-03-09','intensity':1.0,'code':'FINANCIAL'},
    {'id':14,'start':'2010-05-01','end':'2012-07-26','intensity':0.8,'code':'SOVEREIGN'},
    {'id':11,'start':'2020-02-20','end':'2020-03-23','intensity':1.0,'code':'PANDEMIC'},
    {'id':11,'start':'2020-03-24','end':'2020-12-31','intensity':0.6,'code':'PANDEMIC'},
    {'id':13,'start':'2022-02-24','end':'2022-03-08','intensity':1.0,'code':'WAR'},
    {'id':13,'start':'2022-03-09','end':'2022-12-31','intensity':0.5,'code':'WAR'},
    {'id':12,'start':'2023-03-08','end':'2023-03-31','intensity':0.4,'code':'FINANCIAL'},
    {'id':9, 'start':'2026-04-02','end':None,        'intensity':0.7,'code':'GEO_SHOCK'},
]

ETF_LIST = [
    {'t':'SPY', 'name':'S&P 500',        'area':'USA'},
    {'t':'QQQ', 'name':'Nasdaq 100',      'area':'USA'},
    {'t':'IWM', 'name':'Russell 2000',    'area':'USA'},
    {'t':'VGK', 'name':'Europa Broad',    'area':'Europa'},
    {'t':'EWG', 'name':'Germania',        'area':'Europa'},
    {'t':'EWI', 'name':'Italia',          'area':'Europa'},
    {'t':'EWP', 'name':'Spagna',          'area':'Europa'},
    {'t':'EWQ', 'name':'Francia',         'area':'Europa'},
    {'t':'EWU', 'name':'UK',              'area':'Europa'},
    {'t':'HEZU','name':'Eurozona Hedged', 'area':'Europa'},
    {'t':'EWL', 'name':'Svizzera',        'area':'Europa'},
    {'t':'EWN', 'name':'Olanda',          'area':'Europa'},
    {'t':'EWD', 'name':'Svezia',          'area':'Europa'},
    {'t':'EWJ', 'name':'Giappone',        'area':'Asia'},
    {'t':'DXJ', 'name':'Giappone Hed',   'area':'Asia'},
    {'t':'EWH', 'name':'Hong Kong',       'area':'Asia'},
    {'t':'FXI', 'name':'Cina',            'area':'Asia'},
    {'t':'INDA','name':'India',           'area':'Asia'},
    {'t':'EWY', 'name':'Korea',           'area':'Asia'},
    {'t':'EWT', 'name':'Taiwan',          'area':'Asia'},
    {'t':'EWA', 'name':'Australia',       'area':'Asia'},
    {'t':'EEM', 'name':'EM Broad',        'area':'EM'},
    {'t':'VWO', 'name':'EM Vanguard',     'area':'EM'},
    {'t':'EWZ', 'name':'Brasile',         'area':'EM'},
    {'t':'ILF', 'name':'Latin America',   'area':'EM'},
    {'t':'INDA','name':'India',           'area':'EM'},
    {'t':'TLT', 'name':'Treasury 20Y+',   'area':'Bond'},
    {'t':'IEF', 'name':'Treasury 7-10Y',  'area':'Bond'},
    {'t':'HYG', 'name':'High Yield',      'area':'Bond'},
    {'t':'LQD', 'name':'Invest Grade',    'area':'Bond'},
    {'t':'TIP', 'name':'TIPS',            'area':'Bond'},
    {'t':'EMB', 'name':'EM Bond',         'area':'Bond'},
    {'t':'GLD', 'name':'Oro',             'area':'Commodity'},
    {'t':'USO', 'name':'Petrolio',        'area':'Commodity'},
    {'t':'PDBC','name':'Commodity',       'area':'Commodity'},
    {'t':'SLV', 'name':'Argento',         'area':'Commodity'},
    {'t':'COPX','name':'Rame',            'area':'Commodity'},
    {'t':'VXX', 'name':'VIX',            'area':'Vol'},
    {'t':'XLU', 'name':'Utilities',       'area':'Settoriali'},
    {'t':'XLP', 'name':'Staples',         'area':'Settoriali'},
    {'t':'XLE', 'name':'Energy',          'area':'Settoriali'},
    {'t':'XLF', 'name':'Financials',      'area':'Settoriali'},
    {'t':'SOXX','name':'Semicon',         'area':'Settoriali'},
    {'t':'UUP', 'name':'Dollaro USA',     'area':'Valute'},
    {'t':'DBA', 'name':'Agricoltura',     'area':'Commodity'},
]

# ── FETCH ─────────────────────────────────────────────────────────
PROXY_TICKERS = ['SPY','QQQ','IWM','TLT','IEF','HYG','LQD','TIP','GLD','USO','VXX','UUP','EEM','XLE','XLU','XLP','SOXX','EWJ','EWZ','COPX','SLV','EMB']

def fetch_all(tickers: list) -> dict:
    """Scarica 2 anni di dati per tutti i ticker."""
    result = {}
    print(f"  Download {len(tickers)} ticker (2 anni)...")
    try:
        raw = yf.download(
            tickers, period='2y', interval='1wk',
            group_by='ticker', auto_adjust=True, progress=False
        )
    except Exception as e:
        print(f"  err yfinance: {e}")
        return result

    for t in tickers:
        try:
            if len(tickers) == 1:
                closes = raw['Close'].dropna()
            else:
                closes = raw[t]['Close'].dropna()
            if len(closes) < 10:
                continue
            result[t] = closes
        except Exception:
            pass

    print(f"  Scaricati: {len(result)}/{len(tickers)}")
    return result

def fetch_daily(tickers: list) -> dict:
    """Scarica 3 mesi daily per ETF monitor."""
    result = {}
    try:
        raw = yf.download(
            tickers, period='3mo', interval='1d',
            group_by='ticker', auto_adjust=True, progress=False
        )
        today = date.today()
        for t in tickers:
            try:
                closes = (raw[t]['Close'] if len(tickers)>1 else raw['Close']).dropna()
                if len(closes) < 2: continue
                price = float(closes.iloc[-1])
                def ret(d):
                    fd = today - timedelta(days=d)
                    idx = closes.index.searchsorted(str(fd))
                    if idx >= len(closes): return None
                    old = float(closes.iloc[idx])
                    return round((price-old)/old*100, 2) if old else None
                result[t] = {'price':round(price,2),'ret_1w':ret(7),'ret_4w':ret(30),'ret_12w':ret(90)}
            except Exception:
                pass
    except Exception as e:
        print(f"  err daily: {e}")
    return result

# ── L1: SHOCK DETECTOR ────────────────────────────────────────────
def detect_shock(date_str: str, closes: dict) -> tuple:
    """
    Ritorna (shock_code, intensity) se uno shock storico è attivo,
    oppure rileva shock da VXX in tempo reale.
    """
    today = date.today().isoformat()

    # Shock storici
    for sp in SHOCK_PERIODS:
        end = sp['end'] or today
        if sp['start'] <= date_str <= end:
            return sp['code'], sp['intensity']

    # Shock real-time da VXX
    vxx = closes.get('VXX')
    if vxx is not None and len(vxx) >= 5:
        try:
            # Cerca la settimana più vicina a date_str
            idx = vxx.index.searchsorted(date_str)
            if idx >= len(vxx): idx = len(vxx)-1
            if idx < 4: return None, 0

            p_now  = float(vxx.iloc[idx])
            p_4w   = float(vxx.iloc[max(0, idx-4)])
            ret_4w = (p_now - p_4w) / p_4w * 100 if p_4w else 0

            if ret_4w > 50:   return 'PANDEMIC', 0.8   # VXX +50% → crisi estrema
            if ret_4w > 30:   return 'RISK_OFF',  0.7
            if ret_4w > 15:   return 'RISK_OFF',  0.4
        except Exception:
            pass

    return None, 0

# ── L2: CROSS-ASSET MATRIX ────────────────────────────────────────
def get_ret(closes: dict, ticker: str, date_str: str, weeks: int) -> float:
    """Ritorna il rendimento percentuale a N settimane dalla data."""
    series = closes.get(ticker)
    if series is None or len(series) < weeks + 1:
        return None
    try:
        idx = series.index.searchsorted(date_str)
        if idx >= len(series): idx = len(series) - 1
        if idx < weeks: return None
        p_now  = float(series.iloc[idx])
        p_prev = float(series.iloc[idx - weeks])
        return (p_now - p_prev) / p_prev * 100 if p_prev else None
    except Exception:
        return None

def cross_asset_scores(closes: dict, date_str: str) -> dict:
    """
    5 segnali binari cross-asset → scores per scenario.
    Ogni segnale contribuisce con peso diverso.
    """
    scores = {c: 0.0 for c in CODES}

    def r(t, w=4): return get_ret(closes, t, date_str, w)

    spy4  = r('SPY', 4);  spy13 = r('SPY', 13)
    tlt4  = r('TLT', 4);  tlt13 = r('TLT', 13)
    gld4  = r('GLD', 4)
    hyg4  = r('HYG', 4);  lqd4  = r('LQD', 4)
    tip4  = r('TIP', 4)
    uup4  = r('UUP', 4)
    eem4  = r('EEM', 4)
    uso4  = r('USO', 4)
    vxx4  = r('VXX', 4)
    xlu4  = r('XLU', 4);  xlp4  = r('XLP', 4)
    soxx4 = r('SOXX',4)
    ief4  = r('IEF', 4)

    # ── SEGNALE 1: Equity vs Bond (risk-on/off) ───────────────────
    if spy4 is not None and tlt4 is not None:
        eq_vs_bond = spy4 - tlt4
        if eq_vs_bond > 8:
            scores['EUFORIA']     += 30
            scores['GOLDILOCKS']  += 20
        elif eq_vs_bond > 3:
            scores['GOLDILOCKS']  += 25
            scores['REFLAZIONE']  += 10
        elif eq_vs_bond > -3:
            scores['DISINFLAZIONE']+= 20
            scores['ZIRP']        += 10
        elif eq_vs_bond > -8:
            scores['TIGHTENING']  += 20
            scores['RISK_OFF']    += 15
        else:
            scores['RECESSIONE']  += 25
            scores['RISK_OFF']    += 20

    # ── SEGNALE 2: Oro (inflazione/paura) ─────────────────────────
    if gld4 is not None:
        if gld4 > 8:
            scores['GEO_SHOCK']   += 25
            scores['STAGFLAZIONE']+= 20
            scores['RISK_OFF']    += 15
            scores['WAR']         += 15
        elif gld4 > 4:
            scores['REFLAZIONE']  += 20
            scores['STAGFLAZIONE']+= 10
            scores['GEO_SHOCK']   += 10
        elif gld4 > 0:
            scores['REFLAZIONE']  += 10
        elif gld4 < -4:
            scores['GOLDILOCKS']  += 15
            scores['DISINFLAZIONE']+= 10
            scores['TIGHTENING']  += 8

    # ── SEGNALE 3: HY vs IG (credito) ─────────────────────────────
    if hyg4 is not None and lqd4 is not None:
        hy_vs_ig = hyg4 - lqd4
        if hy_vs_ig > 4:
            scores['EUFORIA']     += 25
            scores['GOLDILOCKS']  += 15
        elif hy_vs_ig > 1:
            scores['GOLDILOCKS']  += 20
            scores['REFLAZIONE']  += 10
        elif hy_vs_ig > -2:
            scores['DISINFLAZIONE']+= 15
        elif hy_vs_ig > -5:
            scores['TIGHTENING']  += 20
            scores['STAGFLAZIONE']+= 10
        else:
            scores['RISK_OFF']    += 30
            scores['RECESSIONE']  += 20
            scores['FINANCIAL']   += 15

    # ── SEGNALE 4: TIP vs TLT (inflazione attesa) ─────────────────
    if tip4 is not None and tlt4 is not None:
        tips_vs_tlt = tip4 - tlt4
        if tips_vs_tlt > 3:
            scores['REFLAZIONE']  += 25
            scores['STAGFLAZIONE']+= 15
        elif tips_vs_tlt > 1:
            scores['REFLAZIONE']  += 15
        elif tips_vs_tlt < -3:
            scores['DISINFLAZIONE']+= 25
            scores['RECESSIONE']  += 10
        elif tips_vs_tlt < -1:
            scores['DISINFLAZIONE']+= 15
            scores['TIGHTENING']  += 8

    # ── SEGNALE 5: UUP (Dollar) ───────────────────────────────────
    if uup4 is not None:
        if uup4 > 4:
            scores['TIGHTENING']  += 25
            scores['RISK_OFF']    += 15
            scores['GEO_SHOCK']   += 10
        elif uup4 > 1.5:
            scores['TIGHTENING']  += 15
            scores['DISINFLAZIONE']+= 8
        elif uup4 < -3:
            scores['REFLAZIONE']  += 20
            scores['ZIRP']        += 15
            scores['EUFORIA']     += 10
        elif uup4 < -1:
            scores['REFLAZIONE']  += 10
            scores['GOLDILOCKS']  += 8

    # ── SEGNALE 6: EM vs USA ──────────────────────────────────────
    if eem4 is not None and spy4 is not None:
        em_vs_us = eem4 - spy4
        if em_vs_us > 4:
            scores['REFLAZIONE']  += 20
            scores['ZIRP']        += 10
        elif em_vs_us < -4:
            scores['TIGHTENING']  += 15
            scores['RISK_OFF']    += 10

    # ── SEGNALE 7: Energia ────────────────────────────────────────
    if uso4 is not None:
        if uso4 > 10:
            scores['REFLAZIONE']  += 15
            scores['STAGFLAZIONE']+= 10
            scores['WAR']         += 15
            scores['GEO_SHOCK']   += 10
        elif uso4 < -10:
            scores['RECESSIONE']  += 15
            scores['DISINFLAZIONE']+= 10

    # ── SEGNALE 8: Difensivi vs Ciclici (fine ciclo) ─────────────
    if xlu4 is not None and spy4 is not None:
        def_vs_cyc = xlu4 - spy4
        if def_vs_cyc > 3:
            scores['RECESSIONE']  += 15
            scores['TIGHTENING']  += 10
            scores['RISK_OFF']    += 10
        elif def_vs_cyc < -5:
            scores['EUFORIA']     += 15
            scores['GOLDILOCKS']  += 10

    # ── SEGNALE 9: Tech momentum (euforia/goldilocks) ─────────────
    if soxx4 is not None and spy4 is not None:
        tech_vs_broad = soxx4 - spy4
        if tech_vs_broad > 5:
            scores['EUFORIA']     += 20
            scores['GOLDILOCKS']  += 10
        elif tech_vs_broad < -5:
            scores['TIGHTENING']  += 10
            scores['RISK_OFF']    += 8

    # ── SEGNALE 10: Trend confermato (SPY 13W) ────────────────────
    if spy13 is not None:
        if spy13 > 10:
            scores['EUFORIA']     += 10
            scores['GOLDILOCKS']  += 8
        elif spy13 < -15:
            scores['RECESSIONE']  += 15
            scores['RISK_OFF']    += 10

    # ── SEGNALE 11: ZIRP detector (bond + equity + hy tutti su) ──
    if (tlt4 is not None and tlt4 > 2 and
        spy4 is not None and spy4 > 2 and
        hyg4 is not None and hyg4 > 2):
        scores['ZIRP']            += 20

    return scores

# ── L3: REGIME PERSISTENCE ────────────────────────────────────────
def apply_persistence(new_scores: dict, prev_regime: str, weight: float = 0.25) -> dict:
    """Aggiunge inerzia al regime precedente (25% peso)."""
    if not prev_regime or prev_regime not in CODES:
        return new_scores
    result = dict(new_scores)
    total  = sum(result.values()) or 1
    bonus  = total * weight
    result[prev_regime] = result.get(prev_regime, 0) + bonus
    return result

# ── CLASSIFICATORE PRINCIPALE ─────────────────────────────────────
def classify(closes: dict, date_str: str, prev_regime: str = '') -> dict:
    """
    3 livelli:
    L1 → Shock detector (override se shock forte)
    L2 → Cross-asset matrix
    L3 → Regime persistence
    """
    # L1: Shock
    shock_code, shock_intensity = detect_shock(date_str, closes)

    if shock_code and shock_intensity >= 0.8:
        # Shock forte → override quasi totale
        scores = {c: 2.0 for c in CODES}
        scores[shock_code] = 100.0 * shock_intensity
        # Mantieni un po' di variazione
        cross = cross_asset_scores(closes, date_str)
        for c in CODES:
            scores[c] += cross.get(c, 0) * 0.2
    elif shock_code and shock_intensity > 0:
        # Shock moderato → contribuisce ma non override
        scores = cross_asset_scores(closes, date_str)
        scores[shock_code] = scores.get(shock_code, 0) + shock_intensity * 50
    else:
        # Nessuno shock → solo cross-asset
        scores = cross_asset_scores(closes, date_str)

    # L3: Persistence
    scores = apply_persistence(scores, prev_regime, 0.25)

    # Normalizza a 100%
    total = sum(scores.values())
    if total <= 0:
        return {c: round(100/len(CODES)) for c in CODES}

    normalized = {c: max(0, round(scores[c]/total*100)) for c in CODES}

    # Fix rounding
    diff = 100 - sum(normalized.values())
    if diff != 0:
        top = max(normalized, key=normalized.get)
        normalized[top] += diff

    return normalized

# ── STORICO SETTIMANALE ───────────────────────────────────────────
def build_history(closes: dict, prev_data: dict) -> list:
    """
    Costruisce/aggiorna lo storico settimanale dal 2015.
    Ricalcola solo le ultime 8 settimane se lo storico esiste.
    """
    today     = date.today()
    curr_week = (today - timedelta(days=today.weekday())).isoformat()

    # Se storico esistente e recente → aggiorna solo ultime 8 settimane
    if prev_data:
        prev_sw = prev_data.get('scenario_weights', [])
        if len(prev_sw) > 50:
            print(f"  Storico esistente: {len(prev_sw)} settimane — aggiorno ultime 8")
            # Trova punto di partenza (8 settimane fa)
            cutoff = (today - timedelta(weeks=8)).isoformat()
            stable = [w for w in prev_sw if w['date'] < cutoff]
            to_update = [w for w in prev_sw if w['date'] >= cutoff]

            prev_regime = stable[-1]['scenarios'] if stable else {}
            prev_dom    = max(prev_regime, key=prev_regime.get) if prev_regime else ''

            updated = []
            for w in to_update:
                sc = classify(closes, w['date'], prev_dom)
                prev_dom = max(sc, key=sc.get)
                updated.append({'date':w['date'],'scenarios':sc,'indicators':{}})

            # Aggiunge settimana corrente se non presente
            if not updated or updated[-1]['date'] < curr_week:
                sc = classify(closes, curr_week, prev_dom)
                updated.append({'date':curr_week,'scenarios':sc,'indicators':{}})

            return (stable + updated)[-800:]

    # Prima run → storico completo dal 2015
    print("  Prima run — generazione storico dal 2015...")
    start   = date(2015, 1, 5)
    history = []
    prev_dom = ''

    curr = start
    while curr <= today:
        week_str = curr.isoformat()
        sc = classify(closes, week_str, prev_dom)
        prev_dom = max(sc, key=sc.get)
        history.append({'date':week_str,'scenarios':sc,'indicators':{}})
        curr += timedelta(weeks=1)

    print(f"  Storico generato: {len(history)} settimane")
    return history

def build_indicators(daily: dict) -> dict:
    """Indicatori proxy dagli ETF."""
    def r(t): return (daily.get(t) or {}).get('ret_4w')
    return {
        'yield10y':None,'yield2y':None,'yield_curve':None,
        'cpi_yoy':None,'cpi_mom':None,'fed_funds':None,
        'real_yield':None,'hy_spread':None,'unemployment':None,
        'unemp_delta':None,'gdp':None,'m2_yoy':None,
        'fed_delta12m':None,'btp_spread':None,
        'spy_ret_4w':r('SPY'),'tlt_ret_4w':r('TLT'),
        'gld_ret_4w':r('GLD'),'hyg_ret_4w':r('HYG'),
        'vxx_ret_4w':r('VXX'),'uup_ret_4w':r('UUP'),
        'eem_ret_4w':r('EEM'),'uso_ret_4w':r('USO'),
        'tip_ret_4w':r('TIP'),'soxx_ret_4w':r('SOXX'),
    }

def compute_forecast(scenario_weights, horizons=(1,2,4,8)):
    if len(scenario_weights) < 20: return {}
    current_dom = max(scenario_weights[-1]['scenarios'], key=scenario_weights[-1]['scenarios'].get)
    n = len(scenario_weights); forecast = {}
    for h in horizons:
        counts={c:0.0 for c in CODES}; total_w=0.0
        for i in range(n-h):
            dom_i=max(scenario_weights[i]['scenarios'],key=scenario_weights[i]['scenarios'].get)
            if dom_i!=current_dom: continue
            dom_f=max(scenario_weights[i+h]['scenarios'],key=scenario_weights[i+h]['scenarios'].get)
            w=0.99**(n-i-h); counts[dom_f]+=w; total_w+=w
        forecast[f'{h}w']={c:round(counts[c]/total_w*100) for c in CODES} if total_w>0 else {c:0 for c in CODES}
    return forecast

def get_active_shocks(date_str):
    today=date.today().isoformat(); active=[]
    for sp in SHOCK_PERIODS:
        end=sp['end'] or today
        if sp['start']<=date_str<=end:
            label=sp['code']
            if label and label not in active: active.append(label)
    return active

def check_alerts(current, prev_data, daily):
    alerts=[]; curr_dom=max(current['scenarios'],key=current['scenarios'].get)
    if prev_data:
        prev_sw=prev_data.get('scenario_weights',[])
        if prev_sw:
            prev_dom=max(prev_sw[-1]['scenarios'],key=prev_sw[-1]['scenarios'].get)
            if prev_dom!=curr_dom:
                alerts.append({'type':'scenario_change','severity':'HIGH','msg':f'Cambio regime: {prev_dom} → {curr_dom}'})
    vxx=(daily.get('VXX') or {}).get('ret_4w')
    if vxx and vxx>20:
        alerts.append({'type':'vix_spike','severity':'HIGH','msg':f'VXX +{vxx:.1f}% 4w'})
    hyg=(daily.get('HYG') or {}).get('ret_4w')
    lqd=(daily.get('LQD') or {}).get('ret_4w')
    if hyg is not None and lqd is not None and hyg-lqd<-5:
        alerts.append({'type':'credit_stress','severity':'HIGH','msg':f'HYG-LQD: {hyg-lqd:.1f}% — stress creditizio'})
    return alerts

def compute_etf_divergence(dominant_code, daily):
    bias={
        'GOLDILOCKS':{'SPY':1,'QQQ':1,'HYG':1,'TLT':-1,'GLD':-1,'VXX':-1},
        'REFLAZIONE':{'GLD':1,'USO':1,'TIP':1,'TLT':-1,'UUP':-1,'EEM':1},
        'RISK_OFF':  {'TLT':1,'GLD':1,'VXX':1,'SPY':-1,'HYG':-1,'EEM':-1},
        'TIGHTENING':{'UUP':1,'TIP':1,'TLT':-1,'HYG':-1,'SPY':-1},
        'EUFORIA':   {'QQQ':1,'SPY':1,'HYG':1,'VXX':-1,'TLT':-1,'SOXX':1},
    }.get(dominant_code,{})
    divs=[]
    for ticker,expected_dir in bias.items():
        ret=(daily.get(ticker) or {}).get('ret_4w')
        if ret is None: continue
        actual_dir=1 if ret>0.5 else (-1 if ret<-0.5 else 0)
        if actual_dir!=0 and actual_dir==-expected_dir:
            name=next((e['name'] for e in ETF_LIST if e['t']==ticker),ticker)
            divs.append({'ticker':ticker,'name':name,'expected':'rialzo' if expected_dir>0 else 'ribasso','actual_ret':ret,'severity':abs(ret)})
    return sorted(divs,key=lambda x:x['severity'],reverse=True)[:5]

def generate_oracle(current, forecast, daily, alerts, active_shocks):
    if not GROQ_KEY: return None
    dom_code=max(current['scenarios'],key=current['scenarios'].get)
    dom_pct =current['scenarios'][dom_code]
    dom_name=next(s['name'] for s in SCENARIOS if s['code']==dom_code)
    top3=sorted(current['scenarios'].items(),key=lambda x:x[1],reverse=True)[:3]
    top3_str=' | '.join(f"{c}: {p}%" for c,p in top3)
    f4w=forecast.get('4w',{})
    fcast_dom=sorted(f4w.items(),key=lambda x:x[1],reverse=True)[0] if f4w else ('?','?')
    fcast_str=' | '.join(f"{c}: {p}%" for c,p in sorted(f4w.items(),key=lambda x:x[1],reverse=True)[:3])
    etf_rets=[(t,(d.get('ret_4w') or 0)) for t,d in daily.items() if d.get('ret_4w') is not None]
    etf_rets.sort(key=lambda x:x[1],reverse=True)
    top_str=' | '.join(f"{t}: {r:+.1f}%" for t,r in etf_rets[:4])
    bot_str=' | '.join(f"{t}: {r:+.1f}%" for t,r in etf_rets[-4:])
    alert_ctx=' | '.join(a['msg'] for a in alerts) if alerts else 'nessuno'
    shock_ctx=', '.join(active_shocks) if active_shocks else 'nessuno'
    prompt=(
        f"Sei RAPTOR, hedge fund manager. 4 frasi assertive in italiano.\n"
        f"Regime: {dom_name} ({dom_pct}%) | Mix: {top3_str}\n"
        f"Top ETF 4W: {top_str}\nPeggiori 4W: {bot_str}\n"
        f"Shock: {shock_ctx} | Alert: {alert_ctx}\n"
        f"Forecast 4W: {fcast_str} → {fcast_dom[0]} {fcast_dom[1]}%\n"
        "Frase1: stato mercato. Frase2: COMPRA 2-3 ticker. Frase3: VENDI 2-3 ticker. Frase4: risk factor.\nORACOLO:"
    )
    try:
        r=requests.post('https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization':f'Bearer {GROQ_KEY}','Content-Type':'application/json'},
            json={'model':'llama-3.3-70b-versatile','messages':[
                {'role':'system','content':'Hedge fund manager. Esattamente 4 frasi in italiano. Solo ordini operativi.'},
                {'role':'user','content':prompt}],'max_tokens':300,'temperature':0.8},timeout=25)
        resp=r.json()
        if 'choices' not in resp: return None
        return resp['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"  err Groq: {e}"); return None

def main():
    print("="*60)
    print(f"RAPTOR MACRO CLASSIFIER v3.1 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)

    prev_data = None
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f: prev_data=json.load(f)
            sw=prev_data.get('scenario_weights',[])
            if sw:
                dom=max(sw[-1]['scenarios'],key=sw[-1]['scenarios'].get)
                print(f"  Prev: {prev_data.get('generated','?')} | Regime: {dom}")
        except: pass

    # 1. Scarica dati weekly (classificatore)
    print("\n[1/3] Download weekly closes (classificatore)...")
    closes = fetch_all(PROXY_TICKERS)

    # 2. Scarica dati daily (ETF monitor)
    print("\n[2/3] Download daily (ETF monitor)...")
    all_tickers = list(set([e['t'] for e in ETF_LIST]))
    daily = fetch_daily(all_tickers)

    # 3. Classifica + storico
    print("\n[3/3] Classificazione + storico...")
    history = build_history(closes, prev_data)

    today     = date.today()
    curr_week = (today - timedelta(days=today.weekday())).isoformat()
    current   = next((w for w in reversed(history) if w['date']<=curr_week), history[-1])
    curr_dom  = max(current['scenarios'], key=current['scenarios'].get)
    curr_pct  = current['scenarios'][curr_dom]

    print(f"  Regime corrente: {curr_dom} ({curr_pct}%)")
    top3=sorted(current['scenarios'].items(),key=lambda x:x[1],reverse=True)[:3]
    print(f"  Top3: {' | '.join(f'{c}: {p}%' for c,p in top3)}")

    # Aggiorna indicatori sull'entry corrente
    indicators = build_indicators(daily)
    for w in history:
        if w['date'] == curr_week:
            w['indicators'] = indicators
            break

    forecast     = compute_forecast(history)
    active_shocks= get_active_shocks(curr_week)
    alerts       = check_alerts(current, prev_data, daily)
    divergences  = compute_etf_divergence(curr_dom, daily)
    oracle       = generate_oracle(current, forecast, daily, alerts, active_shocks)

    # ETF data clean (senza closes)
    etf_clean = {t: {k:v for k,v in d.items() if k!='closes'} for t,d in daily.items()}

    output = {
        'generated':       datetime.now(timezone.utc).isoformat(),
        'version':         '3.1',
        'current_week':    curr_week,
        'macro_indicators':indicators,
        'scenario_weights':history,
        'scenarios_meta':  SCENARIOS,
        'forecast':        forecast,
        'etf_data':        etf_clean,
        'etf_list':        ETF_LIST,
        'etf_divergences': divergences,
        'active_shocks':   active_shocks,
        'alerts':          alerts,
        'oracle_comment':  oracle,
    }

    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE,'w') as f:
        json.dump(output, f, separators=(',',':'))
    kb = os.path.getsize(DATA_FILE)/1024
    print(f"\n✅ Salvato {DATA_FILE} ({kb:.0f} KB)")
    print(f"   {len(history)} settimane | Regime: {curr_dom} ({curr_pct}%)")

if __name__=='__main__':
    main()
