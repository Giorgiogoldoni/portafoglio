#!/usr/bin/env python3
"""
RAPTOR MACRO MOVER — Daily Update Script
Runs via GitHub Actions every day at 12:00 CET.
Fetches FRED macro data + ETF prices, classifies 14 scenarios,
computes Markov-chain forecast, saves JSON, sends email alerts.
"""

import os
import json
import time
import smtplib
import requests
import yfinance as yf
from datetime import datetime, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ── CONFIG ──────────────────────────────────────────────────────
FRED_KEY      = os.environ.get('FRED_API_KEY', '')
GMAIL_USER    = os.environ.get('GMAIL_USER', '')
GMAIL_PASS    = os.environ.get('GMAIL_APP_PASSWORD', '')
NOTIFY_EMAIL  = os.environ.get('NOTIFY_EMAIL', '')
GROQ_KEY      = os.environ.get('GROQ_API_KEY', '')
START_DATE    = '2010-01-01'
DATA_FILE     = os.path.join(os.path.dirname(__file__), '..', 'data', 'latest.json')

# ── SCENARI ─────────────────────────────────────────────────────
SCENARIOS = [
    {'id': 1,  'code': 'GOLDILOCKS',    'name': 'Goldilocks',              'color': '#00D4FF', 'tag': 'CICLICO'},
    {'id': 2,  'code': 'REFLAZIONE',    'name': 'Reflazione',              'color': '#7FFF00', 'tag': 'CICLICO'},
    {'id': 3,  'code': 'STAGFLAZIONE',  'name': 'Stagflazione',            'color': '#FF6B35', 'tag': 'CICLICO'},
    {'id': 4,  'code': 'RISK_OFF',      'name': 'Risk-Off Acuto',          'color': '#FF3355', 'tag': 'CICLICO'},
    {'id': 5,  'code': 'DISINFLAZIONE', 'name': 'Disinflazione Morbida',   'color': '#00BFFF', 'tag': 'CICLICO'},
    {'id': 6,  'code': 'RECESSIONE',    'name': 'Recessione Conclamata',   'color': '#8B0000', 'tag': 'CICLICO'},
    {'id': 7,  'code': 'ZIRP',          'name': 'ZIRP / Rep. Finanziaria', 'color': '#9B59B6', 'tag': 'CICLICO'},
    {'id': 8,  'code': 'TIGHTENING',    'name': 'Tightening Aggressivo',   'color': '#E74C3C', 'tag': 'CICLICO'},
    {'id': 9,  'code': 'GEO_SHOCK',     'name': 'Geopolitical Shock',      'color': '#F39C12', 'tag': 'CICLICO'},
    {'id': 10, 'code': 'EUFORIA',       'name': 'Boom Tech / Euforia',     'color': '#FFD700', 'tag': 'CICLICO'},
    {'id': 11, 'code': 'PANDEMIC',      'name': 'Pandemic Shock',          'color': '#FF69B4', 'tag': 'SHOCK'},
    {'id': 12, 'code': 'FINANCIAL',     'name': 'Financial Crisis',        'color': '#DC143C', 'tag': 'SHOCK'},
    {'id': 13, 'code': 'WAR',           'name': 'War / Geo Rupture',       'color': '#8B4513', 'tag': 'SHOCK'},
    {'id': 14, 'code': 'SOVEREIGN',     'name': 'Sovereign / Debt Crisis', 'color': '#4B0082', 'tag': 'SHOCK'},
]
CODES = [s['code'] for s in SCENARIOS]

SHOCK_PERIODS = [
    {'id': 12, 'start': '2007-08-01', 'end': '2008-09-14', 'intensity': 0.5},
    {'id': 12, 'start': '2008-09-15', 'end': '2009-03-09', 'intensity': 1.0},
    {'id': 14, 'start': '2010-05-01', 'end': '2012-07-26', 'intensity': 0.8},
    {'id': 11, 'start': '2020-02-20', 'end': '2020-03-23', 'intensity': 1.0},
    {'id': 11, 'start': '2020-03-24', 'end': '2020-12-31', 'intensity': 0.6},
    {'id': 13, 'start': '2022-02-24', 'end': '2022-03-08', 'intensity': 1.0},
    {'id': 13, 'start': '2022-03-09', 'end': '2022-12-31', 'intensity': 0.5},
    {'id': 12, 'start': '2023-03-08', 'end': '2023-03-31', 'intensity': 0.4},
    {'id': 9,  'start': '2026-04-02', 'end': None,          'intensity': 0.7},
    {'id': 9,  'start': '2014-02-28', 'end': '2014-04-30', 'intensity': 0.4},
    {'id': 9,  'start': '2016-06-24', 'end': '2016-08-01', 'intensity': 0.4},
    {'id': 9,  'start': '2018-03-22', 'end': '2018-12-31', 'intensity': 0.5},
]

FRED_SERIES = [
    {'id': 'CPIAUCSL',        'name': 'CPI',            'freq': 'm'},
    {'id': 'FEDFUNDS',        'name': 'FedFunds',        'freq': 'm'},
    {'id': 'GS10',            'name': 'Yield10Y',        'freq': 'm'},
    {'id': 'GS2',             'name': 'Yield2Y',         'freq': 'm'},
    {'id': 'T10Y2Y',          'name': 'YieldCurve',      'freq': 'd'},
    {'id': 'DFII10',          'name': 'RealYield10Y',    'freq': 'd'},
    {'id': 'BAMLH0A0HYM2',   'name': 'HYSpread',        'freq': 'd'},
    {'id': 'UNRATE',          'name': 'Unemployment',    'freq': 'm'},
    {'id': 'A191RL1Q225SBEA', 'name': 'GDP',            'freq': 'q'},
    {'id': 'M2SL',            'name': 'M2',             'freq': 'm'},
    {'id': 'IRLTLT01ITM156N', 'name': 'ItalyYield10Y',  'freq': 'm'},
    {'id': 'IRLTLT01DEM156N', 'name': 'GermanyYield10Y','freq': 'm'},
]

PUB_LAG = {
    'CPI': 45, 'FedFunds': 0, 'Yield10Y': 0, 'Yield2Y': 0,
    'YieldCurve': 1, 'RealYield10Y': 1, 'HYSpread': 1,
    'Unemployment': 7, 'GDP': 30, 'M2': 45,
    'ItalyYield10Y': 5, 'GermanyYield10Y': 5
}

ETF_LIST = [
    # USA
    {'t': 'SPY',    'name': 'S&P 500',            'area': 'USA'},
    {'t': 'QQQ',    'name': 'Nasdaq 100',          'area': 'USA'},
    {'t': 'IWM',    'name': 'Russell 2000',        'area': 'USA'},
    # Europa
    {'t': 'VGK',    'name': 'Europa Broad',        'area': 'Europa'},
    {'t': 'EWG',    'name': 'Germania',            'area': 'Europa'},
    {'t': 'EWI',    'name': 'Italia',              'area': 'Europa'},
    {'t': 'EWP',    'name': 'Spagna',              'area': 'Europa'},
    {'t': 'EWQ',    'name': 'Francia',             'area': 'Europa'},
    {'t': 'EWU',    'name': 'UK',                  'area': 'Europa'},
    {'t': 'HEZU',   'name': 'Eurozona Hedged',     'area': 'Europa'},
    {'t': 'EPOL',   'name': 'Polonia',             'area': 'Europa'},
    # Asia
    {'t': 'EWJ',    'name': 'Giappone',            'area': 'Asia'},
    {'t': 'DXJ',    'name': 'Giappone Hedged',     'area': 'Asia'},
    {'t': 'EWH',    'name': 'Hong Kong',           'area': 'Asia'},
    {'t': 'FXI',    'name': 'Cina Large Cap',      'area': 'Asia'},
    {'t': 'MCHI',   'name': 'Cina Broad',          'area': 'Asia'},
    {'t': 'INDA',   'name': 'India',               'area': 'Asia'},
    {'t': 'EWA',    'name': 'Australia',           'area': 'Asia'},
    # Emerging
    {'t': 'EEM',    'name': 'Emerging Markets',    'area': 'EM'},
    {'t': 'VWO',    'name': 'EM Vanguard',         'area': 'EM'},
    {'t': 'EWZ',    'name': 'Brasile',             'area': 'EM'},
    {'t': 'KSA',    'name': 'Arabia Saudita',      'area': 'EM'},
    {'t': 'GUR',    'name': 'Europa Est',          'area': 'EM'},
    # Bond
    {'t': 'TLT',    'name': 'Treasury 20Y+',       'area': 'Bond'},
    {'t': 'IEF',    'name': 'Treasury 7-10Y',      'area': 'Bond'},
    {'t': 'HYG',    'name': 'High Yield USA',      'area': 'Bond'},
    {'t': 'LQD',    'name': 'Investment Grade',    'area': 'Bond'},
    {'t': 'TIP',    'name': 'TIPS',                'area': 'Bond'},
    {'t': 'BNDX',   'name': 'Bond Globali',        'area': 'Bond'},
    {'t': 'EMB',    'name': 'EM Bond',             'area': 'Bond'},
    # Commodity
    {'t': 'GLD',    'name': 'Oro',                 'area': 'Commodity'},
    {'t': 'USO',    'name': 'Petrolio',            'area': 'Commodity'},
    {'t': 'PDBC',   'name': 'Commodity Broad',     'area': 'Commodity'},
    {'t': 'DBA',    'name': 'Agricoltura',         'area': 'Commodity'},
    {'t': 'COPX',   'name': 'Rame',                'area': 'Commodity'},
    {'t': 'SLV',    'name': 'Argento',             'area': 'Commodity'},
    # Volatilità
    {'t': 'VXX',    'name': 'VIX',                 'area': 'Vol'},
    # Settoriali
    {'t': 'XLU',    'name': 'Utilities',           'area': 'Settoriali'},
    {'t': 'XLP',    'name': 'Consumer Staples',    'area': 'Settoriali'},
    {'t': 'XLE',    'name': 'Energy',              'area': 'Settoriali'},
    {'t': 'XLF',    'name': 'Financials',          'area': 'Settoriali'},
    {'t': 'SOXX',   'name': 'Semiconduttori',      'area': 'Settoriali'},
    {'t': 'IBB',    'name': 'Biotech',             'area': 'Settoriali'},
    # Valute
    {'t': 'UUP',    'name': 'Dollaro USA',         'area': 'Valute'},
]

# Comportamento atteso ETF per scenario (per Q7)
ETF_SCENARIO_BIAS = {
    'GOLDILOCKS':   {'SPY':+1,'QQQ':+1,'HYG':+1,'TLT':0,'GLD':0,'VXX':-1,'IWM':+1},
    'REFLAZIONE':   {'GLD':+1,'PDBC':+1,'TIP':+1,'TLT':-1,'USO':+1,'SPY':0,'UUP':-1},
    'STAGFLAZIONE': {'GLD':+1,'PDBC':+1,'TLT':-1,'SPY':-1,'USO':+1,'UUP':+1},
    'RISK_OFF':     {'TLT':+1,'GLD':+1,'VXX':+1,'SPY':-1,'HYG':-1,'EEM':-1,'UUP':+1},
    'DISINFLAZIONE':{'TLT':+1,'SPY':+1,'HYG':+1,'GLD':0,'VXX':-1},
    'RECESSIONE':   {'TLT':+1,'GLD':+1,'SPY':-1,'HYG':-1,'VXX':+1,'UUP':+1},
    'ZIRP':         {'SPY':+1,'QQQ':+1,'TLT':+1,'HYG':+1,'GLD':+1,'UUP':-1},
    'TIGHTENING':   {'TLT':-1,'HYG':-1,'UUP':+1,'GLD':-1,'SPY':-1,'TIP':+1},
    'GEO_SHOCK':    {'GLD':+1,'USO':+1,'TLT':+1,'SPY':-1,'VXX':+1},
    'EUFORIA':      {'QQQ':+1,'SPY':+1,'IWM':+1,'HYG':+1,'VXX':-1,'SOXX':+1},
}

# ── UTILITY ─────────────────────────────────────────────────────
def week_start(d):
    """Ritorna il lunedì della settimana di d."""
    if isinstance(d, str):
        d = datetime.strptime(d, '%Y-%m-%d').date()
    delta = d.weekday()
    return (d - timedelta(days=delta)).isoformat()

def add_days(ds, n):
    return (datetime.strptime(ds, '%Y-%m-%d').date() + timedelta(days=n)).isoformat()

def get_shock_intensity(date_str, shock_id):
    today = date.today().isoformat()
    for sp in SHOCK_PERIODS:
        if sp['id'] != shock_id:
            continue
        end = sp['end'] or today
        if sp['start'] <= date_str <= end:
            return sp['intensity']
    return 0.0

# ── FRED ─────────────────────────────────────────────────────────
def fetch_fred(series_id, freq='m', retries=3):
    freq_map = {'d': None, 'm': 'm', 'q': 'q'}
    params = {
        'series_id': series_id,
        'observation_start': START_DATE,
        'api_key': FRED_KEY,
        'file_type': 'json',
    }
    if freq_map.get(freq):
        params['frequency'] = freq_map[freq]

    url = 'https://api.stlouisfed.org/fred/series/observations'
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            data = r.json()
            if 'error_message' in data:
                print(f"  FRED error {series_id}: {data['error_message']}")
                return []
            obs = [{'date': o['date'], 'value': float(o['value'])}
                   for o in data.get('observations', []) if o['value'] != '.']
            print(f"  ✓ {series_id}: {len(obs)} obs")
            return obs
        except Exception as e:
            print(f"  ✗ {series_id} attempt {attempt+1}: {e}")
            time.sleep(2)
    return []

# ── ETF ─────────────────────────────────────────────────────────
def fetch_etf_data():
    result = {}
    tickers = [e['t'] for e in ETF_LIST]
    print(f"  Scarico {len(tickers)} ETF da Yahoo Finance...")

    try:
        data = yf.download(tickers, period='3mo', interval='1d',
                           group_by='ticker', auto_adjust=True, progress=False)
    except Exception as e:
        print(f"  ✗ yfinance batch error: {e}")
        return result

    today = date.today()
    week_ago = today - timedelta(days=7)
    month_ago = today - timedelta(days=30)
    three_months_ago = today - timedelta(days=90)
    ytd_start = date(today.year, 1, 1)

    for etf in ETF_LIST:
        t = etf['t']
        try:
            if len(tickers) == 1:
                closes = data['Close']
            else:
                closes = data[t]['Close'] if t in data.columns.get_level_values(0) else None

            if closes is None or closes.empty:
                continue

            closes = closes.dropna()
            if len(closes) < 2:
                continue

            price = float(closes.iloc[-1])

            def ret(from_date):
                idx = closes.index.searchsorted(str(from_date))
                if idx >= len(closes):
                    return None
                old = float(closes.iloc[idx])
                return round((price - old) / old * 100, 2) if old else None

            result[t] = {
                'name':  etf['name'],
                'area':  etf['area'],
                'price': round(price, 2),
                'ret_1w':  ret(week_ago),
                'ret_1m':  ret(month_ago),
                'ret_3m':  ret(three_months_ago),
                'ret_ytd': ret(ytd_start),
            }
        except Exception as e:
            print(f"  ✗ {t}: {e}")

    print(f"  ✓ ETF caricati: {len(result)}/{len(tickers)}")
    return result

# ── TRANSFORMS ───────────────────────────────────────────────────
def compute_transforms(raw):
    # CPI YoY e MoM
    if 'CPI' in raw and len(raw['CPI']) > 12:
        cpi = raw['CPI']
        raw['CPI_YOY'] = [{'date': cpi[i]['date'],
                           'value': (cpi[i]['value'] - cpi[i-12]['value']) / cpi[i-12]['value'] * 100}
                          for i in range(12, len(cpi))]
        raw['CPI_MOM'] = [{'date': cpi[i]['date'],
                           'value': (cpi[i]['value'] - cpi[i-1]['value']) / cpi[i-1]['value'] * 100}
                          for i in range(1, len(cpi))]

    # M2 YoY
    if 'M2' in raw and len(raw['M2']) > 12:
        m2 = raw['M2']
        raw['M2_YOY'] = [{'date': m2[i]['date'],
                          'value': (m2[i]['value'] - m2[i-12]['value']) / m2[i-12]['value'] * 100}
                         for i in range(12, len(m2))]

    # BTP-Bund Spread
    if 'ItalyYield10Y' in raw and 'GermanyYield10Y' in raw:
        de_map = {o['date']: o['value'] for o in raw['GermanyYield10Y']}
        raw['BTPSpread'] = [{'date': o['date'], 'value': (o['value'] - de_map[o['date']]) * 100}
                            for o in raw['ItalyYield10Y'] if o['date'] in de_map]

    # Unemployment delta 3M
    if 'Unemployment' in raw and len(raw['Unemployment']) > 3:
        u = raw['Unemployment']
        raw['Unemployment_Delta3M'] = [{'date': u[i]['date'], 'value': u[i]['value'] - u[i-3]['value']}
                                       for i in range(3, len(u))]

    # FedFunds delta 12M
    if 'FedFunds' in raw and len(raw['FedFunds']) > 12:
        ff = raw['FedFunds']
        raw['FedFunds_Delta12M'] = [{'date': ff[i]['date'], 'value': ff[i]['value'] - ff[i-12]['value']}
                                    for i in range(12, len(ff))]

    # HYSpread: FRED series è già in % (es. 3.2 = 320bps) — convertiamo in bps
    if 'HYSpread' in raw:
        raw['HYSpread'] = [{'date': o['date'], 'value': o['value'] * 100} for o in raw['HYSpread']]

def get_value_at(series, date_str, lag_days=0):
    if not series:
        return None
    eff = add_days(date_str, -lag_days) if lag_days else date_str
    best = None
    for obs in series:
        if obs['date'] <= eff:
            best = obs['value']
        else:
            break
    return best

# ── WEEKLY DATASET ────────────────────────────────────────────────
def build_weekly_dataset(raw):
    today = date.today().isoformat()
    current = week_start(START_DATE)
    rows = []

    while current <= today:
        g = lambda name, lag=0: get_value_at(raw.get(name, []), current, lag)

        yc = g('YieldCurve', 1)
        y10 = g('Yield10Y', 5)
        y2  = g('Yield2Y', 5)
        if yc is None and y10 is not None and y2 is not None:
            yc = y10 - y2

        row = {
            'date':        current,
            'cpi_yoy':     g('CPI_YOY', 45),
            'cpi_mom':     g('CPI_MOM', 45),
            'fed_funds':   g('FedFunds', 0),
            'yield10y':    y10,
            'yield2y':     y2,
            'yield_curve': yc,
            'real_yield':  g('RealYield10Y', 1),
            'hy_spread':   g('HYSpread', 1),
            'unemployment':g('Unemployment', 7),
            'unemp_delta': g('Unemployment_Delta3M', 7),
            'gdp':         g('GDP', 30),
            'm2_yoy':      g('M2_YOY', 45),
            'fed_delta12m':g('FedFunds_Delta12M', 0),
            'btp_spread':  g('BTPSpread', 5),
        }
        rows.append(row)
        current = add_days(current, 7)

    return rows

# ── CLASSIFY ─────────────────────────────────────────────────────
def classify_week(row):
    scores = {c: 0.0 for c in CODES}
    d = row['date']

    def v(k): return row.get(k)
    def has(k): x = v(k); return x is not None and x == x  # not NaN

    # Shock con precedenza assoluta
    scores['PANDEMIC']  += get_shock_intensity(d, 11) * 100
    scores['FINANCIAL'] += get_shock_intensity(d, 12) * 100
    scores['WAR']       += get_shock_intensity(d, 13) * 100
    scores['SOVEREIGN'] += get_shock_intensity(d, 14) * 100

    # 1. Goldilocks
    if has('cpi_yoy') and 1.5 <= v('cpi_yoy') <= 3.0: scores['GOLDILOCKS'] += 25
    if has('hy_spread') and v('hy_spread') < 350:       scores['GOLDILOCKS'] += 20
    if has('yield_curve') and v('yield_curve') > 0.25:  scores['GOLDILOCKS'] += 20
    if has('gdp') and v('gdp') >= 2.0:                  scores['GOLDILOCKS'] += 20
    if has('unemployment') and v('unemployment') < 5.0: scores['GOLDILOCKS'] += 15

    # 2. Reflazione
    if has('cpi_yoy') and 0 <= v('cpi_yoy') < 3.5:     scores['REFLAZIONE'] += 20
    if has('cpi_mom') and v('cpi_mom') > 0.15:          scores['REFLAZIONE'] += 25
    if has('yield_curve') and v('yield_curve') > 0.5:   scores['REFLAZIONE'] += 20
    if has('fed_delta12m') and v('fed_delta12m') <= 0:  scores['REFLAZIONE'] += 20
    if has('m2_yoy') and v('m2_yoy') > 5:              scores['REFLAZIONE'] += 15

    # 3. Stagflazione
    if has('cpi_yoy') and v('cpi_yoy') > 4.0:          scores['STAGFLAZIONE'] += 35
    if has('gdp') and v('gdp') < 1.5:                  scores['STAGFLAZIONE'] += 25
    if has('fed_delta12m') and v('fed_delta12m') > 0:  scores['STAGFLAZIONE'] += 20
    if has('cpi_yoy') and v('cpi_yoy') > 6.0:          scores['STAGFLAZIONE'] += 20

    # 4. Risk-Off
    if has('hy_spread') and v('hy_spread') > 500:       scores['RISK_OFF'] += 40
    if has('hy_spread') and v('hy_spread') > 700:       scores['RISK_OFF'] += 30
    if has('yield_curve') and v('yield_curve') < -0.25: scores['RISK_OFF'] += 20
    if has('real_yield') and v('real_yield') > 1.5:     scores['RISK_OFF'] += 10

    # 5. Disinflazione
    if has('cpi_yoy') and 2.0 < v('cpi_yoy') < 4.5:   scores['DISINFLAZIONE'] += 20
    if has('cpi_mom') and v('cpi_mom') < 0.2:          scores['DISINFLAZIONE'] += 25
    if has('unemp_delta') and v('unemp_delta') < 0.3:  scores['DISINFLAZIONE'] += 20
    if has('hy_spread') and v('hy_spread') < 450:       scores['DISINFLAZIONE'] += 15
    if has('fed_delta12m') and v('fed_delta12m') <= 0:  scores['DISINFLAZIONE'] += 20

    # 6. Recessione
    if has('gdp') and v('gdp') < 0:                    scores['RECESSIONE'] += 40
    if has('unemp_delta') and v('unemp_delta') > 1.5:  scores['RECESSIONE'] += 30
    if has('hy_spread') and v('hy_spread') > 600:       scores['RECESSIONE'] += 20
    if has('gdp') and v('gdp') < -2.0:                 scores['RECESSIONE'] += 10

    # 7. ZIRP
    if has('fed_funds') and v('fed_funds') < 0.5:       scores['ZIRP'] += 35
    if has('real_yield') and v('real_yield') < 0:       scores['ZIRP'] += 30
    if has('m2_yoy') and v('m2_yoy') > 8:              scores['ZIRP'] += 20
    if has('yield_curve') and v('yield_curve') > 1.0:   scores['ZIRP'] += 15

    # 8. Tightening
    if has('fed_delta12m') and v('fed_delta12m') > 2.0: scores['TIGHTENING'] += 35
    if has('fed_delta12m') and v('fed_delta12m') > 4.0: scores['TIGHTENING'] += 20
    if has('real_yield') and v('real_yield') > 0.5:     scores['TIGHTENING'] += 25
    if has('yield_curve') and v('yield_curve') < 0:     scores['TIGHTENING'] += 20
    if has('m2_yoy') and v('m2_yoy') < 2:              scores['TIGHTENING'] += 20

    # 9. Geo Shock (ciclico)
    if has('hy_spread') and 400 < v('hy_spread') < 600: scores['GEO_SHOCK'] += 15

    # 10. Euforia
    if has('hy_spread') and v('hy_spread') < 300:       scores['EUFORIA'] += 20
    if has('yield_curve') and 0 < v('yield_curve') < 1.5: scores['EUFORIA'] += 15
    if has('fed_funds') and v('fed_funds') < 3.0:       scores['EUFORIA'] += 15
    if has('cpi_yoy') and v('cpi_yoy') < 3.5:          scores['EUFORIA'] += 15
    if has('unemployment') and v('unemployment') < 4.5: scores['EUFORIA'] += 20
    if has('gdp') and v('gdp') > 2.5:                  scores['EUFORIA'] += 15

    total = sum(scores.values())
    if total == 0:
        return {c: 0 for c in CODES}
    return {c: round(scores[c] / total * 100) for c in CODES}

# ── MARKOV FORECAST (ORACLE) ──────────────────────────────────────
def compute_forecast(scenario_weights, horizons=(1, 2, 4, 8)):
    """
    Markov chain forecast: per ogni orizzonte (settimane),
    calcola la distribuzione di probabilità degli scenari futuri
    partendo dallo scenario dominante attuale.
    Usa decay esponenziale per dare più peso alle transizioni recenti.
    """
    if len(scenario_weights) < 20:
        return {}

    forecast = {}
    current = scenario_weights[-1]['scenarios']
    current_dom = max(current, key=current.get)
    n = len(scenario_weights)

    for h in horizons:
        counts = {c: 0.0 for c in CODES}
        total_weight = 0.0

        for i in range(n - h):
            dom_i = max(scenario_weights[i]['scenarios'], key=scenario_weights[i]['scenarios'].get)
            if dom_i != current_dom:
                continue
            dom_future = max(scenario_weights[i + h]['scenarios'], key=scenario_weights[i + h]['scenarios'].get)
            # Recency weight: più recente = più peso (decay 0.99 per settimana)
            weight = 0.99 ** (n - i - h)
            counts[dom_future] += weight
            total_weight += weight

        if total_weight > 0:
            pcts = {c: round(counts[c] / total_weight * 100) for c in CODES}
        else:
            pcts = {c: 0 for c in CODES}

        forecast[f'{h}w'] = pcts

    return forecast

# ── ETF DIVERGENZA (Q7) ───────────────────────────────────────────
def compute_etf_divergence(dominant_code, etf_data):
    """
    Confronta il comportamento atteso degli ETF nel regime attuale
    con quello effettivo (ret_1w). Identifica le divergenze più grandi.
    """
    bias = ETF_SCENARIO_BIAS.get(dominant_code, {})
    divergences = []

    for ticker, expected_dir in bias.items():
        if ticker not in etf_data:
            continue
        ret = etf_data[ticker].get('ret_1w')
        if ret is None:
            continue
        actual_dir = 1 if ret > 0.5 else (-1 if ret < -0.5 else 0)
        if actual_dir != 0 and actual_dir == -expected_dir:
            divergences.append({
                'ticker': ticker,
                'name': etf_data[ticker]['name'],
                'expected': 'rialzo' if expected_dir > 0 else 'ribasso',
                'actual_ret': ret,
                'severity': abs(ret),
            })

    divergences.sort(key=lambda x: x['severity'], reverse=True)
    return divergences[:5]  # top 5 divergenze

# ── ACTIVE SHOCKS ─────────────────────────────────────────────────
def get_active_shocks(date_str):
    shock_labels = {
        12: 'FINANCIAL', 14: 'SOVEREIGN', 11: 'PANDEMIC',
        13: 'WAR', 9: 'GEO_SHOCK'
    }
    active = []
    today = date.today().isoformat()
    for sp in SHOCK_PERIODS:
        end = sp['end'] or today
        if sp['start'] <= date_str <= end:
            label = shock_labels.get(sp['id'], '')
            if label and label not in active:
                active.append(label)
    return active

# ── ALERTS ───────────────────────────────────────────────────────
def check_alerts(current_weight, prev_data, etf_data):
    alerts = []
    current_dom = max(current_weight['scenarios'], key=current_weight['scenarios'].get)

    # 1. Cambio scenario dominante
    if prev_data:
        prev_weights = prev_data.get('scenario_weights', [])
        if prev_weights:
            prev_dom = max(prev_weights[-1]['scenarios'], key=prev_weights[-1]['scenarios'].get)
            if prev_dom != current_dom:
                alerts.append({
                    'type': 'scenario_change',
                    'severity': 'HIGH',
                    'msg': f'Cambio regime: {prev_dom} → {current_dom}',
                })

    # 2. Shock attivati
    active_shocks = get_active_shocks(current_weight['date'])
    for shock in active_shocks:
        pct = current_weight['scenarios'].get(shock, 0)
        if pct > 15:
            alerts.append({
                'type': 'shock_active',
                'severity': 'HIGH',
                'msg': f'Shock attivo: {shock} al {pct}%',
            })

    # 3. HY Spread > 500bps
    ind = current_weight.get('indicators', {})
    hy = ind.get('hy_spread')
    if hy and hy > 500:
        alerts.append({
            'type': 'credit_stress',
            'severity': 'HIGH',
            'msg': f'HY Spread in zona Risk-Off: {hy:.0f}bps (soglia 500bps)',
        })

    # 4. Tail risk scenario > 20%
    for sc in SCENARIOS:
        if sc['tag'] == 'SHOCK':
            pct = current_weight['scenarios'].get(sc['code'], 0)
            if pct > 20:
                alerts.append({
                    'type': 'tail_risk',
                    'severity': 'MEDIUM',
                    'msg': f'Tail risk elevato: {sc["name"]} al {pct}%',
                })

    # 5. ETF VXX spike (se disponibile)
    vxx = etf_data.get('VXX', {})
    if vxx.get('ret_1w', 0) and vxx['ret_1w'] > 20:
        alerts.append({
            'type': 'vix_spike',
            'severity': 'HIGH',
            'msg': f'VXX +{vxx["ret_1w"]:.1f}% nella settimana — spike di volatilità',
        })

    return alerts

# ── EMAIL ────────────────────────────────────────────────────────
def send_email(alerts, current_week_data, forecast, etf_data, pages_url, oracle_comment=None):
    if not GMAIL_USER or not GMAIL_PASS or not NOTIFY_EMAIL:
        print("  Email non configurata — skip")
        return

    dom_code = max(current_week_data['scenarios'], key=current_week_data['scenarios'].get)
    dom_pct  = current_week_data['scenarios'][dom_code]
    dom_name = next(s['name'] for s in SCENARIOS if s['code'] == dom_code)

    oracle_html = ("<h3 style=\"color:#FFD700;letter-spacing:2px\">&#128302; IL COMMENTO DELL'ORACOLO</h3>"
                  f"<p style=\"color:#C8D8E8;font-size:13px;line-height:1.8;border-left:3px solid #FFD700;padding-left:12px\">{oracle_comment}</p>") if oracle_comment else ""
    severity_color = {'HIGH': '#FF3355', 'MEDIUM': '#FFB800', 'LOW': '#00FF88'}

    alert_html = ''
    if alerts:
        rows = ''.join(
            f'<tr><td style="color:{severity_color.get(a["severity"],"#ccc")};padding:4px 8px">'
            f'[{a["severity"]}]</td><td style="padding:4px 8px;color:#C8D8E8">{a["msg"]}</td></tr>'
            for a in alerts
        )
        alert_html = f'<h3 style="color:#FFB800;letter-spacing:2px">⚠ ALERT ATTIVI</h3><table>{rows}</table>'

    # Forecast top scenari a 4w
    f4w = forecast.get('4w', {})
    top_f4w = sorted(f4w.items(), key=lambda x: x[1], reverse=True)[:3]
    forecast_html = ''.join(
        f'<div style="margin:4px 0;color:#C8D8E8">{code}: <strong style="color:#00D4FF">{pct}%</strong></div>'
        for code, pct in top_f4w
    )

    body = f"""
    <html><body style="background:#070A0F;font-family:monospace;color:#C8D8E8;padding:24px">
    <h1 style="color:#00D4FF;letter-spacing:4px">RAPTOR MACRO MOVER</h1>
    <p style="color:#4A6070;font-size:11px">{datetime.now().strftime('%d/%m/%Y %H:%M')} CET</p>
    <hr style="border-color:#1E2A38;margin:16px 0">

    <h3 style="color:#7FFF00;letter-spacing:2px">REGIME ATTUALE</h3>
    <p style="font-size:20px"><strong style="color:#00D4FF">{dom_name}</strong> — {dom_pct}%</p>

    {oracle_html}

    {alert_html}

    <h3 style="color:#F39C12;letter-spacing:2px">ORACOLO — PREVISIONE 4 SETTIMANE</h3>
    {forecast_html}

    <hr style="border-color:#1E2A38;margin:16px 0">
    <p><a href="{pages_url}" style="color:#00D4FF">→ Apri RAPTOR MACRO MOVER</a></p>
    </body></html>
    """

    subject_prefix = '🔴 ALERT' if any(a['severity'] == 'HIGH' for a in alerts) else '📊'
    subject = f"{subject_prefix} RAPTOR — {dom_name} {dom_pct}% | {datetime.now().strftime('%d/%m/%Y')}"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = GMAIL_USER
    msg['To']      = NOTIFY_EMAIL
    msg.attach(MIMEText(body, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
            s.login(GMAIL_USER, GMAIL_PASS)
            s.sendmail(GMAIL_USER, NOTIFY_EMAIL, msg.as_string())
        print(f"  ✓ Email inviata a {NOTIFY_EMAIL}")
    except Exception as e:
        print(f"  ✗ Email error: {e}")

# ── MAIN ─────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("RAPTOR MACRO MOVER — Update Script")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Carica dati precedenti (per confronto alert)
    prev_data = None
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                prev_data = json.load(f)
            print(f"  Dati precedenti: {prev_data.get('generated','?')}")
        except:
            pass

    # 1. Fetch FRED
    print("\n[1/4] FRED API...")
    raw = {}
    for s in FRED_SERIES:
        print(f"  Scarico {s['id']} ({s['name']})...")
        raw[s['name']] = fetch_fred(s['id'], s['freq'])
        time.sleep(0.3)

    # 2. Transforms
    print("\n[2/4] Trasformazioni...")
    compute_transforms(raw)

    # 3. Build weekly dataset + classify
    print("\n[3/4] Dataset settimanale + classificazione...")
    weekly = build_weekly_dataset(raw)
    scenario_weights = []
    for row in weekly:
        sc = classify_week(row)
        scenario_weights.append({
            'date': row['date'],
            'indicators': row,
            'scenarios': sc,
        })
    print(f"  ✓ {len(scenario_weights)} settimane classificate")

    # 4. ETF
    print("\n[4/4] ETF Yahoo Finance...")
    etf_data = fetch_etf_data()

    # Dati correnti
    current = scenario_weights[-1]
    current_dom = max(current['scenarios'], key=current['scenarios'].get)

    # Forecast Markov
    forecast = compute_forecast(scenario_weights)

    # ETF divergenze (Q7)
    divergences = compute_etf_divergence(current_dom, etf_data)

    # Active shocks
    active_shocks = get_active_shocks(current['date'])

    # Alerts
    alerts = check_alerts(current, prev_data, etf_data)

    # Groq Oracle Comment
    print("\n[5/5] Groq Oracle...")
    oracle_comment = generate_oracle_comment(current, forecast, etf_data, alerts, active_shocks)

    # Assembla JSON output
    pages_url = os.environ.get('PAGES_URL', 'https://giorgiogoldoni.github.io/raptor-macro-mover')
    output = {
        'generated': datetime.utcnow().isoformat() + 'Z',
        'version': '2.0',
        'current_week': current['date'],
        'macro_indicators': current['indicators'],
        'scenario_weights': scenario_weights,
        'scenarios_meta': SCENARIOS,
        'forecast': forecast,
        'etf_data': etf_data,
        'etf_list': ETF_LIST,
        'etf_divergences': divergences,
        'active_shocks': active_shocks,
        'oracle_comment': oracle_comment,
        'alerts': alerts,
    }

    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    print(f"\n  ✓ Salvato: {DATA_FILE}")

    # Email (solo se ci sono alert o sempre?)
    # Inviamo sempre — il subject indica il livello
    send_email(alerts, current, forecast, etf_data, pages_url, oracle_comment)

    print("\n✓ RAPTOR update completato.")
    if alerts:
        print(f"  Alert generati: {len(alerts)}")
        for a in alerts:
            print(f"  [{a['severity']}] {a['msg']}")

if __name__ == '__main__':
    main()

# ── GROQ ORACLE COMMENT ──────────────────────────────────────────
def generate_oracle_comment(current, forecast, etf_data, alerts, active_shocks):
    """Genera il commento dell'Oracolo via Groq (llama3)."""
    if not GROQ_KEY:
        print("  GROQ_API_KEY non configurata — skip")
        return None

    dom_code = max(current['scenarios'], key=current['scenarios'].get)
    dom_pct  = current['scenarios'][dom_code]
    dom_name = next(s['name'] for s in SCENARIOS if s['code'] == dom_code)
    ind      = current.get('indicators', {})

    top3 = sorted(current['scenarios'].items(), key=lambda x: x[1], reverse=True)[:3]
    top3_str = ' | '.join(f"{c}: {p}%" for c, p in top3)

    f4w = forecast.get('4w', {})
    top_f4w = sorted(f4w.items(), key=lambda x: x[1], reverse=True)[:3]
    forecast_str = ' | '.join(f"{c}: {p}%" for c, p in top_f4w)

    # ETF performance chiave
    etf_highlights = []
    for t in ['SPY', 'GLD', 'TLT', 'VXX', 'USO', 'HYG']:
        if t in etf_data and etf_data[t].get('ret_1w') is not None:
            etf_highlights.append(f"{t}: {etf_data[t]['ret_1w']:+.1f}% 1w")
    etf_str = ' | '.join(etf_highlights)

    shocks_str = ', '.join(active_shocks) if active_shocks else 'nessuno'
    alerts_str = ' | '.join(a['msg'] for a in alerts) if alerts else 'nessuno'

    prompt = f"""Sei RAPTOR, un oracolo macro-finanziario. Analizza i dati e scrivi un commento diretto, concreto e autorevole di massimo 5 frasi. Parla in prima persona come un oracolo. Indica chiaramente cosa sta succedendo, perché è importante e come conviene comportarsi con gli investimenti (overweight/underweight asset class specifiche). Sii diretto, non generico.

DATI ATTUALI:
- Regime dominante: {dom_name} ({dom_pct}%)
- Mix scenari: {top3_str}
- CPI YoY: {ind.get('cpi_yoy', '—')}% | Fed Funds: {ind.get('fed_funds', '—')}% | Yield Curve: {ind.get('yield_curve', '—')}%
- HY Spread: {ind.get('hy_spread', '—')}bps | Real Yield: {ind.get('real_yield', '—')}% | Disoccupazione: {ind.get('unemployment', '—')}%
- M2 YoY: {ind.get('m2_yoy', '—')}% | BTP-Bund: {ind.get('btp_spread', '—')}bps
- ETF chiave questa settimana: {etf_str}
- Shock attivi: {shocks_str}
- Alert: {alerts_str}
- Previsione 4 settimane (Markov): {forecast_str}

Scrivi il commento dell'Oracolo in italiano, tono diretto e autorevole:"""

    try:
        r = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {GROQ_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'model': 'llama3-70b-8192',
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 400,
                'temperature': 0.7,
            },
            timeout=30
        )
        result = r.json()
        comment = result['choices'][0]['message']['content'].strip()
        print(f"  ✓ Commento Oracolo generato ({len(comment)} chars)")
        return comment
    except Exception as e:
        print(f"  ✗ Groq error: {e}")
        return None
