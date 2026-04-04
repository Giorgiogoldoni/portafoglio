# RAPTOR MACRO MOVER

Classificatore macro settimanale a 14 scenari con forecast Markov chain e monitor ETF globale.

## Struttura repo

```
raptor-macro-mover/
├── index.html              ← Frontend (GitHub Pages)
├── data/
│   └── latest.json         ← Dataset generato automaticamente
├── scripts/
│   └── update.py           ← Script di aggiornamento dati
├── .github/workflows/
│   └── update.yml          ← GitHub Actions (ogni giorno alle 12:00)
└── requirements.txt
```

## Setup GitHub Secrets

Vai su **Settings → Secrets and variables → Actions** e aggiungi:

| Secret | Descrizione |
|--------|-------------|
| `FRED_API_KEY` | API key gratuita da [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| `GMAIL_USER` | Il tuo indirizzo Gmail (es. `tuo@gmail.com`) |
| `GMAIL_APP_PASSWORD` | App Password Gmail (Account Google → Sicurezza → Password per le app) |
| `NOTIFY_EMAIL` | Email dove ricevere gli alert |

## Setup GitHub Pages

Vai su **Settings → Pages**:
- Source: **Deploy from a branch**
- Branch: `main` / `/ (root)`

L'app sarà disponibile su: `https://giorgiogoldoni.github.io/raptor-macro-mover`

## Aggiornamento manuale

Per forzare un aggiornamento: **Actions → RAPTOR Daily Update → Run workflow**

## Scenari classificati (14)

**Ciclici:** Goldilocks · Reflazione · Stagflazione · Risk-Off Acuto · Disinflazione Morbida · Recessione · ZIRP · Tightening Aggressivo · Geopolitical Shock · Boom/Euforia

**Shock:** Pandemic · Financial Crisis · War/Geo Rupture · Sovereign/Debt Crisis

## ETF monitorati (42)

Copertura globale: USA · Europa · Asia · Emerging · Bond · Commodity · Volatilità · Settoriali · Valute
