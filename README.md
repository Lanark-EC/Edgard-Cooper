# Script Hub — Flask App

Een interne web-app om Python scripts uit te voeren via een browser.

## Lokaal starten

```bash
# 1. Installeer dependencies
pip install -r requirements.txt

# 2. Start de app
python app.py

# 3. Open in je browser
http://localhost:5000
```

## Scripts toevoegen

1. Maak een nieuw bestand in `scripts/jouw_script.py`
2. Schrijf een `run_jouw_script(file)` functie die een `io.BytesIO` buffer teruggeeft
3. Voeg een route toe in `app.py`
4. Voeg een pagina toe in `templates/index.html`

## Online zetten (gratis)

### Render.com
1. Push naar GitHub (private repo is ok)
2. Ga naar render.com → New Web Service → koppel je repo
3. Build command: `pip install -r requirements.txt`
4. Start command: `gunicorn app:app`
5. Deploy → je krijgt een URL

### Railway.app
1. Push naar GitHub
2. Ga naar railway.app → New Project → Deploy from GitHub
3. Railway detecteert automatisch de Procfile
4. Deploy → URL delen met collega's

## Projectstructuur

```
scripthub/
├── app.py              ← Flask routes
├── requirements.txt    ← Python packages
├── Procfile            ← Voor Render/Railway
├── scripts/
│   ├── __init__.py
│   ├── launch_check.py   ← Script logica
│   └── garvis_export.py  ← Script logica
└── templates/
    └── index.html        ← Volledige UI
```
