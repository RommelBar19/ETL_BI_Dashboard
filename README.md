# ETL & Business Intelligence Dashboard

Questo progetto gestisce l'intero flusso di dati (pipeline ETL) per l'analisi delle performance aziendali.

## Contenuto del repository

* **Pipeline ETL**: codice Python per estrazione, pulizia (Pandas) e caricamento dei dati in PostgreSQL.
* **Database Design**: architettura *Star Schema* ottimizzata per query analitiche.
* **Business Intelligence**: file Power BI (`.pbix`) con dashboard interattive per il monitoraggio dei KPI.

## Tecnologie utilizzate

* Python (Pandas, Psycopg2)
* PostgreSQL
* Power BI

---

# Guida all'installazione e configurazione

Segui questi passaggi in ordine per configurare l'ambiente, popolare il database e collegare il report Power BI.

## 1. Installazione dei requisiti

Assicurati di avere Python installato. Apri il terminale nella cartella principale del progetto ed esegui il comando seguente per installare tutte le librerie necessarie:

```bash
pip install -r requirements.txt
```

## 2. Configurazione del file di ambiente (`.env`)

Il progetto utilizza un file `.env` per gestire le credenziali in modo sicuro.

1. Individua il file chiamato `.envexample`.
2. Rinominalo esattamente in `.env`.
3. Apri il file `.env` con un editor di testo e inserisci i valori corretti.

## 3. Popolamento del database

Una volta configurate le credenziali, esegui lo script Python per processare i dati e caricarli nel database:

```bash
python main.py
```

## 4. Connessione del report Power BI

Dopo aver eseguito lo script, apri il file di Power BI (`.pbix`) e segui questi passaggi per collegarlo al database:

1. Apri il file del report in **Power BI Desktop**.
2. Nella barra multifunzione in alto, vai sulla scheda **Home**.
3. Clicca sulla freccia accanto a **Trasforma dati** e seleziona **Impostazioni origine dati**.
4. Seleziona l'origine dati attuale e clicca su **Modifica origine**.
5. Inserisci le credenziali del database (le stesse inserite nel file `.env`).
6. Clicca su **Chiudi e applica** per aggiornare i dati del report.
