import os
from datetime import datetime
from src import ROOT_DIR, raw_path, wip_path, processed_path
from src import extract as xt
from src import transform as tr
from src import load as ld
import pandas as pd

pd.set_option("display.max_columns", None)


def iniziale():
    print("Sei dentro l'applicativo di ETL")
    scelta = None
    df = None
    while not scelta == "0":
        while not scelta:
            try:
                if df is not None:
                    print(f"\nHai il file {file}, si trova {stato}")
                print("\nScegli cosa vuoi fare:")
                print("\n1. 📂Estrarre un file")
                print("2. 👀Visualizzare un file")
                print("3. 💱Trasformare il file")
                print("4. 💻Caricare nel Database il file")
                print("5. 🗄️Salvare il file")
                print("6. 🗃️️Fare il ETL completo della cartella 'raw'")
                print("0. 🚪Uscire")
                scelta = input("\nInserisci solo il numero....").strip()
                scelta = scelta[0]
                if not scelta[0] in ("1", "2", "3", "4", "5", "6", "0"):
                    raise ValueError("Il numero deve essere compreso tra 0 e 6.")
            except ValueError as e:
                print("\nError: ", e)
                scelta = None
            except IndexError:
                print("\nError: Hai lasciato vuoto.")
                scelta = None
        if scelta == "1":
            df, file, stato = estrarre()
            scelta = None
        elif scelta == "2":
            try:
                print(f"\nSi visualizzano le prime 10 righe del file {file}...\n")
                print(df.head(10))
            except UnboundLocalError:
                print("\nError: Non c'è nessun file selezionato da visualizzare")
            scelta = None
        elif scelta == "3":
            try:
                if stato == "estratto":
                    df = trasformare(df, file)
                    stato = "trasformato"
                elif stato == "trasformato":
                    raise RuntimeError("Il file è già stato trasformato")
                elif stato == "caricato":
                    raise RuntimeError("Il file è già stato caricato, non serve trasformarlo.")
                elif not stato:
                    raise ValueError("Nessun file estratto da trasformare")
            except(RuntimeError, ValueError) as e:
                print("\nErrore: ", e)
            scelta = None
        elif scelta == "4":
            try:
                if stato == "trasformato":
                    caricare(df, file)
                    print(f"il file {file} è stato caricato nel DataBase.")
                    stato = "caricato"
                elif stato == "caricato":
                    raise RuntimeError("Il file è già stato caricato")
                elif stato == "estratto":
                    raise RuntimeError("Il file non è ancora stato trasformato, non può essere caricato.")
                elif not stato:
                    raise ValueError("Nessun file trasformato da caricare")
            except(RuntimeError, ValueError) as e:
                print("\nErrore: ", e)
            scelta = None
        elif scelta == "5":
            try:
                if stato:
                    salvare(df, file, stato)
                    print(f"Il file {file} è stato salvato")
                elif not stato:
                    raise ValueError("Nessun file da salvare")
            except ValueError as e:
                print("\nErrore: ", e)
            scelta = None
        elif scelta == "6":
            etl_completo()
            scelta = None
        elif scelta == "0":
            print("arrivederci")
def estrarre():
    scelta = None
    while not scelta:
        try:
            print("\nScegli la cartella dove si trova il file con cui vuoi lavorare.")
            print("1. raw - sono i file nuovi")
            print("2. wip - sono i file a cui ci sta ancora lavorando")
            print("3. processed - sono i file gia processati")
            scelta = input("...").strip()
            scelta = scelta[0]
            if not scelta[0] in ("1", "2", "3"):
                raise ValueError("Il numero deve essere compreso tra 1 e 3.")
        except ValueError as e:
            print("\nError: ", e)
            scelta = None
        except IndexError:
            print("\nError: Hai lasciato vuoto.")
            scelta = None
    if scelta == "1":
        cartella = os.scandir(ROOT_DIR / raw_path)
    elif scelta == "2":
        cartella = os.scandir(ROOT_DIR / wip_path)
    elif scelta == "3":
        cartella = os.scandir(ROOT_DIR / processed_path)

    n_file = 1
    archivi = {}
    for file in cartella:
        archivi[str(n_file)] = file
        n_file += 1
    scelta = None
    tabelle = ["customers", "categories", "items", "orders", "sellers", "products"]
    while not scelta:
        try:
            print()
            print("Scegli il file che vuoi estrarre: \n")
            for key, file in archivi.items():
                print(f"{key}. {file.name}")
            scelta = input("\nInserisci solo il numero....").strip()
            if not scelta in archivi.keys():
                raise KeyError("Il numero deve essere compreso tra le opzioni visibili.")
            elif not os.path.splitext(archivi[scelta])[1].lower() == ".csv":
                raise ValueError("Il file selezionato non è un 'csv'.")
            supportato = False
            for tabella in tabelle:
                if tabella in archivi[scelta].name:
                    supportato = True
            if not supportato:
                raise ValueError("Il file selezionato non è una tabella che supporta questo programma.")
        except (ValueError, KeyError) as e:
            print("\nError: ", e)
            scelta = None
        except IndexError:
            print("\nError: Hai lasciato vuoto.")
            scelta = None
    file = archivi[scelta]
    df = xt.extract_csv(file)
    file = file.name
    if "customers" in file:
        file_nome = "customers"
    elif "categories" in file:
        file_nome = "categories"
    elif "items" in file:
        file_nome = "items"
    elif "orders" in file:
        file_nome = "orders"
    elif "sellers" in file:
        file_nome = "sellers"
    elif "products" in file:
        file_nome = "products"

    if "trasformato" in file:
        stato = "trasformato"
    elif "caricato" in file:
        stato = "caricato"
    else:
        stato = "estratto"

    return df, file_nome, stato
def trasformare(df, file):
    if "customers" in str(file):
        df = tr.trf_customer(df)
    elif "categories" in str(file):
        df = tr.trf_category(df)
    elif "items" in str(file):
        df = tr.trf_items(df)
    elif "orders" in str(file):
        df = tr.trf_order(df)
    elif "sellers" in str(file):
        df = tr.trf_seller(df)
    elif "products" in str(file):
        df = tr.trf_prod(df)
    return df
def caricare(df, file):
    if "customers" in str(file):
        ld.ld_customers(df)
    elif "categories" in str(file):
        ld.ld_categories(df)
    elif "items" in str(file):
        ld.ld_items(df)
    elif "orders" in str(file):
        ld.ld_orders(df)
    elif "sellers" in str(file):
        ld.ld_sellers(df)
    elif "products" in str(file):
        ld.ld_products(df)
def salvare(df, file, stato):
    cur_date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_nome = file + "_" + str(stato) + "_" + cur_date_time + ".csv"
    if not stato == "caricato":
        df.to_csv(ROOT_DIR / wip_path / file_nome, encoding="utf-8", index = False)
    else:
        df.to_csv(ROOT_DIR / processed_path / file_nome, encoding="utf-8", index = False)
    print(f"Il file {file_nome} è stato creato")

def etl_completo():
    cartella = os.scandir(ROOT_DIR / raw_path)
    try:
        cartella = list(cartella)
        if not cartella:
            raise FileNotFoundError("⚠️ la cartella è vuota non ci sono file su cui lavorare")
        total_files = len(cartella)
        n = 0
        for file in cartella:
            n += 1
            print(f"\nEseguendo l'ETL del file {file.name} file {n} di {total_files}")
            if not os.path.splitext(file)[1].lower() == ".csv":
                print(f"Il file {file.name} è ignorato perché non è un archivio csv")
                continue
            elif "customers" in str(file):
                df = tr.trf_customer(xt.extract_csv(file))
                ld.ld_customers(df)
            elif "categories" in str(file):
                df = tr.trf_category(xt.extract_csv(file))
                ld.ld_categories(df)
            elif "items" in str(file):
                df = tr.trf_items(xt.extract_csv(file))
                ld.ld_items(df)
            elif "orders" in str(file):
                df = tr.trf_order(xt.extract_csv(file))
                ld.ld_orders(df)
            elif "sellers" in str(file):
                df = tr.trf_seller(xt.extract_csv(file))
                ld.ld_sellers(df)
            elif "products" in str(file):
                df = tr.trf_prod(xt.extract_csv(file))
                ld.ld_products(df)
            else:
                print(f"Il file {file.name} è ignorato perché non è una tabella gestita in questo programma(customers, categories, items, orders, sellers, products)")
                continue
            file = file.name
            print(f"ETL del file {file} completato")

            if "customers" in file:
                file = "customers"
            elif "categories" in file:
                file = "categories"
            elif "items" in file:
                file = "items"
            elif "orders" in file:
                file = "orders"
            elif "sellers" in file:
                file = "sellers"
            elif "products" in file:
                file = "products"
            cur_date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file = file + "_caricato_" + cur_date_time + ".csv"
            df.to_csv(ROOT_DIR / processed_path / file, encoding="utf-8", index=False)
            print(f"\nIl file {file} è stato creato.")
    except FileNotFoundError as e:
        print("Errore: ", e)
if __name__ == "__main__":
    iniziale()
    #estrarre()