import os

from src import ROOT_DIR, raw_path
import pandas as pd
import transform as tr

def scaning_folder():
    cartella = os.scandir(ROOT_DIR / raw_path)
    for file in cartella:
        print(file)
        if "customers" in str(file):
            tr.trf_customer(extract_csv(file))
        elif "categories" in str(file):
            tr.trf_category(extract_csv(file))
        elif "items" in str(file):
            tr.trf_items(extract_csv(file))
        elif "orders" in str(file):
            tr.trf_order(extract_csv(file))
        elif "sellers" in str(file):
            tr.trf_seller(extract_csv(file))
        elif "products" in str(file):
            tr.trf_prod(extract_csv(file))

def extract_csv(file_name):
    print("Extracting file...")
    df = pd.read_csv(ROOT_DIR / raw_path / file_name, sep=",")
    print(f"{file_name} extracted")
    return df

if __name__=="__main__":
    scaning_folder()