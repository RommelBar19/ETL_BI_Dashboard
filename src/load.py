import pandas as pd

from src import ROOT_DIR, processed_path
import transform as trf
import extract as xt

import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
host = os.getenv("dbhost")
db = os.getenv("dbname")
user = os.getenv("dbuser")
password = os.getenv("dbpassword")
port = os.getenv("dbport")

def leggere_csv(df):
    df = pd.read_csv(ROOT_DIR / processed_path / df)
    return df

def conect_database(sql, df = None):
    print("connettendo con il Database")
    with psycopg.connect(host = host,
                         dbname = db,
                         user= user,
                         password = password,
                         port = port) as conn:
        with conn.cursor() as cur:
            if not df is None:
                print(f"Caricamento in corso... {str(len(df))} righe da inserire.")
                perc_int = 0
                for index, row in df.iterrows():
                    perc = float("%.2f" % ((index + 1) / len(df) * 100))
                    if perc >= perc_int:
                        print(f"{round(perc)}% Completato", end="\n")
                        perc_int += 5
                    cur.execute(sql, row.to_list())
            else:
                print("creando la tabella")
                cur.execute(sql)
                print("tabella creata")
            conn.commit()

def ld_categories(df):

    sql = """
        CREATE TABLE IF NOT EXISTS categories(
        subcategory_en character varying PRIMARY KEY,
        subcategory_it character varying,
        category_it character varying);"""
    conect_database(sql)

    sql = """
        INSERT INTO categories(subcategory_en, subcategory_it, category_it)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;"""
    conect_database(sql, df)

def ld_customers(df):

    sql = """
        CREATE TABLE IF NOT EXISTS customers (
        customer_id character varying NOT NULL PRIMARY KEY,
        region character varying,
        city character varying,
        cap character varying(5));
        """
    conect_database(sql)

    sql = """
        INSERT INTO customers(customer_id, region, city, cap)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;"""
    conect_database(sql, df)

def ld_items(df):
    sql = """
        CREATE TABLE IF NOT EXISTS items(
        order_product_id character varying NOT NULL PRIMARY KEY,
        order_id character varying,
        order_item integer,
        product_id character varying,
        seller_id character varying,
        product_price numeric(10,2),
        shipping_price numeric(10,2),
        total_price numeric(10,2))
        ;"""

    conect_database(sql)

    sql = """
        INSERT INTO items(order_product_id, order_id, order_item, product_id, seller_id, product_price, shipping_price, total_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;"""
    conect_database(sql, df)

def ld_orders(df):
    sql = """
        CREATE TABLE IF NOT EXISTS orders(
        order_id character varying PRIMARY KEY,
        customer_id character varying,
        status character varying,
        purchase_date TIMESTAMP,
        delivered_date TIMESTAMP,
        estimated_delivery_date date);"""
    conect_database(sql)
    sql = """
        INSERT INTO orders(order_id ,customer_id , status , purchase_date, delivered_date, estimated_delivery_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING"""

    conect_database(sql, df)

def ld_products(df):
    sql = """
        CREATE TABLE IF NOT EXISTS products(
        product_id character varying PRIMARY KEY,
        subcategory_en character varying,
        name_length integer,
        description_length integer,
        photos_qty integer);"""

    conect_database(sql)

    sql = """
        INSERT INTO products(product_id, subcategory_en, name_length, description_length, photos_qty)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING"""
    conect_database(sql, df)

def ld_sellers(df):
    sql = """
            CREATE TABLE IF NOT EXISTS sellers(
            seller_id character varying PRIMARY KEY,
            region character varying);"""

    conect_database(sql)

    sql = """
            INSERT INTO sellers(seller_id, region)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING"""
    conect_database(sql, df)

"""Region,
city
cap
"""
    #todo constraits:
#items
"""CONSTRAINT orders FOREIGN KEY (order_id)
        REFERENCES orders (order_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID)"""

if __name__ == "__main__":
    #categories()
    #ld_customers(trf.trf_customer(xt.extract_csv("olistPW_2016_customers.csv")))
    #ld_items(trf.trf_items(xt.extract_csv("olistPW_2016_items.csv")))
    #ld_orders(trf.trf_order(xt.extract_csv("olistPW_2016_orders.csv")))
    #ld_products(trf.trf_prod(xt.extract_csv("olistPW_2016_products.csv")))
    ld_sellers(trf.trf_seller(xt.extract_csv("olistPW_2016_sellers.csv")))


