import json

import pandas as pd
from debugpy.common.timestamp import reset

#import extract as xt
from src import ROOT_DIR, processed_path, config_path, raw_path

pd.set_option("display.max_columns", None)

def trf_customer(df):
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df["cap"] = df["cap"].astype(str).str.zfill(5)
    #df.to_csv(ROOT_DIR / processed_path / "customers.csv", encoding="utf-8", index = False)
    return df

def trf_prod(df):
    df= df.drop_duplicates()
    df = df.reset_index(drop=True)
    df = df.fillna({"product_name_length": 0,
                    "product_description_length": 0,
                    "product_photos_qty": 0})
    df = df.astype({"product_name_length": "uint8",
                    "product_description_length": "uint16",
                    "product_photos_qty": "uint8"})
    df = df.rename(columns={"category": "subcategory_en",
                            "product_name_length": "name_length",
                            "product_description_length": "description_length",
                            "product_photos_qty": "photos_quantity"})
    df["subcategory_en"] = df["subcategory_en"].fillna("non_definito")
    #df_cat = trf_category(xt.extract_csv(ROOT_DIR/ raw_path/ "olistPW_2016_categories.csv"))
    #mapper = df_cat.set_index("subcategory_en")["category_it"]
    #df["category_it"] = df["subcategory_en"].map(mapper).fillna("non_definito")

    #df.to_csv(ROOT_DIR / processed_path / "products.csv", encoding="utf-8", index=False)
    return df
def trf_items(df):
    df["total_price"] = df["freight"] + df["price"]
    df = df.reset_index(drop=True)
    #df["index"] = df["index"] + 1
    #df["index"] = df["index"].astype(str).str.zfill(3)
    df = df.rename(columns={"freight": "shipping_price", "price": "product_price", "index":"order_product_id"})
    df = df.astype({"order_id": "string",
                    "order_item": "uint8",
                    "product_id": "string",
                    "seller_id": "string",
                    "shipping_price": "float",
                    "product_price": "float",
                    "total_price": "float"})
    df = df.sort_values(by=["order_id", "order_item"], ascending=True)
    #df.to_csv(ROOT_DIR / processed_path / "items.csv", encoding="utf-8", index=False)
    return df
def trf_category(df):
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df = df.rename(columns={"product_category_name_english": "subcategory_en", "product_category_name_italian": "subcategory_it"})
    df = df.astype({"subcategory_it": "string",
                    "subcategory_en": "string"})
    with open(ROOT_DIR / config_path / "translation.json", "r", encoding="utf-8") as f:
        translations = json.load(f)
    df["category_it"] = df["subcategory_it"].replace(translations)

    """df = pd.concat([df, pd.DataFrame([{
        "subcategory_en" : "non_definito",
        "subcategory_it" : "non_definito",
        "category_it" : "non_definito"
    }])], ignore_index=True)
"""
    #df.to_csv(ROOT_DIR / processed_path / "categories.csv", encoding="utf-8", index=False)
    return df
def trf_order(df):
    df = df.drop_duplicates()
    df = df.astype({"order_id": "string",
                    "customer_id": "string",
                    "order_status": "string",
                    "order_purchase_timestamp": "datetime64[ns]",
                    "order_delivered_customer_date": "datetime64[ns]",
                    "order_estimated_delivery_date": "datetime64[ns]"})
    for col in ["order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date"]:
        df[col] = df[col].replace({pd.NaT: None})
    #df.to_csv(ROOT_DIR / processed_path / "orders.csv", encoding="utf-8", index=False)

    return df
def trf_seller(df):
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    #df.to_csv(ROOT_DIR / processed_path / "sellers.csv", encoding="utf-8", index=False)
    return  df
if __name__=="__main__":
    pass
    #print(trf_prod(xt.extract_csv("olistPW_2016_products.csv")))
