import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
date = datetime.now().date().strftime("%Y-%m-%d")


def connect_database():

    conn = psycopg2.connect(
        host="mstconsumer-aimleap.postgres.database.azure.com",
        database="postgres",
        port=5432,
        user="MSTConsumer@mstconsumer-aimleap",
        password="vFQXD3eCt58BvFa"
    )


def insert_raw_data(df, name):
    conn = connect_database()
    try:
        # Use sql.Identifier to safely format the table name
        table_identifier = sql.Identifier(name)

        query = sql.SQL("""
            INSERT INTO public.{} (date, product_id, product_url, product_name, product_category, product_sub_category, actual_price, discounted_price, feature_of_promotion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """).format(table_identifier)

        for index, row in df.iterrows():
            values = (row['Date'], row['product ID'], row['product URL'], row['product Name'], row['product description'],
                      row['product category'], row['product sub category'], row['actual price'], row['discounted price'], row['feature of promotion'])
            with conn.cursor() as cur:
                cur.execute(query, values)
                conn.commit()
                print("Cleaned data inserted")
    except Exception as e:
        print(f"Error: {e}")


def clean_data(df):
    df.drop_duplicates(subset=['Product Id', 'Product Url'], inplace=True)
    df['Discounted Price'] = df['Discounted Price'].replace('0', '')
    df['Actual price'] = df['Actual price'].replace('0', '')
    return df


def insert_cleaned_data(df, name):
    conn = connect_database()
    table_identifier = sql.Identifier(name)
    try:
        query = sql.SQL("""
                INSERT INTO public.{} (date, product_id, product_url, product_name, product_category, product_sub_category, actual_price, discounted_price, feature_of_promotion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """).format(table_identifier)
        for index, row in df.iterrows():
            values = (row['Date'], row['product ID'], row['product URL'], row['product Name'], row['product description'],
                      row['product category'], row['product sub category'], row['actual price'], row['discounted price'], row['feature of promotion'])
            with conn.cursor() as cur:
                cur.execute(query, values)
                conn.commit()
                print("Cleaned data inserted")
    except Exception as e:
        print(f"Error: {e}")
