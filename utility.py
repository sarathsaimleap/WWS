import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
from sqlalchemy import create_engine
date = datetime.now().date().strftime("%Y-%m-%d")
from psycopg2 import sql
from psycopg2.extras import execute_values

def connect_database():
    conn = psycopg2.connect(
        host="mstconsumer-aimleap.postgres.database.azure.com",
        database="postgres",
        port=5432,
        user="MSTConsumer@mstconsumer-aimleap",
        password="Retail01"
    )
    return conn

def insert_raw_data(df, name):
    conn = connect_database()
    """   store cleaned data back to database with new table name """
    try:
        #     database_url = "postgresql://doadmin:mq2i4pwpvlen6mho@db-postgresql-sfo3-smartbot-1db-do-user-8157534-0.b.db.ondigitalocean.com:25060/alpha"
        database_url = "postgresql://MSTConsumer@mstconsumer-aimleap:Retail01@mstconsumer-aimleap.postgres.database.azure.com:5432/postgres"
        engine = create_engine(database_url)
        table_name = name
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print('File successfully  moved')
    except Exception as e:
        print('Error in  movement')
        print(f'Error :- {e}')
    # query = """
    #     INSERT INTO public."+{name}+" (date, product_id, product_url, product_name, product_category, product_sub_category, actual_price, discounted_price, feature_of_promotion)
    #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    # """
    # for index, row in df.iterrows():
    #         values = (row['Date'], row['product ID'], row['product URL'], row['product Name'], row['product description'], row['product category'], row['product sub category'], row['actual price'], row['discounted price'], row['feature of promotion'])
    #         with conn.cursor() as cur:
    #             cur.execute(query, values)
    #             conn.commit()
    #             print(" raw data inserted")
    


def clean_data(df):
    df.drop_duplicates(subset=['product_Id', 'product_url'], inplace=True)
    df['discount_price'] = df['discount_price'].replace('0', '')
    df['actual_price'] = df['actual_price'].replace('0', '')
    return df
def insert_cleaned_data(df,table):
    """   store cleaned data back to database with new table name """
    try:
        #     database_url = "postgresql://doadmin:mq2i4pwpvlen6mho@db-postgresql-sfo3-smartbot-1db-do-user-8157534-0.b.db.ondigitalocean.com:25060/alpha"
        database_url = "postgresql://MSTConsumer@mstconsumer-aimleap:Retail01@mstconsumer-aimleap.postgres.database.azure.com:5432/postgres"
        engine = create_engine(database_url)
        table_name = table
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print('File successfully  moved')
    except Exception as e:
        print('Error in  movement')
        print(f'Error :- {e}')
    