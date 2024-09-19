'''
===================================================================================
Milestone 3

Nama  : Fahmi
Batch : RMT-034

Program ini dirancang dalam rangka mengotomatiskan transform dan load data dari PostgreSQL ke ElasticSearch. 
Mengenai dataset yang dipakai, itu mengenai laporan penjualan komoditas dari perusahaan produksi hasil olahan susu di India.

===================================================================================
'''

# import 
import datetime as dt
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from elasticsearch import Elasticsearch
import pandas as pd

# Fungsi untuk mengambil data dari PostgreSQL
def fetch_data_from_postgresql():
    '''
    Fungsi ini menyambungkan dengan database PostgreSQL menggunakan `PostgresHook` dari Airflow. 
    Setelah itu, mengambil data dari tabel `table_m3` lalu menyimpan data yang diambil ke dalam file CSV.

    Raises:
        AirflowException: Jika terjadi masalah dengan koneksi atau eksekusi syntax.
    '''
    # Koneksi ke PostgreSQL menggunakan PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Mengambil data dari table_m3
    query = "SELECT * FROM table_m3"
    df = pg_hook.get_pandas_df(query)
    
    # Simpan data mentah ke CSV
    raw_data_path = '/opt/airflow/dags/P2M3_fahmi_data_raw.csv'
    df.to_csv(raw_data_path, index=False)

# Fungsi untuk membersihkan data
def clean_data():
    '''
    Fungsi ini membaca data dari file CSV yang telah disimpan sebelumnya lalu melakukan berbagai pembersihan data.
    Barulah setelah itu, menyimpannya ke file CSV baru.

    Raises:
        FileNotFoundError: Jika file CSV mentah tidak ditemukan pada path yang ditentukan.
        pd.errors.EmptyDataError: Jika file CSV kosong atau tidak dapat dibaca.
    '''
    # Membaca data dari CSV
    raw_data_path = '/opt/airflow/dags/P2M3_fahmi_data_raw.csv'
    df = pd.read_csv(raw_data_path)
    
    # Menghapus duplikat
    df.drop_duplicates(inplace=True)
    
    # Normalisasi nama kolom
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
    
    # Mengatasi missing values, misalnya dengan mengisi nilai yang hilang dengan median
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col].fillna(df[col].median(), inplace=True)

    # membuat kolom unik
    df['unique_id'] = df['location'].str[0] + df['customer_location'].str[0] + df['storage_condition'].str[0] + df['farm_size'].str[0] + df['sales_channel'].str[0] + df['brand'].str[0] + df['product_name'].str[0] + df['product_id'].astype(str).str[0] + df['quantity_sold'].astype(str).str[0] + df['quantity_in_stock'].astype(str).str[0] + df['price_per_unit'].astype(str).str[0] + df['minimum_stock_threshold'].astype(str).str[0] + df['total_value'].astype(str).str[0]  
    
    # Menemukan kolom yang mengandung kata 'date'
    date_columns = [col for col in df.columns if 'date' in col]

    # Mengubah tipe data kolom tersebut menjadi datetime
    df[date_columns] = df[date_columns].apply(pd.to_datetime)    

    # Simpan data yang sudah dibersihkan ke CSV
    clean_data_path = '/opt/airflow/dags/P2M3_fahmi_data_clean.csv'
    df.to_csv(clean_data_path, index=False)

# Fungsi untuk posting data ke Elasticsearch
def post_to_elasticsearch():
    '''
    Fungsi ini membaca data dari file CSV yang telah dibersihkan lalu menginisialisasi klien Elasticsearch. 
    Walakhir, memposting setiap baris data ke indeks di Elasticsearch.

     Raises:
        FileNotFoundError: Jika file CSV yang dibaca tidak ditemukan pada path yang ditentukan.
        ElasticsearchException: Jika terjadi masalah saat menginisialisasi klien Elasticsearch
        atau memposting dokumen.
    '''
    # Membaca data yang sudah dibersihkan
    clean_data_path = '/opt/airflow/dags/P2M3_fahmi_data_clean.csv'
    df = pd.read_csv(clean_data_path)

    # Inisialisasi client Elasticsearch
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])  # Sesuaikan dengan URL Elasticsearch Anda

    # Nama indeks di Elasticsearch
    index_name = 'cleaned_data_index'

    # Posting setiap baris data ke Elasticsearch
    for i, row in df.iterrows():
        # Mengubah setiap baris menjadi dictionary
        doc = row.to_dict()
        # Posting dokumen ke Elasticsearch
        es.index(index=index_name, body=doc)

    print(f"Data berhasil diposting ke Elasticsearch di index '{index_name}'")

# Definisi default_args
default_args = {
    'owner': 'Fahmi',
    'start_date': datetime(2024, 9, 11, 7, 28, 0) - timedelta(hours=7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definisi DAG
with DAG(
    "H8_eda_pipeline",
    description='H8 EDA pipeline',
    schedule_interval='30 6 * * *',
    default_args=default_args, 
    catchup=False
) as dag:

    # Task untuk mengambil data dari PostgreSQL
    fetch_task = PythonOperator(
        task_id='fetch_data_from_postgresql',
        python_callable=fetch_data_from_postgresql
    )

    # Task untuk melakukan data cleaning
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    # Tambahkan task baru ke DAG
    post_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    # Dependencies antartask
    fetch_task >> clean_task >> post_task