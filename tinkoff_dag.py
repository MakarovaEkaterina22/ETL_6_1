from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import gzip
from urllib import request
import os
from urllib.error import HTTPError, URLError

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="tinkoff_page_views",
    start_date=datetime(2025, 2, 23),
    end_date=datetime(2025, 2, 24),
    schedule_interval="@once",
    catchup=False,
    default_args=default_args,
    max_active_runs=1
)

# 1. Функция для скачивания данных
def _download_all_hours(**context):
    logical_date = context['logical_date']
    output_dir = "/tmp/wikimedia_pageviews_split"
    os.makedirs(output_dir, exist_ok=True)
    
    downloaded_files = []
    
    for hour in range(24):
        url = (
            f"https://dumps.wikimedia.org/other/pageviews/"
            f"{logical_date.year}/{logical_date.year}-{logical_date.month:02d}/"
            f"pageviews-{logical_date.year}{logical_date.month:02d}"
            f"{logical_date.day:02d}-{hour:02d}0000.gz"
        )
        output_file = f"{output_dir}/pageviews_{logical_date.strftime('%Y%m%d')}-{hour:02d}.gz"
        
        try:
            logging.info(f"Downloading {url}")
            request.urlretrieve(url, output_file)
            
            if os.path.getsize(output_file) == 0:
                os.remove(output_file)
                raise ValueError(f"Empty file: {output_file}")
                
            downloaded_files.append(output_file)
            logging.info(f"Downloaded {output_file} ({os.path.getsize(output_file)} bytes)")
            
        except Exception as e:
            logging.error(f"Failed to download hour {hour}: {str(e)}")
            continue
    
    if not downloaded_files:
        raise ValueError("No files were downloaded successfully")
    
    return downloaded_files

# 2. Функция для обработки данных
def _process_data(**context):
    logical_date = context['logical_date']
    ti = context['ti']
    downloaded_files = ti.xcom_pull(task_ids='download_all_hours')
    
    if not downloaded_files:
        raise ValueError("No files to process")
    
    exact_matches = {
        'en': ["Tinkoff", "Tinkoff Bank", "Tinkoff_Bank", "T-Bank"],
        'ru': ["Тинькофф", "Тинькофф Банк", "Тинькофф_Банк", "Т-Банк"],
        'de': ["Tinkoff", "Tinkoff Bank"],
        'fr': ["Tinkoff", "Banque Tinkoff"],
    }
    
    processed_data = []
    
    for file_path in downloaded_files:
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        parts = line.strip().split()
                        if len(parts) < 4:
                            continue
                            
                        domain = parts[0]
                        title = parts[1]
                        views = int(parts[2])
                        
                        domain_parts = domain.split('.')
                        language = domain_parts[0]
                        is_mobile = len(domain_parts) > 1 and domain_parts[1] == 'm'
                        
                        if language in exact_matches:
                            normalized_title = title.replace('_', ' ')
                            for term in exact_matches[language]:
                                if normalized_title.lower() == term.lower():
                                    processed_data.append({
                                        'title': normalized_title,
                                        'views': views,
                                        'language': language,
                                        'is_mobile': is_mobile
                                    })
                                    break
                    except Exception as e:
                        logging.warning(f"Error parsing line: {str(e)}")
                        continue
                        
            os.remove(file_path)
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            continue
    
    if not processed_data:
        logging.warning("No matching records found after processing")
        return None
    
    # Агрегируем данные по одинаковым страницам
    aggregated_data = {}
    for item in processed_data:
        key = (item['title'], item['language'], item['is_mobile'])
        aggregated_data[key] = aggregated_data.get(key, 0) + item['views']
    
    # Преобразуем в список словарей для XCom
    result = [{
        'title': k[0],
        'language': k[1],
        'is_mobile': k[2],
        'views': v
    } for k, v in aggregated_data.items()]
    
    return result

# 3. Функция для загрузки данных в PostgreSQL
def _load_data(**context):
    logical_date = context['logical_date']
    ti = context['ti']
    processed_data = ti.xcom_pull(task_ids='process_data')
    
    if not processed_data:
        logging.warning("No data to load")
        return
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            "DELETE FROM tinkoff_pageview_counts WHERE view_date = %s",
            (logical_date.date(),)
        )
        
        for item in processed_data:
            cursor.execute(
                """
                INSERT INTO tinkoff_pageview_counts
                (page_name, views_count, view_date, language_code, is_mobile)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (item['title'], item['views'], logical_date.date(), 
                 item['language'], item['is_mobile'])
            )
        
        conn.commit()
        logging.info(f"Loaded {len(processed_data)} records for {logical_date.date()}")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Database error: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

# 4. Функция для создания таблицы
def _create_table_if_not_exists():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tinkoff_pageview_counts (
                id SERIAL PRIMARY KEY,
                page_name TEXT NOT NULL,
                views_count INTEGER NOT NULL,
                view_date DATE NOT NULL,
                language_code VARCHAR(10) NOT NULL,
                is_mobile BOOLEAN NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(page_name, view_date, language_code, is_mobile)
            );
        """)
        conn.commit()
        logging.info("Table created or already exists")
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

# Определение задач
create_table_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=_create_table_if_not_exists,
    dag=dag,
)

download_task = PythonOperator(
    task_id="download_all_hours",
    python_callable=_download_all_hours,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_data",
    python_callable=_process_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=_load_data,
    provide_context=True,
    dag=dag,
)

# Порядок выполнения задач
create_table_task >> download_task >> process_task >> load_task