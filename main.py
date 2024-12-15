import os
from dotenv import load_dotenv
import time
import psycopg2
import hashlib
import random
from faker import Faker
from threading import Thread
from datetime import datetime

env_file = ".env.production" if os.getenv("ENV") == "production" else ".env.development"
load_dotenv(env_file)

def generate_primary_key():
    """
    Genera una clave primaria combinando un hash y un valor incremental.
    """
    # Obtener la fecha y hora actuales
    current_datetime = datetime.now()

    # Formatear la fecha y hora incluyendo milisegundos
    datetime_string = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")  # Truncar a 3 dígitos de milisegundos

    # Generar un hash SHA256 basado en el base_string
    hash_object = hashlib.sha256(datetime_string.encode())
    hash_string = hash_object.hexdigest()[:10]  # Tomar los primeros 10 caracteres del hash


    # Combinar el hash con el valor incremental
    primary_key = f"{hash_string}-{datetime_string}"
    return primary_key

def connect_to_db(host, dbname, user, password):
    """Establece una conexión con la base de datos PostgreSQL."""
    return psycopg2.connect(host=host, dbname=dbname, user=user, password=password)

def generate_random_data(fields, faker):
    """Genera un diccionario de datos aleatorios basado en los tipos especificados."""
    data = {}
    for field, dtype in fields.items():

        if field == "tps":
            # Obtener la fecha y hora actuales
            current_datetime = datetime.now()

            # Formatear la fecha y hora incluyendo segundos
            datetime_string = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            data[field] = datetime_string
            continue
        if dtype.startswith("VARCHAR") or dtype.startswith("TEXT"):
            data[field] = faker.word()
        elif dtype == "INTEGER":
            data[field] = random.randint(1, 100)
        elif dtype == "FLOAT":
            data[field] = round(random.uniform(1, 100), 2)
        elif dtype == "BOOLEAN":
            data[field] = random.choice([True, False])
        elif dtype == "DATE":
            data[field] = faker.date()
        elif dtype == "TIMESTAMP":
            data[field] = faker.date_time()
        elif dtype == "JSON" or dtype == "JSONB":
            data[field] = faker.json(data_columns={"key": "word", "value": "word"}, num_rows=1)
        else:
            raise ValueError(f"Unsupported data type: {dtype}")
    return data

def insert_data(conn, table_name, fields, tps):
    """Inserta datos aleatorios en la tabla a una velocidad especificada."""
    faker = Faker()
    interval = 1 / tps  # Intervalo en segundos entre transacciones
    while True:
        data = generate_random_data(fields, faker)
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = list(data.values())

        with conn.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)
            conn.commit()

        print(f"Inserted: {data}")
        time.sleep(interval)

def main():
    # Configuración de la base de datos
    db_config = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    }

    # Configuración de la tabla y campos
    table_name = "traceability_test_json"
    fields = {
        "transaction_type": "VARCHAR",
        "data_origin": "JSON",
        "data_destination": "JSON",
        "tps": "TIMESTAMP"
    }


    #-----------------------------------------------------------------------
    # Configuración de TPS
    # tps = 2000  # Transacciones por segundo
    #
    # # Conexión a la base de datos
    # conn = connect_to_db(**db_config)
    #
    # # Ejecutar la inserción de datos en un hilo separado
    # thread = Thread(target=insert_data, args=(conn, table_name, fields, tps))
    # thread.start()


    #-----------------------------------------------------------------------------
    # Configuración de TPS y hilos
    total_tps = 4000  # TPS objetivo
    num_threads = 40  # Número de hilos
    tps_per_thread = total_tps // num_threads  # TPS por hilo

    # Crear y arrancar los hilos
    threads = []
    for _ in range(num_threads):
        conn = connect_to_db(**db_config)
        thread = Thread(target=insert_data, args=(conn, table_name, fields, tps_per_thread))
        time.sleep(1/5)
        thread.start()
        threads.append(thread)

    # Mantener el programa principal corriendo
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
