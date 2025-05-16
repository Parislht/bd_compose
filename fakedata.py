import mysql.connector
import psycopg2
from faker import Faker
import random
from datetime import datetime
from pymongo import MongoClient

fake = Faker()

# --- Configuración conexiones ---

# MySQL (microservicio 1 - usuarios)
mysql_config = {
    'host': 'localhost',  # O IP privada MV
    'port': 3307,         # puerto mapeado para MySQL
    'user': 'root',
    'password': 'utecmysql',
    'database': 'usuariosdb'
}

# PostgreSQL (microservicio 2 - libros)
postgres_config = {
    'host': 'localhost',  # O IP privada MV
    'port': 5432,         # puerto mapeado para PostgreSQL
    'user': 'postgres',
    'password': 'postgres',
    'dbname': 'libros_db'
}

# MongoDB (microservicio 3 - reseñas)
mongo_uri = "mongodb://localhost:27017"
mongo_db_name = "resenas_db"

# --- Conexiones ---
mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

pg_conn = psycopg2.connect(**postgres_config)
pg_cursor = pg_conn.cursor()

mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]
reviews_collection = mongo_db['reviews']

# --- Microservicio 1: Poblar usuarios y roles en MySQL ---

# Crear roles fijos (si no están creados)
roles = ['admin', 'user', 'moderator']
mysql_cursor.execute("SELECT COUNT(*) FROM rol")
if mysql_cursor.fetchone()[0] == 0:
    for rol in roles:
        mysql_cursor.execute("INSERT INTO rol (nombre_rol) VALUES (%s)", (rol,))
    mysql_conn.commit()

mysql_cursor.execute("SELECT id_rol FROM rol")
roles_ids = [r[0] for r in mysql_cursor.fetchall()]

print("Insertando 20,000 usuarios con roles en MySQL...")

for _ in range(20000):
    nombre = fake.first_name()
    apellido = fake.last_name()
    correo = fake.unique.email()
    fecha_registro = fake.date_between(start_date='-2y', end_date='today')
    n_resena = random.randint(0, 10)
    n_prestamo = random.choice([0, 1])  # <=1 préstamo activo

    mysql_cursor.execute("""
        INSERT INTO usuario (nombre, apellido, correo, fecha_registro, n_resena, n_prestamo)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (nombre, apellido, correo, fecha_registro, n_resena, n_prestamo))
    id_usuario = mysql_cursor.lastrowid

    # Asignar 1 o 2 roles aleatorios
    user_roles = random.sample(roles_ids, random.randint(1, 2))
    for id_rol in user_roles:
        mysql_cursor.execute("INSERT INTO usuario_rol (id_usuario, id_rol) VALUES (%s, %s)", (id_usuario, id_rol))

mysql_conn.commit()
print("Usuarios insertados correctamente.")

# --- Microservicio 2: Poblar libros en PostgreSQL ---

# Asumimos que editorial y categoria tienen IDs del 1 al 10 ya creados

print("Insertando 20,000 libros en PostgreSQL...")

for _ in range(20000):
    titulo = fake.sentence(nb_words=4).rstrip('.')
    anio_publicacion = random.randint(1950, 2025)
    disponible = random.choice([True, False])
    puntuacion = random.randint(1, 5)
    id_categoria = random.randint(1, 10)
    id_editorial = random.randint(1, 10)

    pg_cursor.execute("""
        INSERT INTO libro (titulo, anio_publicacion, disponible, puntuacion, id_categoria, id_editorial)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (titulo, anio_publicacion, disponible, puntuacion, id_categoria, id_editorial))

pg_conn.commit()
print("Libros insertados correctamente.")

# --- Microservicio 3: Poblar reseñas en MongoDB ---

print("Insertando 20,000 reseñas en MongoDB...")

for _ in range(20000):
    id_user = random.randint(1, 20000)  # asumimos que usuarios tienen ids de 1 a 20000
    id_book = random.randint(1, 20000)  # idem para libros
    comment = fake.text(max_nb_chars=200)
    punctuation = random.randint(1, 5)
    date = fake.date_time_between(start_date='-2y', end_date='now')

    review = {
        "id_user": id_user,
        "id_book": id_book,
        "comment": comment,
        "punctuation": punctuation,
        "date": date
    }

    reviews_collection.insert_one(review)

print("Reseñas insertadas correctamente.")

# --- Cerrar conexiones ---

mysql_cursor.close()
mysql_conn.close()

pg_cursor.close()
pg_conn.close()

mongo_client.close()
