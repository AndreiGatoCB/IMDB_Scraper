import csv
import os
import random
import threading
from functools import wraps
from queue import Queue

from lxml import html
from lxml.html import fromstring
import requests
from bs4 import BeautifulSoup
import re
from scraper import get_headers, obtener_ip_publica, get_page, extraer_enlaces_imdb
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging
from config import use_proxies
load_dotenv()

TOP_URL = "https://www.imdb.com/chart/top/"
# Asegura que la carpeta de logs exista
os.makedirs("data", exist_ok=True)

# Configura el logging para consola + archivo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),  # consola
        logging.FileHandler("data/scraper.log", mode='w', encoding='utf-8')  # archivo
    ]
)

with open("data/proxies/valid_proxies.txt", "r") as f:
    proxies = f.read().split('\n')


def extraer_metascore_flexible(soup):
    """
    Extrae el metascore desde IMDb usando heurísticas cuando no tiene data-testid.
    """
    posibles_spans = soup.select('section span')
    for span in posibles_spans:
        texto = span.get_text(strip=True)
        if texto.isdigit():
            valor = int(texto)
            if 0 <= valor <= 100:
                # Heurística: si el padre o abuelo tiene otras pistas visuales
                padre = span.find_parent('li')
                if padre and 'Metascore' in padre.get_text():
                    return valor
    return None


def probar_conexion():
    ip = obtener_ip_publica()
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            dbname=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD")
        )
        logging.info(f"Great Conexión exitosa a PostgreSQL")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Fail Error al conectar: {e}")
        return False


def insertar_en_bd(func):
    @wraps(func)
    def wrapper(url, *args, **kwargs):
        # Ejecutar la función original
        data = func(url, *args, **kwargs)

        # Añadir URL a los datos para la inserción
        data['url'] = url
        ip = obtener_ip_publica()
        try:
            logging.debug(f'{os.environ.get("DB_HOST")}, {os.environ.get("DB_PORT")}, {os.environ.get("POSTGRES_DB")}, {os.environ.get("POSTGRES_USER")}, {os.environ.get("POSTGRES_PASSWORD")}')

            conn = psycopg2.connect(
                host=os.environ.get("DB_HOST"),
                port=os.environ.get("DB_PORT"),
                dbname=os.environ.get("POSTGRES_DB"),
                user=os.environ.get("POSTGRES_USER"),
                password=os.environ.get("POSTGRES_PASSWORD")
            )
            cur = conn.cursor()

            cur.execute(
                sql.SQL("""INSERT INTO peliculas (titulo, anio, calificacion, duracion_min, metascore, url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    RETURNING id;
                    """),
                (
                    data.get('titulo'),
                    data.get('año'),
                    data.get('calificacion'),
                    data.get('duracion_min'),
                    data.get('metascore'),
                    url
                )
            )

            # Obtener ID de la película insertada
            result = cur.fetchone()

            if result:
                pelicula_id = result[0]
                logging.info(f"Up PostgreSQL: Película insertada ID {pelicula_id}")

                # 2. Insertar actores relacionados
                actores = data.get('actores', [])
                for actor in actores:
                    cur.execute(
                        sql.SQL("""
                                        INSERT INTO actores (pelicula_id, nombre)
                                        VALUES (%s, %s)
                                    """),
                        (pelicula_id, actor)
                    )
                logging.info(f"Up Insertados {len(actores)} actores para película ID {pelicula_id}")

            conn.commit()
            logging.info(f"Up PostgreSQL: {data.get('titulo', 'N/A')}")
            conn.close()
        except psycopg2.Error as e:
            logging.error(f"Fail Error PostgreSQL: {e}")

        return data

    return wrapper


def crear_tabla_si_no_existe():
    """Crea la tabla si no existe"""
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD")
    )
    cur = conn.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS peliculas (
                    id SERIAL PRIMARY KEY,
                    titulo TEXT NOT NULL,
                    anio INTEGER,
                    calificacion FLOAT,
                    duracion_min INTEGER,
                    metascore INTEGER,
                    url TEXT UNIQUE NOT NULL,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

    # Crear tabla actores con relación a películas
    cur.execute("""
                CREATE TABLE IF NOT EXISTS actores (
                    id SERIAL PRIMARY KEY,
                    pelicula_id INTEGER NOT NULL REFERENCES peliculas(id) ON DELETE CASCADE,
                    nombre TEXT NOT NULL
                );
            """)
    conn.commit()
    cur.close()
    conn.close()


@insertar_en_bd
def extraer_info_pelicula(url):
    headers = get_headers()
    intentos_max = 5
    intento = 0
    while intento < intentos_max:
        proxy_idx = random.randint(0, len(proxies) - 1)
        proxy_actual = proxies[proxy_idx]
        try:
            if use_proxies:
                response = requests.get(
                    url,
                    headers=headers,
                    proxies={"http": proxy_actual,
                             "https": proxy_actual},
                    timeout=10
                )
            else:
                response = requests.get(
                    url,
                    headers=headers
                )

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")

            soup = BeautifulSoup(response.text, 'html.parser')
            tree = fromstring(response.text)
            data = {}

            # Título
            title_tag = soup.find('h1')
            if title_tag:
                data['titulo'] = title_tag.get_text(strip=True)
            ip = obtener_ip_publica()
            # año
            try:
                # El XPath que proporcionaste
                año_element = tree.xpath(
                    '//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/ul/li[1]/a')

                if año_element:
                    año_text = año_element[0].text.strip()
                    año_match = re.search(r'\d{4}', año_text)
                    if año_match:
                        data['año'] = int(año_match.group())
            except Exception as e:
                logging.warning(f"Error extrayendo año con XPath: {e}")
            # Calificación IMDb
            rating_tag = soup.select_one('[data-testid="hero-rating-bar__aggregate-rating__score"] span')
            if rating_tag:
                try:
                    data['calificacion'] = float(rating_tag.text.strip())
                except ValueError:
                    pass

            # Duración
            duracion_tag = soup.select_one('li[data-testid="title-techspec_runtime"]')
            if duracion_tag:
                duracion_text = duracion_tag.get_text(strip=True)
                horas = re.search(r'(\d+)h', duracion_text)
                minutos = re.search(r'(\d+)m', duracion_text)
                total_min = 0
                if horas:
                    total_min += int(horas.group(1)) * 60
                if minutos:
                    total_min += int(minutos.group(1))
                if total_min > 0:
                    data['duracion_min'] = total_min

            # Metascore (nuevo selector)
            metascore = extraer_metascore_flexible(soup)
            if metascore:
                try:
                    data['metascore'] = metascore
                except ValueError:
                    pass

            # Actores principales (nuevo selector, más confiable)
            actores = []
            credit_blocks = soup.select('li[data-testid="title-pc-principal-credit"]')

            for block in credit_blocks:
                if 'Stars' in block.text:
                    a_tags = block.select('a[href^="/name/"]')
                    for tag in a_tags:
                        nombre = tag.text.strip()
                        if nombre and nombre.lower() != "see more":
                            actores.append(nombre)
                        if len(actores) == 3:
                            break
                    break

            data['actores'] = actores
            data['url'] = url

            return data
        except Exception as e:
            logging.warning(f"Error en la solicitud: {e}")
            intento += 1

    logging.error(f"Fail Fallaron los {intentos_max} intentos para {url}")
    return None


def procesar_peliculas_csv(input_csv='data/enlaces_peliculas.csv',
                            output_csv='data/detalle_peliculas.csv',
                            delay=1):
    """
    Lee un CSV de enlaces IMDb, extrae datos por película y guarda los resultados en un nuevo CSV.
    """
    if not probar_conexion():
        return
    crear_tabla_si_no_existe()

    resultados = []
    resultados_lock = threading.Lock()
    tareas = Queue()

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, fila in enumerate(reader):
            if i >= 250:
                break
            tareas.put(fila['Enlace'])

    def trabajador():
        while not tareas.empty():
            url = tareas.get()
            try:
                info = extraer_info_pelicula(url)
                info['url'] = url
                with resultados_lock:
                    resultados.append(info)
                logging.info(f"Great Procesado: {info.get('titulo', 'N/A')}")
            except Exception as e:
                logging.warning(f"Fail Error al procesar {url}: {e}")
            tareas.task_done()

    # Crear y lanzar 10 hilos
    hilos = []
    for _ in range(10):
        t = threading.Thread(target=trabajador)
        t.start()
        hilos.append(t)

    for t in hilos:
        t.join()

    # Guardar resultados
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        campos = ['titulo', 'año', 'calificacion', 'duracion_min', 'metascore', 'actores', 'url']
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        for fila in resultados:
            # Serializar la lista de actores como string
            fila['actores'] = ', '.join(fila.get('actores', []))
            writer.writerow(fila)

    logging.info(f"Great Archivo generado: {output_csv}")
    logging.info(f"Done Total de películas procesadas: {len(resultados)}")


html = get_page(TOP_URL)
debug_path = os.path.join('data', 'imdb_debug.html')
with open(debug_path, 'w', encoding='utf-8') as f:
    f.write(html)
extraer_enlaces_imdb(debug_path)
procesar_peliculas_csv()