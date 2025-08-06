import csv
import os
from functools import wraps
from lxml import html
import requests
from bs4 import BeautifulSoup
import re
from scraper import get_headers
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

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
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            dbname=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD")
        )
        print("✅ Conexión exitosa a PostgreSQL")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error al conectar: {e}")
        return False


def insertar_en_bd(func):
    @wraps(func)
    def wrapper(url, *args, **kwargs):
        # Ejecutar la función original
        data = func(url, *args, **kwargs)

        # Añadir URL a los datos para la inserción
        data['url'] = url

        try:
            print(
                f'{os.environ.get("DB_HOST")}, {os.environ.get("DB_PORT")}, {os.environ.get("POSTGRES_DB")}, {os.environ.get("POSTGRES_USER")}, {os.environ.get("POSTGRES_PASSWORD")}')

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
                print(f"↑ PostgreSQL: Película insertada ID {pelicula_id}")

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
                print(f"↑ Insertados {len(actores)} actores para película ID {pelicula_id}")

            conn.commit()
            print(f"↑ PostgreSQL: {data.get('titulo', 'N/A')}")
            conn.close()
        except psycopg2.Error as e:
            print(f"❌ Error PostgreSQL: {e}")

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
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    tree = html.fromstring(str(soup))
    data = {}

    # Título
    title_tag = soup.find('h1')
    if title_tag:
        data['titulo'] = title_tag.get_text(strip=True)

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
        print(f"Error extrayendo año con XPath: {e}")
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
    print(data)
    return data

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

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        n = 0

        for fila in reader:
            while n < 250:
                url = fila['Enlace']
                try:
                    info = extraer_info_pelicula(url)
                    info['url'] = url
                    resultados.append(info)
                    n += 1
                    print(f"✅ {n} Procesado: {info.get('titulo', 'N/A')}")
                except Exception as e:
                    print(f"❌ Error al procesar {url}: {e}")
                # time.sleep(delay)

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

    print(f"\n📄 Archivo generado: {output_csv}")
    print(f"🎬 Total de películas procesadas: {len(resultados)}")


procesar_peliculas_csv()
