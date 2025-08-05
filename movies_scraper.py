import csv
import os
import time

import requests
from bs4 import BeautifulSoup
import re
from scraper import get_headers


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


def extraer_info_pelicula(url):
    headers = get_headers()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    data = {}

    # Título
    title_tag = soup.find('h1')
    if title_tag:
        data['titulo'] = title_tag.get_text(strip=True)

    # Año
    año_tag = soup.select_one('ul li span.ipc-inline-list__item')
    if año_tag:
        año = re.search(r'\d{4}', año_tag.text)
        if año:
            data['año'] = int(año.group())

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
    # print(f'{title_tag.get_text(strip=True)}, {actores}'
    #       f'{float(rating_tag.text.strip())}, {total_min}, {metascore}')
    return data

def procesar_peliculas_csv(input_csv='data/enlaces_peliculas.csv',
                            output_csv='data/detalle_peliculas.csv',
                            delay=1):
    """
    Lee un CSV de enlaces IMDb, extrae datos por película y guarda los resultados en un nuevo CSV.
    """
    resultados = []

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        n = 0
        for fila in reader:
            url = fila['Enlace']
            try:
                info = extraer_info_pelicula(url)
                info['url'] = url
                resultados.append(info)
                n += 1
                print(f"✅ {n} Procesado: {info.get('titulo', 'N/A')}")
            except Exception as e:
                print(f"❌ Error al procesar {url}: {e}")
            time.sleep(delay)

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
