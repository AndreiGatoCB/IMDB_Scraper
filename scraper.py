import csv
import logging
import os
import time

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import random
import re

USER_AGENTS = [
    # Chrome en Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",

    # Firefox en Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0",

    # Safari en iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
    "Mobile/15E148 Safari/604.1"

]
TOP_URL = "https://www.imdb.com/chart/top/"
os.makedirs('data', exist_ok=True)


def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Accept-Encoding': 'gzip, deflate, br'
    }


def get_page(url, max_retries=3, delay=1):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=get_headers(), timeout=10)
            if response.status_code in {200, 201, 202}:
                content = response.text

                # Detección de bloqueo tipo CAPTCHA o tráfico inusual
                if "unusual traffic" in content or "captcha" in content.lower():
                    logging.warning(f"[{attempt}] Posible bloqueo por tráfico inusual en {url}")
                    time.sleep(delay)
                    continue

                return content
            else:
                logging.warning(f"[{attempt}] Error HTTP {response.status_code} al acceder a {url}")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            logging.warning(f"[{attempt}] Excepción al acceder a {url}: {e}")

        time.sleep(delay)

    logging.error(f"Error final al obtener {url} tras {max_retries} intentos")
    return None


def extraer_enlaces_imdb(html_path, output_csv_path='data/enlaces_peliculas.csv'):
    """
    Extrae enlaces de películas desde un archivo HTML de IMDb y los guarda con su posición en un CSV.
    """
    # Cargar el archivo HTML
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Buscar todos los enlaces de películas
    urls = re.findall(r'"url":"(https://www\.imdb\.com/title/tt\d+/)"', content)

    # Eliminar duplicados manteniendo el orden
    urls = list(dict.fromkeys(urls))

    # Guardar en CSV
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Posición', 'Enlace'])
        for i, url in enumerate(urls, start=1):
            writer.writerow([i, url])

    print(f"Se guardaron {len(urls)} enlaces en '{output_csv_path}'")
    return len(urls)


html = get_page(TOP_URL)
debug_path = os.path.join('data', 'imdb_debug.html')
with open(debug_path, 'w', encoding='utf-8') as f:
    f.write(html)
extraer_enlaces_imdb(debug_path)


