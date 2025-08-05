import requests
from bs4 import BeautifulSoup
import logging
import time
import random

USER_AGENTS = [
    # Chrome en Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",

    # Firefox en Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0",

    # Safari en iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"

]


def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Accept-Encoding': 'gzip, deflate, br'
    }


def get_page(url, max_retries=3):
    """Obtiene el contenido HTML de una URL con manejo de errores avanzado"""
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers=get_headers(),
                timeout=15
            )

            # Detectar bloqueos basados en contenido
            if "unusual traffic" in response.text or "captcha" in response.text:
                logging.warning(f"Detectado posible bloqueo en {url}")
                raise RuntimeError("Blocked by CAPTCHA")

            # Verificar estado HTTP
            response.raise_for_status()
            return response.text

        except requests.exceptions.HTTPError as e:
            # No reintentar para errores 4xx (excepto 429)
            if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                logging.error(f"Error HTTP {e.response.status_code} no reintentable: {url}")
                return None
            logging.warning(f"Error HTTP {e.response.status_code} en intento {attempt + 1}/{max_retries}")

        except (requests.exceptions.RequestException, RuntimeError) as e:
            logging.warning(f"Error en intento {attempt + 1}/{max_retries}: {str(e)}")

        # Espera exponencial antes del reintento
        wait_time = 2 ** attempt + random.random()
        logging.info(f"Esperando {wait_time:.1f}s antes de reintentar")
        time.sleep(wait_time)

    logging.error(f"Error final al obtener {url} después de {max_retries} intentos")
    return None

