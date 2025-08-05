import csv
import sys
import os
import tempfile

import pytest
import requests
import logging
from unittest.mock import patch
from scraper import get_headers, get_page, USER_AGENTS, extraer_enlaces_imdb
import responses

# Configurar path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Configurar logging para pruebas
logging.basicConfig(level=logging.DEBUG)

def test_get_headers_structure():
    """Verifica que get_headers() retorna un diccionario con las claves correctas"""

    headers = get_headers()

    assert isinstance(headers, dict)
    assert 'User-Agent' in headers
    assert 'Accept-Language' in headers
    assert 'Referer' in headers
    assert 'DNT' in headers
    assert 'Accept-Encoding' in headers


def test_get_headers_randomization():
    """Comprueba que el User-Agent rota aleatoriamente entre
    diferentes opciones predefinidas."""
    agents = set()

    # Probamos 20 veces para asegurar aleatoriedad
    for _ in range(20):
        headers = get_headers()
        agents.add(headers['User-Agent'])

    # Deberíamos tener al menos 2 agentes diferentes
    assert len(agents) >= 2
    assert all(agent in USER_AGENTS for agent in agents)


def test_get_headers_values():
    """Valida que los valores de los headers no aleatorios
    sean correctos y consistentes."""
    headers = get_headers()

    assert headers['Accept-Language'] == 'en-US,en;q=0.9'
    assert headers['Referer'] == 'https://www.google.com/'
    assert headers['DNT'] == '1'
    assert headers['Accept-Encoding'] == 'gzip, deflate, br'


@pytest.mark.parametrize("status_code", [200, 201, 202])
def test_get_page_success(status_code):
    """Prueba que get_page() maneja correctamente respuestas
     HTTP exitosas (códigos 200-202)."""
    test_url = "https://test-success.com"
    test_content = "<html>Success!</html>"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            test_url,
            body=test_content,
            status=status_code
        )

        result = get_page(test_url)

        assert result == test_content
        assert len(rsps.calls) == 1
        assert rsps.calls[0].request.headers['User-Agent'] in USER_AGENTS


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
def test_get_page_client_errors(status_code):
    """Verifica que get_page() retorna None ante errores
    cliente HTTP (400-404) sin reintentos."""
    test_url = f"https://test-error-{status_code}.com"

    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, test_url, status=status_code)
        result = get_page(test_url, max_retries=1)

        assert result is None
        assert len(rsps.calls) == 1


def test_get_page_server_error():
    """Comprueba que get_page() reintenta y falla ante
    errores de servidor (500)."""
    test_url = "https://test-server-error.com"

    with responses.RequestsMock() as rsps:
        # Simular 3 errores 500 (se reintentará 3 veces)
        for _ in range(3):
            rsps.add(responses.GET, test_url, status=500)

        result = get_page(test_url)
        assert result is None
        assert len(rsps.calls) == 3


def test_get_page_blocked_detection():
    """Valida que get_page() detecta bloqueos CAPTCHA por
    contenido y reintenta."""
    test_url = "https://test-blocked.com"
    blocked_content = "We've detected unusual traffic from your network"

    with responses.RequestsMock() as rsps:
        for _ in range(3):
            rsps.add(responses.GET, test_url, body=blocked_content, status=200)

        result = get_page(test_url)
        assert result is None
        assert len(rsps.calls) == 3


def test_get_page_timeout():
    """Prueba que get_page() maneja correctamente errores de
     timeout en conexiones."""
    test_url = "https://test-timeout.com"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            test_url,
            body=requests.exceptions.ConnectTimeout()
        )

        result = get_page(test_url)

        assert result is None


def test_get_page_retry_behavior():
    """Verifica el comportamiento de reintento exitoso tras
    un fallo inicial."""
    test_url = "https://test-retry.com"

    with responses.RequestsMock() as rsps:
        # Primera llamada: error
        rsps.add(
            responses.GET,
            test_url,
            body=requests.exceptions.ConnectionError()
        )
        # Segunda llamada: éxito
        rsps.add(
            responses.GET,
            test_url,
            body='<html>Success after retry</html>',
            status=200
        )

        result = get_page(test_url, max_retries=2)
        assert result == '<html>Success after retry</html>'
        assert len(rsps.calls) == 2


def test_get_page_headers_in_request():
    """Comprueba que los headers personalizados se envían en
     las peticiones."""
    test_url = "https://test-headers.com"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            test_url,
            body="headers-test"
        )

        get_page(test_url)

        request_headers = rsps.calls[0].request.headers

        assert request_headers['Accept-Language'] == 'en-US,en;q=0.9'
        assert request_headers['Referer'] == 'https://www.google.com/'
        assert request_headers['DNT'] == '1'
        assert request_headers['Accept-Encoding'] == 'gzip, deflate, br'
        assert request_headers['User-Agent'] in USER_AGENTS


def test_get_page_logging(caplog):
    """Valida que los errores durante el scraping se
    registran correctamente en los logs."""
    test_url = "https://test-logging.com"

    with responses.RequestsMock() as rsps:
        for _ in range(3):
            rsps.add(responses.GET, test_url, status=500)

        with caplog.at_level(logging.WARNING):
            get_page(test_url)

            # Verificar mensajes de reintento
            assert any("500" in record.message for record in caplog.records)
            assert any("Error HTTP 500" in record.message for record in caplog.records)

            # Verificar error final
            assert any("Error final al obtener" in record.message for record in caplog.records)
            assert test_url in caplog.text


def test_extraer_enlaces_imdb_crea_csv_correctamente():
    # Crear HTML simulado con 3 enlaces
    html_fake = '''
    {
      "@type":"ListItem",
      "position":1,
      "url":"https://www.imdb.com/title/tt0111161/"
    },
    {
      "@type":"ListItem",
      "position":2,
      "url":"https://www.imdb.com/title/tt0068646/"
    },
    {
      "@type":"ListItem",
      "position":3,
      "url":"https://www.imdb.com/title/tt0468569/"
    }
    '''

    # Crear archivos temporales
    with tempfile.TemporaryDirectory() as tmpdir:
        html_path = os.path.join(tmpdir, 'fake_imdb.html')
        csv_path = os.path.join(tmpdir, 'enlaces_peliculas.csv')

        # Guardar el HTML falso
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_fake)

        # Ejecutar la función
        total = extraer_enlaces_imdb(html_path, csv_path)

        # Leer el CSV y validar contenido
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = list(csv.reader(f))

        # Encabezado + 3 filas
        assert total == 3
        assert len(reader) == 4  # 1 header + 3 líneas
        assert reader[0] == ['Posición', 'Enlace']
        assert reader[1][1] == 'https://www.imdb.com/title/tt0111161/'
        assert reader[3][0] == '3'


def test_extraer_enlaces_imdb_sin_resultados():
    html_fake = '''
    <html>
        <head><title>Sin películas</title></head>
        <body><p>No hay nada acá.</p></body>
    </html>
    '''

    with tempfile.TemporaryDirectory() as tmpdir:
        html_path = os.path.join(tmpdir, 'sin_peliculas.html')
        csv_path = os.path.join(tmpdir, 'enlaces_peliculas.csv')

        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_fake)

        total = extraer_enlaces_imdb(html_path, csv_path)

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = list(csv.reader(f))

        assert total == 0
        assert len(reader) == 1
        assert reader[0] == ['Posición', 'Enlace']


