# Scraper IMDB - Prueba Técnica

## Objetivo

Extraer información de las top 50 películas de IMDB implementando:
- [x] Scraper robusto con BeautifulSoup
- [x] Sistema de rotación de proxies
- [x] Persistencia en PostgreSQL
- [x] Análisis SQL avanzado



## Estructura de archivos
### Módulo de scraping

Archivo | Función | Dependencias
---|---|---
`scraper.py` | Funciones base de scraping | `requests`, `BeautifulSoup`, `csv`, `logging`
`movie_scraper.py` | Scraper principal (multi-hilo + PostgreSQL) | `psycopg2`, `dotenv`, `threading`, `Queue`
`config.py` | Control de uso de proxies | n/a

### Datos & Configuración

Carpeta/Archivo | Contenido
--- | ---
`data/detalle_películas.csv` | Dataset completo de películas
`data/enlace_películas.csv` | URLs del to 250
`data/imdb_debug.html` | HTML local de TOP 250
`data/scraper.log` | Registro detallado de ejecución
`.env` | Variables de entorno para PostgreSQL

### SQL & Análisis

Archivo | Contenido
--- | ---
`advanced_queries.sql` | Consultas SQL avanzadas y optimizaciones.

### Pruebas

Archivo | Contenido
--- | ---
`tests/test_scraper.py` | Pruebas unitarias para funciones de scraping.

### Gestión de proxies

Archivo | Contenido
--- | ---
`check_proxies.py` | Validación de proxies multi-hilo


### Funcionalidades Clave

1. Scraping Robusto: `scraper.py`
   1. Rotación automática de User-Agents
   2. Detección de bloqueos de CAPTCHA
   3. Sistema de reintentos con backoff
   4. Extracción de:
      - Títulos
      - Año de producción
      - Calificación IMDB
      - Duración en minutos
      - Metascores (cuando los hay)
      - Actores principales
2. Gestión de proxies: `check_proxies.py`
   1. Validación concurrente con 50 hilos
   2. Prueba de conectividad a `http://ipinfo.io/json`
   3. Salida de proxies válidos en consola
3. Persistencia en PostgreSQL: `movie_scraper.py`
   1. Conexión mediante variables de entorno `.env` a base de datos levantada en Docker.
   2. Inserción en dos tablas relacionadas:
      ````sql
      peliculas(id, titulo, anio, calificacion, duracion_min, metascore, url)
      actores(id, pelicula_id, nombre)
      ````
   3. Decorador `@insertar_en_db` para inserción automática
4. Consultas analíticas: `advanced_queries.sql`
   1. Top 5 películas más largas por década
   2. Desviación estandar de calificaciones por año
   3. Detección de discrepancias IMDB/Metascore (>20%)
   4. Vista de actores principales
   5. Optimización con índices:
      - `idx_peliculas_anio`
      - `idx_peliculas_decada_calificacion`
      - `idx_actores_pelicula_id`
      - `idx_actores_nombre_trgm`
      - `idx_peliculas_anio_calificacion`
5. Pruebas unitarias: `tests/test_scraper.py`
   - validación de generación de headers.
   - Simulación de respuestas HTTP (200, 400, 500)
   - Pruebas de detección de CAPTCHAS
   - Tests para extracción de enlaces
   - Manejo de timeouts y reintentos

### Ejecución

#### Requisitos previos

##### Levantamiento del contenedor en docker

1. En una terminal correr los siguientes comandos:

   ```$powershell
   docker run --name imdb_postgres_container -e POSTGRES_PASSWORD=admin -p 5431:5432 -d postgres
   docker ps -a
   docker exec -it imdb_postgres_container psql -U postgres
   ```
   - El primero corre el contenedor de docker con la contraseña dada.
   - El segundo muestra una tabla con los contenedores activos.
   - El tercero ejecuta postgresql en la terminal y nos permite asegurarnos de que la base de datos es funcional.

2. Escribir el documento `.env` y ubicarlo en la carpeta raíz

3. Flujo completo
   
   1. Validar proxies (opcional)
      - Es preferible usar proxies personalizados, estos deben agregarse en el archivo `data/proxies/valid_proxies.txt`
      - `python check_proxies.py`
   2. Ejecutar scraper principal
      - `python movie_scraper.py` _El tiempo de ejecución es de aproximadamente 1 minuto y 43 segundos._
      - Genera el archivo `data/imdb_debug.html` descargándo la página web completa del _top 250 de IMDB_
      - Genera el archivo `data/enlace_peliculas.csv` con todos los enlaces del _top 250 de IMDB_
      - Conecta en tiempo real la base de datos y la función que extrae datos película a película, lo que permite la población de la base de datos en postgreSQL durante la ejecución del archivo
      - Genera en tiempo real el archivo `data/scraper.log` con todas las ejecuciones de la relación entre el programa y la base de datos en postgreSQL
      - Por último genera el archivo `data/detalle_peliculas.csv` con toda la información relevante de cada película.
   3. Ejecutar pruebas unitarias
      `pytest tests/`
   4. Ejecutar consultas SQL
      - Ejecutar cada consulta dentro del archivo `advanced_queries.sql` por separado

## Salidas Generadas

`data/detalle_peliculas.csv`
````csv
titulo,año,calificacion,duracion_min,metascore,actores,url
The Shawshank Redemption,1994,9.3,142,82,"Tim Robbins, Morgan Freeman, Bob Gunton",https://www.imdb.com/title/tt0111161/
The Godfather Part II,1974,9.0,202,90,"Al Pacino, Robert De Niro, Robert Duvall",https://www.imdb.com/title/tt0071562/
12 Angry Men,1957,9.0,96,97,"Henry Fonda, Lee J. Cobb, Martin Balsam",https://www.imdb.com/title/tt0050083/
The Lord of the Rings: The Fellowship of the Ring,2001,8.9,178,92,"Elijah Wood, Ian McKellen, Orlando Bloom",https://www.imdb.com/title/tt0120737/
The Dark Knight,2008,9.1,152,85,"Christian Bale, Heath Ledger, Aaron Eckhart",https://www.imdb.com/title/tt0468569/
````

`data/scraper.log`
````shell
2025-08-06 19:29:27,387 [INFO] Great Conexión exitosa a PostgreSQL
2025-08-06 19:29:30,504 [INFO] Up PostgreSQL: Película insertada ID 1
2025-08-06 19:29:30,510 [INFO] Up Insertados 3 actores para película ID 1
2025-08-06 19:29:30,518 [INFO] Up PostgreSQL: The Shawshank Redemption
2025-08-06 19:29:30,518 [INFO] Great Procesado: The Shawshank Redemption
````

## Configuración de Proxies
Modificar `config.py` para activar o desactivar los proxies:

````python
use_proxies = False # Cambiar a True para usar proxies
````

> Los proxies válidos se deben encontrar en `data/proxies/valid_queries.txt` uno por línea.

## Resultados SQL

Ejecutar consultas directamente en PostgreSQL:

````SQL
-- Calcular la desviación estándar de las calificaciones por año.
SELECT 
    anio,
    COUNT(*) AS cantidad_peliculas,
    ROUND(AVG(calificacion)::numeric, 2) AS promedio_calificacion,
    ROUND(STDDEV_SAMP(calificacion)::numeric, 2) AS desviacion_estandar
FROM peliculas
WHERE calificacion IS NOT NULL
GROUP BY anio
ORDER BY anio;
````

![Desviación estándar](imagenes/desviacion_estandar.jpg Desviación estándar)

## Ejecución de pruebas

Esta sección se ampliará en próximas ediciones testeando todas las funciones del proyecto.

![Tests](imagenes/tests.jpg Tests)

## Consideraciones técnicas

1. El scraper accede únicamente a rutas permitidas por IIMDB
2. Delay entre requests: 1 segundo por defecto
3. Límite de películas: La cantidad de películas que se encuentren en la página, en este caso 250
4. Manejo de errores: Reintentos automáticos.
5. Persistencia dual: CSV + PostgreSQL
6. La lista de proxies la extraje de [PROXY-List](https://github.com/TheSpeedX/PROXY-List)

## Comparación técnica: Playwright/Selenium vs Requests/BeautifulSoup

1. Configuración avanzada del navegador:
   - Ambos tienen la posibilidad de ejecutarse en segundo plano sin interfaz gráfica.
   - Playwright y Selenium tienen headers personalizados que permiten un control granular sobre el user-agent, las 
   cookies y los refers, mientras que Requests y BS4 manejan headers básicos, simplemente el user-agent rotativo.
   - En cuanto a la evasión de detección esta sólo la tienen Playright y Selenium, lo que les permite interactuar 
   directamente con la página web en vivo. Requests y bs4 no lo permiten, por esto fue necesario descargar el html de la
   página del top 250.
   - Playwright y selenium tienen la capacidad de emular dispositivos móviles, geolocalizaciones e idiomas, Requests y 
   BS4 no tienen esta capacidad corriendo mayor riesgo de ser detectados como bots.
2. Selectores dinámicos
   - PW/S tienen esperas inteligentes que R/BS4 no permite ya que sólo usa HTML estático
   - PW/S permite interactuar con estados y puede esperar hasta que un elemento sea visible o clickeable. R/BS4 Requiere
   un análisis de redes para contenido ajax a modo de reconstrucción manual.
   - PW/W tiene selectores relativos y condicionales que permiten el uso de XPath avanzado, por otro lado R/BS4 es más 
   frágil en este aspecto porque un mínimo cambio puede romper selectores fácilmente.
3. Manejo de CAPTCHAS/JS
   - PW/S ejecutan todo el Javascript como navegador real, capacidad que no tiene R/BS4
   - PW/S permiten interactuar con CAPTCHAS, ya sea por medio de APIs como 2Captcha o con resolución manual, R/BS4 no 
   tiene esta capacidad.
   - PW/S pueden interactuar en tiempo real con la página web haciendo clicks, arrastrándola y escribiendo en campos de 
   esta. Por otro lado R/BS4 no lo permiten. Y su única defensa para evitar ser identificado es rotando proxies.
4. Control de concurrencia
   - PW/S tienen navegadores aislados que le permite tener contextos independientes por worker, R/BS4 sólo puede tomar 
   la información de páginas estáticas una a la vez.
   - En PW/S cada instancia consume hasta 300MB de RAM, en R/BS4 consume aproximadamente 10MB por hilo.
   - PW/S tienen colas complejas y gestión manual de sesiones persistentes. Mientras que R/BS4 maneja con 
   `threading.Queue` las queue de manera simple.
5. Justificación de uso
   - Playwright/Selenium se usan en sitios con carga dinámica, cuando se requiere interacción con la página web, para 
   aplicaciones web complejas como SPAs y WebSockets o cuando el sitio bloquea IPs de manera sistemática.
   - Requests/BS4 Se usa en sitios en los que todos los datos iniciales están en HTML.

__Por qué Requests/BS4 sobre Playwright/Selenium__
   
Si bien con selenium hubiera podido tomar los enlaces de las películas directamente desde la página web al permitirme
scrollear en la pagina web cuando no pudiera pasar de las primeras 25 películas el poder descargar el html facilitó 
mucho más el scraping de no sólo las 50 películas solicitadas sino de todas las películas del __top250__ Procesa las 250
películas en menos de dos minutos y entrega todos los CSVs y la base de datos poblada en tiempo real. Requests y BS4 
tienen un código más simple permitiendo todo su desarrollo en menos de 300 líneas que en playwright/selenium serían más 
de 500. Se utilizan 90% menos de recursos de CPU y Memoria.

Playwright y selenium sólo serían necesarios si IMDB implementara carga dinámica de actores o calificaciones, necesitara
interacciones para ver detalles completos o se protegiera sistemáticamente con CAPTCHAS complejos.
