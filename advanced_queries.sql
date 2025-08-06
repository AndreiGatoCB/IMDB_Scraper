SELECT * FROM actores;
SELECT * FROM peliculas;

-- Obtener las 5 películas con mayor promedio de duración por década.
WITH peliculas_con_decada AS (
    SELECT
        *,
        (anio / 10) * 10 AS decada
    FROM peliculas
),
ranking_duracion AS (
    SELECT
        id,
        titulo,
        anio,
        duracion_min,
        decada,
        RANK() OVER (PARTITION BY decada ORDER BY duracion_min DESC) AS posicion
    FROM peliculas_con_decada
)
SELECT
    id,
    titulo,
    anio,
    duracion_min,
    decada
FROM ranking_duracion
WHERE posicion <= 5
ORDER BY decada, posicion;


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


-- Detectar películas con más de un 20% de diferencia entre calificación IMDB y Metascore (normalizado).
SELECT
    id,
    titulo,
    calificacion,
    metascore,
    ROUND((metascore / 10.0)::numeric, 2) AS metascore_normalizado,
    ROUND(ABS(calificacion - metascore / 10.0)::numeric, 2) AS diferencia_absoluta,
    ROUND((ABS(calificacion - metascore / 10.0) / calificacion)::numeric * 100, 2) AS diferencia_porcentual
FROM peliculas
WHERE calificacion IS NOT NULL
  AND metascore IS NOT NULL
  AND (ABS(calificacion - metascore / 10.0) / calificacion) > 0.20
ORDER BY diferencia_porcentual DESC;


-- Crear una vista que relacione películas y actores, y permita filtrar por actor principal.
CREATE OR REPLACE VIEW vista_peliculas_actores_principales AS
SELECT * FROM (
    SELECT
        p.id AS pelicula_id,
        p.titulo,
        p.anio,
        p.calificacion,
        p.duracion_min,
        p.metascore,
        a.nombre AS actor_principal,
        ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY a.id) AS rn
    FROM peliculas p
    JOIN actores a ON a.pelicula_id = p.id
) sub
WHERE rn = 1;

SELECT * FROM vista_peliculas_actores_principales
WHERE actor_principal = 'Al Pacino';


-- Crear un índice o partición si se justifica para consultas frecuentes
CREATE INDEX idx_peliculas_anio ON peliculas(anio);
-- Obtener películas de la década de los 90
SELECT * FROM peliculas
WHERE anio BETWEEN 1990 AND 1999;

CREATE INDEX idx_peliculas_anio_calificacion ON peliculas(anio, calificacion);
-- Películas de los 90 con calificación mayor a 8
SELECT * FROM peliculas
WHERE anio BETWEEN 1990 AND 1999
  AND calificacion > 8;

CREATE INDEX idx_actores_pelicula_id ON actores(pelicula_id);
-- Listar actores de una película específica
SELECT a.nombre FROM actores a
JOIN peliculas p ON a.pelicula_id = p.id
WHERE p.id = 123;

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_actores_nombre_trgm ON actores USING gin(nombre gin_trgm_ops);
-- Buscar actores cuyo nombre contiene 'Tom'
SELECT * FROM actores
WHERE nombre ILIKE '%Tom%';

CREATE INDEX idx_peliculas_anio_calificacion ON peliculas(anio, calificacion);
-- Obtener top películas ordenadas por año y calificación
SELECT titulo, anio, calificacion
FROM peliculas
ORDER BY anio DESC, calificacion DESC
LIMIT 10;

SELECT
  schemaname,
  tablename,
  indexname,
  indexdef
FROM
  pg_indexes
WHERE
  tablename = 'peliculas';

SELECT pg_get_indexdef(indexrelid)
FROM pg_index
WHERE indexrelid = 'idx_peliculas_anio'::regclass;
