# Scraper IMDB - Prueba Técnica

## Objetivo

Extraer información de las top 50 películas de IMDB implementando:
- [x] Scraper robusto con BeautifulSoup
- [ ] Sistema de rotación de proxies
- [x] Persistencia en PostgreSQL
- [ ] Análisis SQL avanzado


## Levantamiento del contenedor en docker

En una terminal correr los siguientes comandos:
```$powershell
docker run --name imdb_postgres_container -e POSTGRES_PASSWORD=admin -p 5431:5432 -d postgres
docker ps -a
docker exec -it imdb_postgres_container psql -U postgres
```

- El primero corre el contenedor de docker con la contraseña dada.
- El segundo muestra una tabla con los contenedores activos.
- El tercero ejecuta postgresql en la terminal y nos permite asegurarnos de que la base de datos es funcional.