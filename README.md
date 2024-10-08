# README - ETL de YouTube itba-cde-pda


## Instrucciones para reproducir el proceso

### 1. Clonar el repo

```bash
git clone https://github.com/fsantoliquido/itba-cde-pda.git
```

### 2. Instalar las dependencias con poetry

En este proyecto decidí utilizar **Poetry** para gestionar las dependencias. Hay que instalar las dependencias del file de poetry
```bash
poetry install
```

### 3. Configurar credenciales

- Las credenciales para las APIs de YouTube y para Redshift serán compartidas por Slack por separado.
- Se tienen que configurar las variables de entorno necesarias para las conexiones a las APIs y Redshift. Un ejemplo de estas variables puede ser:

```bash
export YOUTUBE_API_KEY=api-compartida
export REDSHIFT_ENDPOINT=endpoint-compartido
export REDSHIFT_USER=usuario-compartido
export REDSHIFT_PASSWORD=password-compartido
export REDSHIFT_DATABASE=database-compartida
```

### 4. Ejecutar el ETL

Con esto se puede ejecutar desde **Airflow**, pero también se puede ejecutar con **Poetry**:

```bash
poetry run python airflow-etl/module_etl/etl.py
```

### 5. Tests

Se pueden ejecutar los tests que :

```bash
poetry run pytest
```


## Descripción

Este proyecto tiene como objetivo crear un ETL que consume datos de las APIs de YouTube (suscriptores + videos) para obtener información sobre videos, visualizaciones, comentarios, likes y suscriptores de canales. Todo el código está contenido en la carpeta `airflow-etl`, y está dividido de forma que puedas entender claramente qué hace cada parte del proceso.

El script `etl.py` es el encargado de ejecutar toda la funciones que hacen la extracción de datos de YouTube y hacer las transformaciones necesarias para luego cargarlos a una base de datos Redshift. Las funciones que utiliza este archivo están organizadas en `utils.py`, para mantener el código limpio y prolijo.


---

## Estructura del repositorio

Dentro de la carpeta `airflow-etl` encontrarás:

- **dags/**: Tiene la DAG para orquestar las tareas en Airflow.
- **module_etl/**:
  - **etl.py**: El archivo principal del proceso ETL, donde se conectan las APIs de YouTube, se extraen los datos, se transforman, se crean variables nuevas y luego se cargan en Redshift con la función `upload_to_redshift`.
  - **utils.py**: Este archivo tiene todas las funciones auxiliares que invoca `etl.py` para hacer consultas a las APIs, calcular métricas y gestionar la conexión a la base de datos.
- **sql/queries.sql**: Contiene los `upserts` para mover los datos de las tablas de staging a las tablas finales en Redshift.
- **tests/**: En esta carpeta está el file `test_youtube_api.py` que tiene tres test para probar que elfuncionamiento y estructura de respuesta de las funciones que extraen datos de las APIs. <

---

## Proceso ETL

1. **Extracción**: 
   - `etl.py` se conecta a dos APIs de YouTube: youtube.search() y  youtube.videos(): una para obtener información sobre los canales y la cantidad de suscriptores, y otra para traer los videos y sus visualizaciones.
   - Extrae información como título del video, cantidad de visualizaciones, likes, comentarios, duración del video

2. **Transformación**:
   - Se calculan métricas adicionales como **likes per view** y **comments per view** para medir el **engagement** de los videos.
   - Se consolidan los datos de videos y suscriptores.

3. **Carga**:
   - Los datos procesados se cargan en tablas de staging en Redshift, y luego, mediante las consultas SQL en `queries.sql`, se realiza el `upsert` de los datos a las tablas finales.

---
