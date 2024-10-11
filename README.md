# README - ETL YouTube Trabajo Práctico - itba-cde-pda


## Descripción

Este proyecto tiene como objetivo crear un ETL que consume datos de las APIs de YouTube (suscriptores + videos) para obtener para los principales canales de Streaming de Argentina, información sobre videos, visualizaciones, comentarios, likes, duración y suscriptores de canales. Todo el código está contenido en la carpeta `airflow-etl`, y está dividido de forma que puedas entender claramente qué hace cada parte del proceso.
En el ETL se calculan adicionalmente dos métricas para medir engagement en los distintos canales. Ambas métricas entre más cercana a uno sean más engagement generan en la audiencia:

  1.	Likes por vistas (likes_per_view): Esta variable se obtiene dividiendo el número de likes por la cantidad total de visualizaciones. Es una métrica importante porque nos da una idea de cuántas personas que vieron el video se tomaron el tiempo de darle “like”, lo que refleja cuán bien ha sido recibido el contenido por la audiencia.
	2.	Comentarios por vistas (comments_per_view): Esta métrica se obtiene dividiendo el número de comentarios por la cantidad total de visualizaciones. Los comentarios son aún interacciones más profundas porque el usuario se toma tiempo para redactar o reflexionar algo sobre el video por más que sea positivo o negativo.

El siguiente diagrama describe el funcionamiento

<img width="983" alt="image" src="https://github.com/user-attachments/assets/ce155788-b8b1-40fb-b934-9bd4df0418d6">


Las tablas finales son dos:
  - `BT_YOUTUBE_VIDEO_STATS` : contiene todos los videos de un canal de youtube de Streaming Argentino y su cantidad de visualizaciones durante los primeros 7 días de creado. 
  - `LG_CHANNEL_SUBSCRIBERS` : contiene un log de la cantidad de suscriptores a un día determinado para todos los canales principales de Streaming Argentino

![image](https://github.com/user-attachments/assets/11e19d99-2f86-4572-ac68-c893de7327ab)

![image](https://github.com/user-attachments/assets/37799d9f-ea94-49b4-bc77-0e4b592a2855)


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
   - Primero se carga la raw data a una tabla de Staging 'youtube_videos_stg' y 'youtube_subscribers_stg' respectivamente.
   - Luego actualiza las tablas finales a través de un UPDATE e INSERT en SQL. El update lo hacemos solo en la tabla de videos para todos los videos creados en los ultimos 7 días. En el caso de los suscriptores, tenemos una foto de la cantidad de suscriptores por día.   
---

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
Si se va a ejecutar con Airflow hay que levantar el contenedor:
```bash
docker-compose up -d
```


### 5. Tests

Se pueden ejecutar los tests que :

```bash
poetry run pytest
```


