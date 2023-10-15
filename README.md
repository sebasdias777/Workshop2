# Workshop 2

Este proyecto se enfoca en el procesamiento, transformación y análisis de datos de los Premios Grammy y canciones de Spotify. Utiliza Apache Airflow para automatizar el flujo de trabajo ETL y se apoya en diversas herramientas, como PostgreSQL, Python, Visual Studio Code, Power BI y Jupyter Notebook.

## Objetivo

El objetivo principal de este proyecto es realizar un análisis de datos de canciones que han ganado premios Grammy . Para ello, se combinan dos conjuntos de datos:

- `the_grammy_awards.csv`: Contiene información sobre los premios Grammy, incluyendo detalles sobre las canciones ganadoras.
- `spotify_dataset.csv`: Contiene información sobre canciones de Spotify, como su popularidad, artistas, géneros, etc.

El proceso ETL consiste en extraer los datos, transformarlos para adaptarlos a nuestras necesidades de análisis, fusionarlos y cargarlos en una base de datos PostgreSQL.

## Herramientas Utilizadas

- **Apache Airflow**: Se utiliza para orquestar el flujo de trabajo ETL.
- **PostgreSQL**: La base de datos relacional se emplea para almacenar los datos transformados.
- **Python**: Se utiliza para escribir scripts de transformación y manipulación de datos.
- **Visual Studio Code**: Se utiliza como entorno de desarrollo para escribir y depurar código.
- **Power BI**: Se emplea para crear visualizaciones y dashboards interactivos.
- **Jupyter Notebook**: Se usa para realizar análisis exploratorio de datos y documentar el proceso.

## Instrucciones de Uso

1. Clona el repositorio:
```
git clone https://github.com/sebasdias777/Workshop2
```
2. Crea un entorno virtual de Python e instala las dependencias:
```
python -m venv venv
source venv/bin/activate
```
3. Utiliza Jupyter Notebook para realizar análisis exploratorio de datos y crear visualizaciones.
   
4. Configura Apache Airflow. Asegúrate de tener Apache Airflow instalado y configurado.
```
airflow scheduler
airflow standalone
```
5. Ejecuta el flujo de trabajo ETL con Apache Airflow. Puedes definir las tareas y programarlas según tus necesidades.

6. Los datos transformados se cargarán en una base de datos PostgreSQL. Asegúrate de configurar adecuadamente las credenciales de la base de datos en tu archivo de configuración Airflow.
7. Utiliza Power BI para crear dashboards interactivos y presentar los resultados.
