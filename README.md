# Málaga Parking Dashboard

Este dashboard es parte de un proyecto para la visualización de datos de ocupación de parkings en Málaga.

![Mapa de parkings de Málaga](https://github.com/javi-aranda/malaga-parking-dashboard/blob/master/resources/dashboard_01.png?raw=true)

La información para nutrir la base de datos se obtiene del repositorio [Málaga Parking Data](https://github.com/javi-aranda/malaga-parking-data), que a su vez los recopila de forma automática del endpoint de consulta de datos abiertos del Ayuntamiento de Málaga desde mayo de 2022.

En enero de 2024 el formato de los datos cambió, previamente había sensores de parkings que no estaban funcionando correctamente y se han descartado para su análisis.


![Mapa de parkings de Málaga](https://github.com/javi-aranda/malaga-parking-dashboard/blob/master/resources/dashboard_02.png?raw=true)

El código fuente de esta herramienta, así como la base de datos, está disponible en [Málaga Parking Dashboard](https://github.com/javi-aranda/malaga-parking-dashboard).

## Instalación de dependencias y uso

Se puede clonar el proyecto mediante el comando
```bash
~ git clone https://github.com/javisenberg/malaga-parking-data-dashboard.git
```

Para trabajar de forma local se recomienda crear un entorno virtual de Python >= 3.9
```bash
~ python -m venv ~/.venvs/malaga-parking-dashboard
~ source ~/.venvs/malaga-parking-dashboard/bin/activate
(malaga-parking-dashboard) ~ pip install -r requirements.txt
(malaga-parking-dashboard) ~ git clone https://github.com/javi-aranda/malaga-parking-data ../malaga-parking-data
(malaga-parking-dashboard) ~ python ./scripts/ingest.py  # Opcional, para cargar los últimos datos
(malaga-parking-dashboard) ~ streamlit run app.py
```

Made with ❤️ by Javi Aranda

