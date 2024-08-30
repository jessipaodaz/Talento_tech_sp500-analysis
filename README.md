# Talento_tech_sp500-analysis
Descripción del proyecto
Este proyecto tiene como objetivo analizar las empresas del S&amp;P 500 a través de diferentes fases, que incluyen la extracción de datos, análisis estadístico, almacenamiento en SQL Server, creación de dashboards en Power BI, y finalmente, una clusterización basada en la volatilidad de las acciones.
## Requisitos
- Python 3.x
- Librerías: `pandas`, `sqlalchemy`, `pyodbc`, `scikit-learn`
- Power BI Desktop
- SQL Server
## Estructura del Proyecto
- `data/`: Contiene los archivos CSV con los datos de las empresas y perfiles.
- `scripts/`: Contiene los scripts Python para las diferentes fases del proyecto.
- `dashboards/`: Contiene el archivo .pbix de Power BI.
- `README.md`: Documento explicativo del proyecto.
## Instrucciones de Instalación y Uso
1. Clona este repositorio: git clone https://github.com/jessipaodaz/sp500-analysis.git
cd sp500-analysis
2. Instala las dependencias necesarias:
pip install -r requirements.txt
3. Configura la conexión a SQL Server en los scripts de las fases correspondientes.
4. Ejecuta los scripts en orden para realizar el análisis completo.
