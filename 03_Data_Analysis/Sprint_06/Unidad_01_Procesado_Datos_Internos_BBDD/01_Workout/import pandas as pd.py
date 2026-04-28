import requests
import pandas as pd

# 1. Definir la API Key y la URL
api_key = "tu_api_key_aqui"
url_base = "https://opendata.aemet.es/opendata"
endpoint = "/api/prediccion/especifica/playa/{codigo_playa}"

# 2. Obtener los códigos de playa desde un CSV de la AEMET
url_codigos = "https://www.aemet.es/documentos/es/eltiempo/prediccion/playas/Playas_codigos.csv"
df_playas = pd.read_csv(url_codigos, sep=";", encoding="latin-1")
# ↑ pandas.read_csv puede leer directamente desde URLs

# 3. Filtrar las playas de Melilla
playas_melilla = df_playas[df_playas["Provincia"] == "Melilla"]

# 4. Llamar a la API con la API Key en los parámetros
codigo = playas_melilla.iloc[0]["ID_playa"]
url = url_base + endpoint.format(codigo_playa=codigo)

querystring = {"api_key": api_key}
headers = {"cache-control": "no-cache"}

respuesta = requests.request("GET", url, headers=headers, params=querystring)
print(respuesta.status_code)