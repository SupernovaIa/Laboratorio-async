import pandas as pd
from time import sleep
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import asyncio
import random


def crear_df(tabla):
    """
    Crea un DataFrame a partir de una tabla de texto con datos meteorológicos, procesando las unidades y formateando las columnas.

    Parameters:
    - tabla (str): Texto que contiene los datos meteorológicos de la tabla extraída de la web.

    Returns:
    - (pd.DataFrame): DataFrame con las columnas correspondientes a las diferentes variables meteorológicas y las filas organizadas por fecha.
    """
    df = pd.DataFrame(tabla.split("\n"))
    df.drop([0, 1], inplace=True)
    df[0] = df[0].str.replace(' °F', '')
    df[0] = df[0].str.replace(' mph', '')
    df[0] = df[0].str.replace(' in', '')
    df[0] = df[0].str.replace(' %', '')
    df = df[0].str.split(' ', expand=True)
    df.columns = ["Date", "High Temp (ºF)", "Avg Temp (ºF)", "Low T (ºF)", "High Dew Pt (ºF)", "Avg Dew Pt (ºF)", "Low Dew Pt (ºF)", "High Hum (%)", "Avg Hum (%)", "Low Hum (%)", "High Spe (mph)", "Avg Spe (mph)", "Low Spe (mph)", "High Press (in)", "Low Press (in)", "Sum Prec (In)"]
    df.set_index('Date', inplace=True)
    df = df.applymap(lambda x: float(x))
    return df


def obtener_codigo(municipio):
    """
    Obtiene el código de la estación meteorológica de un municipio desde una página web utilizando Selenium.

    Parameters:
    - municipio (str): Nombre del municipio para el cual se obtendrá el código de la estación.

    Returns:
    - (str): Código de la estación meteorológica del municipio.
    """

    # Abrir navegador
    driver = webdriver.Chrome()
    url = f"https://www.wunderground.com/weather/es/{municipio}"
    driver.get(url)
    driver.maximize_window()

    # Rechazar cookies
    iframe = WebDriverWait(driver, 10).until(EC.presence_of_element_located(('xpath', '//*[@id="sp_message_iframe_1165301"]')))
    driver.switch_to.frame(iframe)
    sleep(3)
    driver.find_element("css selector", "#notice > div.message-component.message-row.cta-buttons-container > div.message-component.message-column.cta-button-column.reject-column > button").click()
    driver.switch_to.default_content()

    # Capturar código
    sleep(3)
    driver.find_element("css selector", "#inner-content > div.region-content-top > lib-city-header > div:nth-child(1) > div > div > a.station-name").click()
    sleep(3)
    codigo_municipio = driver.find_element("css selector", "#inner-content > div.region-content-top > app-dashboard-header > div.dashboard__header.small-12.ng-star-inserted > div > div.heading > h1").text.split(' - ')[1]
    
    # Cerrar navegador
    driver.close()
    return codigo_municipio


def obtener_df_mes(i, codigo):
    """
    Recupera un DataFrame con los datos de un mes específico desde una página web utilizando Selenium.

    Parameters:
    - i (int): Número del mes para el cual se obtendrán los datos (1 para enero, 2 para febrero, etc.).
    - codigo (str): Código del municipio para generar la URL de la consulta.

    Returns:
    - (pd.DataFrame): DataFrame que contiene los datos extraídos de la tabla de la página web.
    """

    # Abrir navegador
    driver = webdriver.Chrome()
    url = f"https://www.wunderground.com/dashboard/pws/{codigo}/table/2024-{i}-1/2024-{i}-1/monthly"
    driver.get(url)
    driver.maximize_window()

    # Rechazar cookies
    iframe = WebDriverWait(driver, 10).until(EC.presence_of_element_located(('xpath', '//*[@id="sp_message_iframe_1165301"]')))
    driver.switch_to.frame(iframe)
    sleep(3)
    driver.find_element("css selector", "#notice > div.message-component.message-row.cta-buttons-container > div.message-component.message-column.cta-button-column.reject-column > button").click()
    driver.switch_to.default_content()

    # Caputurar tabla
    tabla = driver.find_element("css selector", "#main-page-content > div > div > div > lib-history > div.history-tabs > lib-history-table > div > div").text
    df = crear_df(tabla)
    
    # Cerrar navegador
    driver.close()
    return df


async def obtener_dfs_municipio(municipio):
    """
    Lanza subprocesos de Selenium para obtener datos de un municipio de forma concurrente para varios meses.

    Parameters:
    - municipio (str): Nombre del municipio del cual se obtendrán los datos.

    Returns:
    - (pd.DataFrame): DataFrame que combina los resultados de cada mes.
    """
    codigo_municipio = obtener_codigo(municipio)
    
    # Definimos un 'executor' con varios hilos para realizar los scrapes de manera concurrente
    with ThreadPoolExecutor(max_workers=9) as executor:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(executor, obtener_df_mes, mes, codigo_municipio)
            for mes in range(1, 10)  # Meses de enero a septiembre
        ]
        
        # Esperamos que todas las tareas asíncronas terminen
        resultados = await asyncio.gather(*tasks)

    return pd.concat(resultados)


async def obtener_dfs_municipios(municipios):
    """
    Obtiene y guarda los datos meteorológicos de múltiples municipios de forma asíncrona, procesando los datos y almacenándolos en archivos CSV.

    Parameters:
    - municipios (list): Lista de nombres de municipios a procesar.
    """

    for municipio in municipios:
        print(f"Procesando municipio: {municipio}")
        try:
            resultados = await obtener_dfs_municipio(municipio)
            resultados.to_csv(f'datos/{municipio}_weather_data.csv')
            print(f"Datos guardados para {municipio}")
        except Exception as e:
            print(f"Error al procesar {municipio}: {e}")


# Lista de municipios que deseas procesar
municipios = ['acebeda-la', 'ajalvir', 'alameda-del-valle', 'alamo-el', 'alcala-de-henares', 'alcobendas', 'alcorcon', 'aldea-del-fresno', 'algete', 'alpedrete', 'ambite', 'anchuelo', 'aranjuez', 'arganda-del-rey', 'arroyomolinos', 'atazar-el', 'batres', 'becerril-de-la-sierra', 'belmonte-de-tajo', 'berrueco-el', 'berzosa-del-lozoya', 'boadilla-del-monte', 'boalo-el', 'braojos', 'brea-de-tajo', 'brunete', 'buitrago-del-lozoya', 'bustarviejo', 'cabanillas-de-la-sierra', 'cabrera-la', 'cadalso-de-los-vidrios', 'camarma-de-esteruelas', 'campo-real', 'canencia', 'carabana', 'casarrubuelos', 'cenicientos', 'cercedilla', 'cervera-de-buitrago', 'chapineria', 'chinchon', 'ciempozuelos', 'cobena', 'collado-mediano', 'collado-villalba', 'colmenar-del-arroyo', 'colmenar-de-oreja', 'colmenarejo', 'colmenar-viejo', 'corpa', 'coslada', 'cubas-de-la-sagra', 'daganzo-de-arriba', 'escorial-el', 'estremera', 'fresnedillas-de-la-oliva', 'fresno-de-torote', 'fuenlabrada', 'fuente-el-saz-de-jarama', 'fuentiduena-de-tajo', 'galapagar', 'garganta-de-los-montes', 'gargantilla-del-lozoya-y-pinilla-de-buitrago', 'gascones', 'getafe', 'grinon', 'guadalix-de-la-sierra', 'guadarrama', 'hiruela-la', 'horcajo-de-la-sierra-aoslos', 'horcajuelo-de-la-sierra', 'hoyo-de-manzanares', 'humanes-de-madrid', 'leganes', 'loeches', 'lozoya', 'lozoyuela-navas-sieteiglesias', 'madarcos', 'madrid', 'majadahonda', 'manzanares-el-real', 'meco', 'mejorada-del-campo', 'miraflores-de-la-sierra', 'molar-el', 'molinos-los', 'montejo-de-la-sierra', 'moraleja-de-enmedio', 'moralzarzal', 'morata-de-tajuna', 'mostoles', 'navacerrada', 'navalafuente', 'navalagamella', 'navalcarnero', 'navarredonda-y-san-mames', 'navas-del-rey', 'nuevo-baztan', 'olmeda-de-las-fuentes', 'orusco-de-tajuna', 'paracuellos-de-jarama', 'parla', 'patones', 'pedrezuela', 'pelayos-de-la-presa', 'perales-de-tajuna', 'pezuela-de-las-torres', 'pinilla-del-valle', 'pinto', 'pinuecar-gandullas', 'pozuelo-de-alarcon', 'pozuelo-del-rey', 'pradena-del-rincon', 'puebla-de-la-sierra', 'puentes-viejas-manjiron', 'quijorna', 'rascafria', 'reduena', 'ribatejada', 'rivas-vaciamadrid', 'robledillo-de-la-jara', 'robledo-de-chavela', 'robregordo', 'rozas-de-madrid-las', 'rozas-de-puerto-real', 'san-agustin-del-guadalix', 'san-fernando-de-henares', 'san-lorenzo-de-el-escorial', 'san-martin-de-la-vega', 'san-martin-de-valdeiglesias', 'san-sebastian-de-los-reyes', 'santa-maria-de-la-alameda', 'santorcaz', 'santos-de-la-humosa-los', 'serna-del-monte-la', 'serranillos-del-valle', 'sevilla-la-nueva', 'somosierra', 'soto-del-real', 'talamanca-de-jarama', 'tielmes', 'titulcia', 'torrejon-de-ardoz', 'torrejon-de-la-calzada', 'torrejon-de-velasco', 'torrelaguna', 'torrelodones', 'torremocha-de-jarama', 'torres-de-la-alameda', 'tres-cantos', 'valdaracete', 'valdeavero', 'valdelaguna', 'valdemanco', 'valdemaqueda', 'valdemorillo', 'valdemoro', 'valdeolmos-alalpardo', 'valdepielagos', 'valdetorres-de-jarama', 'valdilecha', 'valverde-de-alcala', 'velilla-de-san-antonio', 'vellon-el', 'venturada', 'villaconejos', 'villa-del-prado', 'villalbilla', 'villamanrique-de-tajo', 'villamanta', 'villamantilla', 'villanueva-de-la-canada', 'villanueva-del-pardillo', 'villanueva-de-perales', 'villar-del-olmo', 'villarejo-de-salvanes', 'villaviciosa-de-odon', 'villavieja-del-lozoya', 'zarzalejo']

# Función principal para ejecutar el bucle de municipios
async def main():
    await obtener_dfs_municipios(municipios)

# Ejecutar el script
if __name__ == '__main__':
    asyncio.run(main())
