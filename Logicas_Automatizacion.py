import os
import logging
import shutil
import Automatizaciones_Patron as automatizaciones

logging.basicConfig(filename='logicas_automatizacion.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def definir_funciones_procesamiento():
    # Este diccionario debería ser llenado con todos los mapeos necesarios
    funciones = {
        # MG
        'MGAM': automatizaciones.procesar_MGAM,
        'MGAS': automatizaciones.procesar_MGAS,
        'MGAY': automatizaciones.procesar_MGAY,
        'MGBA': automatizaciones.procesar_MGBA,
        'MGCX': automatizaciones.procesar_MGCX,
        'MGDL': automatizaciones.procesar_MGDL,
        'MGIB': automatizaciones.procesar_MGIB,
        'MGJL': automatizaciones.procesar_MGJL,
        'MGLH': automatizaciones.procesar_MGLH,
        'MGMH': automatizaciones.procesar_MGMH,
        'MGQF': automatizaciones.procesar_MGQF,
        'MGQR': automatizaciones.procesar_MGQR,
        'MGRJ': automatizaciones.procesar_MGRJ,
        'MGVS': automatizaciones.procesar_MGVS,
        # MI
        'MIAM': automatizaciones.procesar_MIAM,
        'MIAS': automatizaciones.procesar_MIAS,
        'MIAY': automatizaciones.procesar_MIAY,
        'MIBA': automatizaciones.procesar_MIBA,
        'MICX': automatizaciones.procesar_MICX,
        'MIDL': automatizaciones.procesar_MIDL,
        'MIIB': automatizaciones.procesar_MIIB,
        'MIJL': automatizaciones.procesar_MIJL,
        'MILH': automatizaciones.procesar_MILH,
        'MIMH': automatizaciones.procesar_MIMH,
        'MIQF': automatizaciones.procesar_MIQF,
        'MIQR': automatizaciones.procesar_MIQR,
        'MIRJ': automatizaciones.procesar_MIRJ,
        'MIVS': automatizaciones.procesar_MIVS,
        # PI
        'PIAY': automatizaciones.procesar_PIAY,
        'PIBA': automatizaciones.procesar_PIBA,
        'PIIB': automatizaciones.procesar_PIIB,
        'PIQR': automatizaciones.procesar_PIQR,
        'PIJL': automatizaciones.procesar_PIJL,
        'PICX': automatizaciones.procesar_PICX,  
        # PG
        'PGDL': automatizaciones.procesar_PGDL,
        'TMP': automatizaciones.procesar_TMP,               


    }
    return funciones

def procesar_archivos(tipo, proceso, carriers):
    carpeta_input = f'/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/{tipo}/INPUT'
    carpeta_historico = f'/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/{tipo}/HISTORICO'
    carpeta_output = f'/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/{tipo}/OUTPUT'

    # Asegúrate de que las carpetas de salida e histórico existan
    os.makedirs(carpeta_historico, exist_ok=True)
    os.makedirs(carpeta_output, exist_ok=True)

    # Lista de archivos para el proceso específico
    archivos = [archivo for archivo in os.listdir(carpeta_input) if archivo.startswith(proceso)]

    if not archivos:
        logging.info(f"No se encontraron archivos para procesar en {carpeta_input}")
        print(f"No se encontraron archivos para procesar en {carpeta_input}")
        return

    logging.info(f"Se encontraron {len(archivos)} archivos {proceso} comenzando a procesar...")
    print(f"Se encontraron {len(archivos)} archivos {proceso} comenzando a procesar...")

    for archivo in archivos:
        ruta_archivo = os.path.join(carpeta_input, archivo)

        # Procesar solo los archivos que contengan "MGAM" en su nombre
        if "MGAM" in archivo:
            automatizaciones.procesar_MGAM(ruta_archivo, carpeta_input, carpeta_output, carpeta_historico)

        # Mover el archivo procesado a la carpeta histórica
        ruta_destino_historico = os.path.join(carpeta_historico, archivo)
        shutil.move(ruta_archivo, ruta_destino_historico)
        logging.info(f"Archivo movido a {ruta_destino_historico}")
        print(f"Archivo movido a {ruta_destino_historico}")

        # Mostrar cuántos archivos quedan por procesar
        archivos_restantes = len(archivos) - archivos.index(archivo) - 1
        print(f"Archivos restantes: {archivos_restantes}")
# Definición de los carriers por proceso

carriers_por_proceso = {
    'OLA': {
        'MG': ["AM", "AS", "AY", "BA", "CX", "DL", "IB", "JL", "LH", "MH", "QF", "QR", "RJ", "VS"],
        'MI': ["AM", "AS", "AY", "BA", "CX", "DL", "IB", "JL", "LH", "MH", "QF", "QR", "RJ", "VS"],
        'PI': ["BA", "CX", "IB", "JL", "QR", "AY"],
        'PG': ["DL", "TMP"],
    },
    'IH': {
        'MG': ["JJLA"],
        'MI': ["LAXL", "LA4C", "LAJJ","LALP"],
        'PI': [],
        'PG': [],
    }
}
