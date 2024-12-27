import logging
import gspread
import pandas as pd
import os
import shutil
import numpy as np
import Logicas_Automatizacion
from datetime import datetime, timedelta 
from google.auth import default
from google.colab import auth

# Configuración del logger
logging.basicConfig(filename='automatizaciones_patron.log', level=logging.INFO, format='%(asctime)s - %(message)s')
####################################################################################################### 
##### #AUTOMATIZACIONES ESPECIFICAS PARA CADA ARCHIVO #################################################
#######################################################################################################
#######################################################################################################
######## OLA ##########################################################################################
#######################################################################################################  
#######################################################################################################
######## MG ###########################################################################################
#######################################################################################################       
#######################################################################################################

def procesar_MGAM(ruta_archivo, carpeta_input, carpeta_output, carpeta_historico):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Verifica que el archivo exista
        if not os.path.exists(ruta_archivo):
            logging.error(f"El archivo {ruta_archivo} no existe.")
            print(f"Error: El archivo {ruta_archivo} no existe.")
            return

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        encabezados = [
            'Carrier', 'Nro pax frecuente', 'Apellido del pasajero', 'Nombre del pasajero',
            'Carrier operador', 'Vuelo operador', 'Cruce nro vlo', 'Fecha uso', 'Origen', 'Destino','Clase tarifa',
            'Base millas', 'Bonus millas','Extra bonus', 'Promoción bonus', 'Total millas' ,'Factor', 'Monto Facturado USD','Filtro 3', 'Status', 'billing month',
            'Filtro 1','Filtro 2','Periodo facturación','Mes facturación', 'Sociedad','Tipo de facturación'
        ]

        # Función para convertir un archivo TXT a un archivo Excel
        def convert_txt_to_excel(txt_file_path, excel_file_path):
            print(f'Procesando archivo: {os.path.basename(txt_file_path)}')
            with open(txt_file_path, 'r') as txt_file:
                lines = txt_file.readlines()
                data = []
                for line in lines:
                    values = [
                        line[0:2].strip(),      # Columna A: CARRIER
                        line[2:12].strip(),     # Columna B: Nro pax frecuente
                        line[12:51].strip(),    # Columna C: Apellido del pasajero
                        line[51:52].strip(),    # Columna D: Nombre del pasajero
                        line[52:54].strip(),    # Columna F: Carrier operador
                        line[54:59].strip(),    # Columna G: Vuelo operador
                        line[59:67].strip(),    # Columna H: Fecha uso
                        line[67:70].strip(),    # Columna I: Origen
                        line[70:74].strip(),    # Columna H: Destino
                        line[74:76].strip(),    # Columna I: Clase tarifa
                        line[76:86].strip(),    # Columna J: Base millas
                        line[86:93].strip(),    # Columna K: Bonus millas
                        line[93:100].strip(),   # Columna L: Extra bonus
                        line[100:107].strip(),  # Columna M: Promoción bonus
                        line[107:109].strip(),  # Columna N: Filtro 1
                        line[109:111].strip(),  # Columna O: Status
                        line[111:117].strip(),  # Columna P: billing month
                        line[117:127].strip(),  # Columna Q: FILTRO 2
                        line[127:137].strip(),  # Columna R: FILTRO 3
                    ]
                    data.append(values)
                    
                df = pd.DataFrame(data, dtype=str)

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(txt_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 5] = df.iloc[:, 5].apply(remove_leading_zeros)

                # Cambia el formato de las columnas Q, R, S, T a número entero
                def to_int(x):
                    try:
                        return int(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [10,11,12,13,17,18]
                for col in columns_to_convert: # Iterate through the list of column indices
                    df.iloc[:, col] = df.iloc[:, col].apply(to_int) # Apply the function to each column

                # Función para agregar una columna con la suma de las columnas Q, R, S y T en la posición X
                def agregar_columna_suma(df):
                    df.insert(14, 'Total millas', df.iloc[:, [10, 11, 12, 13]].sum(axis=1))
                    return df

                # Agrega la columna con la suma de Q, R, S y T en la posición X
                df = agregar_columna_suma(df)

                # Añadir la columna 'Factor' en la posición 15 (índice 14)
                df.insert(15, 'Factor', 0.01)

                # Función para agregar una columna con el producto de las columnas X e Y en la posición Z
                def agregar_columna_producto(df):
                    df.iloc[:, 14] = pd.to_numeric(df.iloc[:, 14], errors='coerce').fillna(0)
                    df.iloc[:, 15] = pd.to_numeric(df.iloc[:, 15], errors='coerce').fillna(0)
                    df.insert(16, 'Monto Facturado USD', df.iloc[:, 15] * df.iloc[:, 14])
                    return df

                df = agregar_columna_producto(df)

                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)  # Convertir a cadena
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                # Transformar la fecha en la columna P
                df[6] = df[6].apply(transformar_fecha_corta)
                # Añade otras columnas
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Elimina la fila 2 (índice 1)
                df = df.drop(df.index[0])
                df = df.iloc[:-1]

                # Realiza el cruce de números de vuelo usando la columna O (índice 14)
                valores_buscados = df.iloc[:, 5].tolist()  # La columna O es la columna de interés
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtiene el valor de la columna F de hoja origen
                            break  # Sale del bucle si encuentra el valor
                    resultados.append(valor_encontrado)

                # Añadir la columna 'Cruce nro vlo' en la posición G (índice 6)
                df.insert(6, 'Cruce nro vlo', resultados)

                # Organiza las columnas en el orden especificado en 'encabezados'
                df.columns = encabezados[:len(df.columns)]
                for columna_faltante in encabezados[len(df.columns):]:
                    df[columna_faltante] = ''

                # Convertir la columna a datetime especificando el formato actual de las cadenas
                df['Fecha uso'] = pd.to_datetime(df['Fecha uso'], format='%d-%m-%Y')
                df['Mes facturación'] = pd.to_datetime(df['Mes facturación'], format='%d-%m-%Y')

                # Cambiar el formato a fecha corta y sobreescribir la columna original
                df['Fecha uso'] = df['Fecha uso'].dt.strftime('%d-%m-%Y')

                # Cambiar el formato a fecha corta y sobreescribir la columna original
                df['Mes facturación'] = df['Mes facturación'].dt.strftime('%d-%m-%Y')

                # Convertir todos los valores de 'Total millas' a enteros y luego a strings
                df['Total millas'] = pd.to_numeric(df['Total millas'], errors='coerce').astype(int).astype(str)

                # Convertir todos los valores de 'R' a 2 decimales y luego a strings
                df.iloc[:, 17] = pd.to_numeric(df.iloc[:, 17], errors='coerce').apply(lambda x: f"{round(x, 2):.2f}")

                # Convertir todos los valores de 'monto_facturado_usd' a float64
                df['Monto Facturado USD'] = pd.to_numeric(df['Monto Facturado USD'], errors='coerce').astype('float64')

                # Convertir todas las columnas del DataFrame a tipo string excepto 'monto_facturado_usd'
                for col in df.columns:
                    if col != 'Monto Facturado USD':
                        df[col] = df[col].astype('string')


                output = f"Procesando archivo: {os.path.basename(txt_file_path)}\n"
                for col in df.columns:
                    dtype = 'STRING' if col != 'Monto Facturado USD' else 'float64'
                    output += f"{col:<25} {dtype}\n"
                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Define la ruta completa para guardar el archivo Excel
                output_file_path = os.path.join(carpeta_output, f'{os.path.basename(ruta_archivo).replace(".txt", ".xlsx")}')
                
                print(f"Intentando guardar en: {output_file_path}")
                os.makedirs(carpeta_output, exist_ok=True)

                df.to_excel(output_file_path, index=False, header=True)
                
                # Verificar si el archivo se guardó correctamente
                if os.path.exists(output_file_path):
                    print(f"Archivo guardado exitosamente en {output_file_path}")
                    logging.info(f"Archivo guardado exitosamente en {output_file_path}")
                else:
                    print("Error: No se pudo guardar el archivo.")
                    logging.error("Error: No se pudo guardar el archivo.")
        
        output_file_path = os.path.join(carpeta_output, f'{os.path.basename(ruta_archivo).replace(".txt", ".xlsx")}')
        convert_txt_to_excel(ruta_archivo, output_file_path)
        
    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################  
def procesar_MGDL(ruta_archivo, carpeta_input, carpeta_output, carpeta_historico):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        print("Autenticación completada") 
        # Verifica que el archivo exista
        if not os.path.exists(ruta_archivo):
            logging.error(f"El archivo {ruta_archivo} no existe.")
            print(f"Error: El archivo {ruta_archivo} no existe.")
            return
        print("Archivo verificado")

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        encabezados = [
            'Transaction ID', 'Carrier', 'Nro pax frecuente', 'Apellido del pasajero', 'Nombre del pasajero',
            'Partners', 'Vuelo marketing', 'Partner 2', 'Vuelo operador', 'Cruce nro de vuelo', 'Fecha uso',
            'Clase tarifa', 'Origen', 'Destino', 'Nro de ticket', 'Fecha uso', 'Base millas', 'Bonus millas',
            'Extra bonus', 'Promoción bonus', 'Filtro 1', 'Status tkt', 'Status designator', 'Total millas', 'Factor',
            'Net amount', 'Tax 1,075', 'Monto Facturado USD', 'Tipo de facturación', 'Carrier operador',
            'Periodo facturación', 'Mes facturación', 'Sociedad'
        ]

        # Función para convertir un archivo TXT a un archivo Excel
        def convert_txt_to_excel(txt_file_path, excel_file_path):
            print(f'Procesando archivo: {os.path.basename(txt_file_path)}')
            with open(txt_file_path, 'r') as txt_file:
                lines = txt_file.readlines()
                data = []
                for line in lines:
                    values = [
                        line[0:22].strip(), line[22:25].strip(), line[25:45].strip(),
                        line[45:65].strip(), line[65:85].strip(), line[85:88].strip(),
                        line[88:93].strip(), line[93:95].strip(), line[95:101].strip(),
                        line[101:110].strip(), line[110:118].strip(), line[118:121].strip(),
                        line[121:124].strip(), line[126:138].strip(), line[138:147].strip(),
                        line[147:152].strip(), line[152:157].strip(), line[157:162].strip(),
                        line[162:167].strip(), line[167:168].strip(), line[168:169].strip(),
                        line[169:170].strip()
                    ]
                    data.append(values)
                    
                df = pd.DataFrame(data, dtype=str)

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(txt_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade otras columnas
                df['Factor'] = 0.009
                df['Tax 1,075'] = 1.075
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier operador'] = 'LA'
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Sociedad'] = ''

                # Procesamiento específico del DataFrame
                df = df.drop(df.index[0])
                df = df.iloc[:-1]

                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 8] = df.iloc[:, 8].apply(remove_leading_zeros)
                df.iloc[:, 6] = df.iloc[:, 6].apply(remove_leading_zeros)

                valores_buscados = df.iloc[:, 8].tolist()
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:
                            valor_encontrado = fila[5]
                            break
                    resultados.append(valor_encontrado)

                df.insert(9, 'Cruce nro vlo', resultados)

                def to_int(x):
                    try:
                        return int(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [16, 17, 18, 19]
                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_int)

                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                def agregar_columna_suma(df):
                    df.insert(23, 'Total millas', df.iloc[:, [16, 17, 18, 19]].sum(axis=1))
                    return df

                def agregar_columna_net_amount(df):
                    df['Factor'] = pd.to_numeric(df['Factor'], errors='coerce')
                    df['Total millas'] = pd.to_numeric(df['Total millas'], errors='coerce')
                    df.insert(25, 'Net Amount', df['Factor'].astype(float) * df['Total millas'].astype(float))
                    return df

                def agregar_columna_monto_facturado(df):
                    df['Net Amount'] = pd.to_numeric(df['Net Amount'], errors='coerce')
                    df['Tax 1,075'] = pd.to_numeric(df['Tax 1,075'], errors='coerce')
                    df.insert(27, 'Monto facturado USD', df['Net Amount'].astype(float) * df['Tax 1,075'].astype(float))
                    return df

                df = agregar_columna_suma(df)
                df = agregar_columna_net_amount(df)
                df = agregar_columna_monto_facturado(df)
                df[9] = df[9].apply(transformar_fecha_corta)
                df[14] = df[14].apply(transformar_fecha_corta)

                def formatear_monto_facturado_usd(df):
                    df['Monto Facturado USD'] = df['Monto Facturado USD'].apply(lambda x: f"{x:.3f}")
                    return df

                df = formatear_monto_facturado_usd(df)
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados


                # Define la ruta completa para guardar el archivo Excel
                output_file_path = os.path.join(carpeta_output, f'{os.path.basename(ruta_archivo).replace(".txt", ".xlsx")}')
                
                print(f"Intentando guardar en: {output_file_path}")
                os.makedirs(carpeta_output, exist_ok=True)

                df.to_excel(output_file_path, index=False, header=True)
                
                # Verificar si el archivo se guardó correctamente
                if os.path.exists(output_file_path):
                    print(f"Archivo guardado exitosamente en {output_file_path}")
                    logging.info(f"Archivo guardado exitosamente en {output_file_path}")
                else:
                    print("Error: No se pudo guardar el archivo.")
                    logging.error("Error: No se pudo guardar el archivo.")
        
        output_file_path = os.path.join(carpeta_output, f'{os.path.basename(ruta_archivo).replace(".txt", ".xlsx")}')
        convert_txt_to_excel(ruta_archivo, output_file_path)
        print("convert_txt_to_excel llamada exitosamente")
        
    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")#######################################################################################################        
#######################################################################################################
def procesar_MGJL(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos TXT en la carpeta de entrada
        txt_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'FFP Program', 'Nro pax frecuente', 'Apellido del pasajero', 'Nombre del pasajero', 'Mk', 'Vuelo operador', 
            'Cruce nro VLO', 'Fecha uso', 'Origen', 'Destino', 'Clase tarifa', 'Clase cabina', 'Base millas', 
            'Bonus millas', 'Billable Partner', 'Entry Source Code', 'Billing Month', 'Request ID', 'Total millas', 
            'Factor', 'Monto Facturado USD', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 
            'Carrier operador', 'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")

                # Cargar el archivo XLSX en un DataFrame
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(xlsx_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                # Fragmentar la columna 'Mkt Flt Number' en dos columnas
                df['Mk'] = df['Mkt Flt Number'].str[:2]  # Primeros 2 caracteres
                df['Vuelo operador'] = df['Mkt Flt Number'].str[-4:]  # Últimos 4 caracteres

                # Insertar las nuevas columnas a la derecha de 'Mkt Flt Number'
                df.insert(df.columns.get_loc('Mkt Flt Number') + 1, 'Mk', df.pop('Mk'))
                df.insert(df.columns.get_loc('Mk') + 1, 'Vuelo operador', df.pop('Vuelo operador'))

                # Eliminar la columna original 'Mkt Flt Number'
                df.drop(columns='Mkt Flt Number', inplace=True)

                # Aplica la función remove_leading_zeros a la columna F (índice 5)
                df.iloc[:, 5] = df.iloc[:, 5].apply(remove_leading_zeros)

                # Añade otras columnas
                df['Factor'] = 0.0093
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Sociedad'] = ''
                df['Carrier'] = 'JL'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Elimina la fila 2 (índice 1)
                df = df.drop(df.index[0])
                df = df.iloc[:-1]

                # Realiza el cruce de números de vuelo usando la columna F (índice 5)
                valores_buscados = df.iloc[:, 5].tolist()  # La columna F es la columna de interés
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtiene el valor de la columna F de hoja origen
                            break  # Sale del bucle si encuentra el valor
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados al DataFrame existente en la posición G (índice 6)
                df.insert(6, 'Cruce nro VLO', resultados)

                # Cambia el formato de las columnas Q, R, S, T a número entero
                def to_int(x):
                    try:
                        return int(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [12, 13]
                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_int)

                # Función para transformar la fecha
                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                # Transformar la fecha en la columna 'Flight Date'
                df['Flight Date'] = df['Flight Date'].apply(transformar_fecha_corta)

                # Función para agregar una columna con la suma de las columnas
                def agregar_columna_suma(df):
                    df.insert(18, 'Total millas', df.iloc[:, [12, 13]].sum(axis=1))
                    return df

                # Función para agregar una columna con el producto de las columnas
                def agregar_columna_producto(df):
                    df.insert(20, 'Monto facturado USD', df.iloc[:, 18].astype(float) * df.iloc[:, 19].astype(float))
                    return df

                # Aplicar las funciones de agregación
                df = agregar_columna_suma(df)
                df = agregar_columna_producto(df)

                # Asigna los encabezados
                df.columns = encabezados

                # Guarda el DataFrame en un archivo Excel
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in txt_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################  
def procesar_MGLH(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos TXT en la carpeta de entrada
        txt_files = [f for f in os.listdir(input_folder) if f.endswith('.txt')]

        # Encabezados para el DataFrame
        encabezados = [
            "Filtro 1", "Carrier", "Transaction ID", "Filtro 2", "Fecha uso", "Nro pax frecuente",
            "Apellido del pasajero", "Nombre del pasajero", "Carrier operador", "Vuelo operador",
            "Fecha emisión", "Origen", "Destino", "Clase cabina", "Clase tarifa", "Booking Class",
            "Nro de ticket", "Cupón", "Filtro 3", "Base millas", "Bonus millas", "Extra bonus",
            "Filtro 4", "Total millas", "Factor", "Filtro 5", "Filtro 6", "Filtro 7",
            "Status Ticket", "Monto Facturado USD", "Filtro 8", "Periodo facturación",
            "Mes facturación", "Sociedad", "Tipo de facturación"
        ]

        def convert_txt_to_excel(txt_file_path, excel_file_path):
            logging.info(f"Procesando archivo: {os.path.basename(txt_file_path)}")
            with open(txt_file_path, 'r') as txt_file:
                lines = txt_file.readlines()
                data = []
                for line in lines:
                    values = [
                        line[0:4].strip(), line[4:7].strip(), line[7:25].strip(),
                        line[25:41].strip(), line[41:49].strip(), line[49:69].strip(),
                        line[69:99].strip(), line[99:209].strip(), line[209:211].strip(),
                        line[211:217].strip(), line[217:225].strip(), line[225:228].strip(),
                        line[228:233].strip(), line[233:236].strip(), line[236:238].strip(),
                        line[238:240].strip(), line[240:254].strip(), line[254:256].strip(),
                        line[256:261].strip(), line[261:266].strip(), line[266:271].strip(),
                        line[271:351].strip(), line[351:356].strip(), line[356:361].strip(),
                        line[361:371].strip(), line[371:379].strip(), line[379:380].strip(),
                        line[380:385].strip(), line[385:386].strip(), line[386:408].strip(),
                        line[408:411].strip()
                    ]
                    data.append(values)
                df = pd.DataFrame(data, dtype=str)
                logging.debug("DataFrame creado.")

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(txt_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
                logging.debug(f"Fecha de facturación formateada: {mes_facturacion}")

                # Añade otras columnas
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Cambia el formato de las columnas Q, R, S, T a número entero
                def to_int(x):
                    try:
                        return int(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 30]
                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_int)

                def to_float(x):
                    try:
                        return float(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [24, 29]
                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_float)

                # Cambia el formato de la columna O a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 9] = df.iloc[:, 9].apply(remove_leading_zeros)

                # Elimina la fila 2 (índice 1)
                df = df.drop(df.index[0])
                df = df.drop(df.index[-1])

                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)  # Convertir a cadena
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                # Formatear la columna fecha uso para que tenga formato fecha corta
                df.iloc[:, 4] = df.iloc[:, 4].apply(transformar_fecha_corta)
                df.iloc[:, 10] = df.iloc[:, 10].apply(transformar_fecha_corta)

                # Asegúrate de que las columnas 19 (S) y 24 (X) sean las correctas en pandas
                columna_S = 28  # Columna S (índice 18)
                columna_X = 29  # Columna X (índice 23)

                # Aplica la transformación
                df.iloc[:, columna_X] = df.apply(lambda row: '-' + str(row.iloc[columna_X]) if row.iloc[columna_S] == '-' else row.iloc[columna_X], axis=1)

                # Asigna los encabezados.
                df.columns = encabezados

                # Guarda el DataFrame como un archivo Excel
                df.to_excel(excel_file_path, index=False, header=True)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

        def move_file_to_processed(txt_file_path):
            ##shutil.move(txt_file_path, os.path.join(processed_folder, os.path.basename(txt_file_path)))
            logging.info(f'Archivo {os.path.basename(txt_file_path)} movido a la carpeta de Input_Antiguo.')

        for txt_file in txt_files:
            txt_file_path = os.path.join(input_folder, txt_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(txt_file)[0] + '.xlsx')
            convert_txt_to_excel(txt_file_path, excel_file_path)
            move_file_to_processed(txt_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
####################################################################################################### 
def procesar_MGQF(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos TXT en la carpeta de entrada
        txt_files = [f for f in os.listdir(input_folder) if f.endswith('.txt')]

        # Encabezados para el DataFrame
        encabezados = [
            "Carrier", "Carrier operador", "Nro pax frecuente", "Apellido del pasajero",
            "Nombre del pasajero", "Type pax", "Filiales", "Vuelo operador", "Cruce nro vlo",
            "Partner 2", "Status", "Fecha uso", "Origen", "Destino", "Clase tarifa",
            "Base millas", "Bonus millas", "Extra bonus", "Total millas", "Status refund",
            "Filtro 5", "Billing month", "Factor", "Monto neto groso", "Tax 1,10",
            "Monto Facturado USD", "Clase cabina", "Periodo facturación", "Mes facturación",
            "Sociedad", "Tipo de facturación"
        ]

        def convert_txt_to_excel(txt_file_path, excel_file_path):
            logging.info(f"Procesando archivo: {os.path.basename(txt_file_path)}")
            with open(txt_file_path, 'r') as txt_file:
                lines = txt_file.readlines()
                data = []
                for line in lines:
                    values = [
                        line[0:32].strip(), line[32:34].strip(), line[34:36].strip(), line[36:54].strip(),
                        line[54:85].strip(), line[85:118].strip(), line[117:124].strip(), line[124:130].strip(),
                        line[130:134].strip(), line[134:136].strip(), line[136:153].strip(), line[153:161].strip(),
                        line[161:164].strip(), line[164:167].strip(), line[167:169].strip(), line[169:177].strip(),
                        line[177:185].strip(), line[185:193].strip(), line[193:201].strip(), line[201:210].strip(),
                        line[210:212].strip(), line[212:230].strip(), line[230:246].strip(), line[246:259].strip(),
                        line[259:411].strip()
                    ]

                    # Convertir fecha en formato dd-mm-yy a dd-mm-yyyy
                    fecha_vuelo = values[11]  # Columna L12: Fecha Vuelo
                    if len(fecha_vuelo) == 8:  # Verificar si el formato es dd-mm-yy
                        dia, mes, año = fecha_vuelo.split('-')
                        año = '20' + año  # Añadir '20' al año para convertirlo a formato yyyy
                        values[11] = f'{dia}-{mes}-{año}'

                    data.append(values)

                df = pd.DataFrame(data, dtype=str)  # Convertir todos los valores a cadenas de texto
                df.drop(df.index[0], inplace=True)
                df.drop(columns=[0], inplace=True)

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(txt_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade otras columnas
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Cambia el formato de las columnas Q, R, S, T a número entero
                def to_int(x):
                    try:
                        return int(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [14, 15, 16, 17, 22]

                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_int)

                def to_float(x):
                    try:
                        return float(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [21, 22, 23]
                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_float)

                # Cambia el formato de la columna O a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 7] = df.iloc[:, 7].apply(remove_leading_zeros)
                df.iloc[:, 24] = df.iloc[:, 24].apply(remove_leading_zeros)

                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)  # Convertir a cadena
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                # Realiza el cruce de números de vuelo usando la columna O (índice 14)
                valores_buscados = df.iloc[:, 7].tolist()
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen[1:]:  # Comienza a comparar desde la fila 2
                        if fila[4] == valor_buscado:
                            valor_encontrado = fila[5]
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 15)
                df.insert(8, 'Cruce nro vlo', resultados)

                # Añade una nueva columna con el encabezado 'tax' en la posición Z (índice 25)
                df.insert(24, 'Tax 1,10', 1.1)

                # Asegúrate de que las columnas 19 (S) y 24 (X) sean las correctas en pandas (recuerda que en pandas los índices comienzan en 0)
                columna_S = 19  # Columna S (índice 18)
                columna_X = 23  # Columna X (índice 23)

                # Aplica la transformación
                df.iloc[:, columna_X] = df.apply(lambda row: '-' + str(row.iloc[columna_X]) if row.iloc[columna_S] == 'C' else row.iloc[columna_X], axis=1)

                # Función para agregar una columna con el producto de las columnas X e Y en la posición Z
                def agregar_columna_producto(df):
                    df.insert(25, 'Monto Facturado USD', df.iloc[:, 23].astype(float) * df.iloc[:, 24].astype(float))
                    return df

                # Añade las nuevas columnas antes de hacer cualquier actualización
                df = agregar_columna_producto(df)

                def replace_dot_with_comma_in_column_X(df):
                    # Verificar si la columna 'X' existe en el DataFrame
                    if 'Monto neto groso' not in df.columns:
                        raise KeyError("La columna 'X' no existe en el DataFrame.")

                    # Realizar el reemplazo
                    df['Monto neto groso'] = df['Monto neto groso'].astype(str).str.replace('.', ',')

                    return df

                df = to_float(df)

                # Asigna los encabezados.
                df.columns = encabezados

                # Reemplazar puntos por comas en la columna 'X'
                df = replace_dot_with_comma_in_column_X(df)

                # Guarda el DataFrame como un archivo Excel
                df.to_excel(excel_file_path, index=False, header=True)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

        def move_file_to_processed(txt_file_path):
            ###shutil.move(txt_file_path, os.path.join(processed_folder, os.path.basename(txt_file_path)))
            logging.info(f'Archivo {os.path.basename(txt_file_path)} movido a la carpeta de InputAntiguo.')

        for txt_file in txt_files:
            txt_file_path = os.path.join(input_folder, txt_file)
            excel_file_path = os.path.join(output_folder, txt_file.replace('.txt', '.xlsx'))
            convert_txt_to_excel(txt_file_path, excel_file_path)
            move_file_to_processed(txt_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MGVS(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos TXT en la carpeta de entrada
        txt_files = [f for f in os.listdir(input_folder) if f.endswith('.txt')]

        # Encabezados para el DataFrame
        encabezados = [
            'Transaction ID', 'Carrier', 'Nro pax frecuente', 'Apellido del pasajero', 'Nombre del pasajero', 'Partners',
            'Vuelo marketing', 'Carrier operador', 'Vuelo operador', 'Fecha de emisión', 'Clase cabina', 'Clase tarifa',
            'Booking', 'Origen', 'Destino', 'Nro de ticket', 'Fecha uso', 'Base millas', 'Bonus millas', 'Extra bonus',
            'Promoción bonus','Total Millas', 'Factor','Monto Facturado USD','Credit Source Type','Status Ticket',
            'Periodo facturación','Mes Facturación', 'Tipo de facturación', 'Cruce Nro Vuelo', 'Sociedad'
        ]

        def convert_txt_to_excel(txt_file_path, excel_file_path):
            logging.info(f"Procesando archivo: {os.path.basename(txt_file_path)}")
            with open(txt_file_path, 'r') as txt_file:
                lines = txt_file.readlines()
                data = []
                for line in lines:
                    values = [
                        line[0:22].strip(), line[22:24].strip(), line[24:45].strip(), line[45:65].strip(),
                        line[65:85].strip(), line[85:88].strip(), line[88:92].strip(), line[92:96].strip(),
                        line[96:100].strip(), line[100:109].strip(), line[109:110].strip(), line[110:111].strip(),
                        line[111:118].strip(), line[118:121].strip(), line[121:124].strip(), line[124:139].strip(),
                        line[139:147].strip(), line[147:152].strip(), line[157:162].strip(), line[162:167].strip(),
                        line[152:157].strip(), line[167:169].strip(), line[169:170].strip()
                    ]
                    data.append(values)

                df = pd.DataFrame(data, dtype=str)  # Convertir todos los valores a cadenas de texto

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(txt_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade otras columnas
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Sociedad'] = ''

                # Elimina la fila 2 (índice 1)
                df = df.drop(df.index[0])
                df = df.iloc[:-1]

                # Cambia el formato de las columnas Q, R, S, T a número entero
                def to_int(x):
                    try:
                        return int(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [17,18,19,20]
                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_int)

                # Cambia el formato de la columna O a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 8] = df.iloc[:, 8].apply(remove_leading_zeros)

                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)  # Convertir a cadena
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                # Realiza el cruce de números de vuelo usando la columna O (índice 14)
                valores_buscados = df.iloc[:, 8].tolist()
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:
                            valor_encontrado = fila[5]
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 15)
                df.insert(26, 'Cruce nro vlo', resultados)

                # Función para agregar una columna con la suma de las columnas Q, R, S y T en la posición X
                def agregar_columna_suma(df):
                    df.insert(21, 'Total millas', df.iloc[:, [17,18,19,20]].sum(axis=1))
                    return df

                df.insert(21 , 'Factor', 0.007)

                # Función para agregar una columna con el producto de las columnas X e Y en la posición Z
                def agregar_columna_producto(df):
                    df.insert(22, 'Monto facturado USD', df.iloc[:, 15].astype(float) * df.iloc[:, 16].astype(float))
                    return df

                # Función para agregar una columna 'Monto facturado USD' con el producto de las columnas 'Net Amount' y 'Tax'
                def agregar_columna_monto_facturado(df):
                    df['Total millas'] = pd.to_numeric(df['Total millas'], errors='coerce')
                    df['Factor'] = pd.to_numeric(df['Factor'], errors='coerce')
                    df.insert(23, 'Monto facturado USD', df['Total millas'].astype(float) * df['Factor'].astype(float))
                    return df

                # Agrega la columna con la suma de Q, R, S y T en la posición X
                df = agregar_columna_suma(df)
                df = agregar_columna_monto_facturado(df)

                # Transformar la fecha en la columna P
                df[9] = df[9].apply(transformar_fecha_corta)
                df[16] = df[16].apply(transformar_fecha_corta)

                # Asigna los encabezados.
                df.columns = encabezados

                # Guarda el DataFrame como un archivo Excel
                df.to_excel(excel_file_path, index=False, header=True)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

        def move_file_to_processed(txt_file_path):
            ##shutil.move(txt_file_path, os.path.join(processed_folder, os.path.basename(txt_file_path)))
            logging.info(f'Archivo {os.path.basename(txt_file_path)} movido a la carpeta de Inputs_Antiguos.')

        for txt_file in txt_files:
            txt_file_path = os.path.join(input_folder, txt_file)
            excel_file_path = os.path.join(output_folder, txt_file.replace('.txt', '.xlsx'))
            convert_txt_to_excel(txt_file_path, excel_file_path)
            move_file_to_processed(txt_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
####################################################################################################### 
def procesar_MGQR(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos TXT en la carpeta de entrada
        txt_files = [f for f in os.listdir(input_folder) if f.endswith('.txt')]

        encabezados = [
            "Filtro", "Carrier", "Nro pax frecuente", "Apellido del pasajero", "Nombre del pasajero",
            "Partner", "Vuelo operador", "Cruce nro vlo", "Fecha uso", "Origen", "Destino",
            "Clase tarifa", "Base millas", "Bonus millas", "Extra bonus", "Promoción bonus",
            "Carrier operador", "Indicador", "Billing month", "Total millas", "Factor", "Monto Facturado USD",
            "Nro tarjeta", "Periodo facturación", "Mes facturación", "Sociedad", "Tipo de facturación"
        ]

        # Función para convertir un archivo TXT a un archivo XLSX
        def convert_txt_to_xlsx(txt_file_path, xlsx_file_path):
            # Logger para el archivo que se está procesando
            print(f'Procesando archivo: {os.path.basename(txt_file_path)}')
            with open(txt_file_path, 'r') as txt_file:
                lines = txt_file.readlines()
                data = []
                for line in lines:
                    values = [
                        line[0:1].strip(), line[1:4].strip(), line[4:20].strip(), line[20:52].strip(),
                        line[52:53].strip(), line[53:56].strip(), line[56:60].strip(), line[60:68].strip(),
                        line[68:72].strip(), line[72:75].strip(), line[75:80].strip(), line[80:87].strip(),
                        line[87:94].strip(), line[94:101].strip(), line[101:108].strip(), line[108:111].strip(),
                        line[111:112].strip(), line[112:118].strip(), line[118:160].strip()
                    ]
                    data.append(values)
                df = pd.DataFrame(data, dtype=str)  # Convertir todos los valores a cadenas de texto

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(txt_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade otras columnas
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Elimina la fila 2 (índice 1)
                df = df.drop(df.index[0])
                df = df.iloc[:-1]

                # Cambia el formato de las columnas Q, R, S, T a número entero
                def to_int(x):
                    try:
                        return int(x)
                    except (ValueError, TypeError):
                        return x

                columns_to_convert = [11, 12, 13, 14]
                for col in columns_to_convert:
                    df.iloc[:, col] = df.iloc[:, col].apply(to_int)

                # Cambia el formato de la columna O a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 6] = df.iloc[:, 6].apply(remove_leading_zeros)

                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                # Realiza el cruce de números de vuelo usando la columna O
                valores_buscados = df.iloc[:, 6].tolist()
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:
                            valor_encontrado = fila[5]
                            break
                    resultados.append(valor_encontrado)
                # Añade la columna de resultados en la posición P
                df.insert(7, 'Cruce nro vlo', resultados)

                # Función para agregar una columna con la suma de las columnas Q, R, S y T
                def agregar_columna_suma(df):
                    df.insert(19, 'Total millas', df.iloc[:, [12, 13, 14, 15]].sum(axis=1))
                    return df

                df = agregar_columna_suma(df)
                df.insert(20, 'Factor', 0.01)

                # Función para agregar una columna 'Monto facturado USD'
                def agregar_columna_monto_facturado(df):
                    df['Total millas'] = pd.to_numeric(df['Total millas'], errors='coerce')
                    df['Factor'] = pd.to_numeric(df['Factor'], errors='coerce')
                    df.insert(21, 'Monto facturado USD', df['Total millas'].astype(float) * df['Factor'].astype(float))
                    return df

                df = agregar_columna_monto_facturado(df)

                # Transformar la fecha en la columna P
                df[7] = df[7].apply(transformar_fecha_corta)

                # Asigna los encabezados.
                df.columns = encabezados

                # Guarda el DataFrame como un archivo Excel
                df.to_excel(xlsx_file_path, index=False)
                print(f'Archivo convertido y guardado como: {excel_file_path}')


        for txt_file in txt_files:
            txt_file_path = os.path.join(input_folder, txt_file)
            excel_file_path = os.path.join(output_folder, txt_file.replace('.txt', '.xlsx'))
            convert_txt_to_xlsx(txt_file_path, excel_file_path)
            print(f'Archivo {txt_file} movido a la carpeta de HISTORICOS.')

    except Exception as e:
        print(f"Error al procesar archivos: {e}")
####################################################################################################### 
def procesar_MGIB(ruta_archivo): 
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Transaction date', 'Transaction ID', 'Origen', 'Destino', 'City uplift', 'City discharge', 'Clase tarifa', 'Fare paid', 'Tax paid', 'Fecha uso',
            'Filtro 1', 'Marketing airline', 'Operating airline', 'PNR REF', 'Clase cabina', 'Vuelo marketing', 'Vuelo operador', 'Cruce nro de vlo',
            'PNR Create date', 'Program', 'Distance', 'JB Segment', 'Billing rule', 'Factor', 'Billing currency', 'Monto Facturado USD', 'Transaction status',
            'Billing partner', 'Retro indicator', 'Earn base AVIOS volume', 'Earn min AVIOS', 'Earn cabin AVIOS volume', 'Earn loyalty AVIOS volume',
            'Earn other AVIOS volume', 'Earn bonus AVIOS volume', 'Earn promo AVIOS volume', 'Total AVIOS earn', 'Base millas', 'Bonus millas', 'Extra bonus',
            'Promoción bonus', 'Other bonus', 'Billing bonus AVIOS volume', 'Billing other AVIOS volume', 'Total millas', 'Nro de ticket', 'Fecha de emisión',
            'Filtro 2', 'Pax type', 'Nombre del pasajero', 'Apellido del pasajero', 'Categoria FFP', 'Pax country', 'Nro pax frecuente', 'Batch file reference',
            'Transaction type', 'AVIOS transaction type', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador', 'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Fragmenta la columna en la posición J en dos nuevas columnas
                columna_posicion_j = 9  # Ajusta el índice según sea necesario (0-indexado)
                df.insert(columna_posicion_j + 1, 'Filtro 1', df.iloc[:, columna_posicion_j].str[10:])
                df.insert(columna_posicion_j, 'Fecha uso', df.iloc[:, columna_posicion_j].str[:10])

                # Elimina la columna original
                df.drop(df.columns[columna_posicion_j + 2], axis=1, inplace=True)

                # Fragmenta la columna en la posición AU en dos nuevas columnas
                columna_posicion_au = 45  # Índice 46 corresponde a la columna AU (0-indexado)
                df.insert(columna_posicion_au + 1, 'Filtro 2', df.iloc[:, columna_posicion_au].str[10:])
                df.insert(columna_posicion_au, 'Fecha emisión', df.iloc[:, columna_posicion_au].str[:10])

                # Elimina la columna original AU
                df.drop(df.columns[columna_posicion_au + 2], axis=1, inplace=True)

                # Convertir la columna J al formato DD-MM-YYYY
                df['Fecha uso'] = pd.to_datetime(df['Fecha uso']).dt.strftime('%d-%m-%Y')

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(xlsx_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'IB'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Cambia el formato de la columna Q a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 16] = df.iloc[:, 16].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna Q (índice 16)
                valores_buscados = df.iloc[:, 16].tolist()  # La columna Q es la columna de interés
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición R (índice 17)
                df.insert(17, 'Cruce nro de vlo', resultados)

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
####################################################################################################### 
def procesar_MGMH(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Sold to Party', 'Station', 'Bill Date', 'Service Rendered Date', 'Doc. Curr.', 'Factor', 'Bill Qty', 'Gross Amt', 'Monto Facturado USD',
            'Clase tarifa', 'Exc. Rate', 'Clase cabina', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero', 'Origen', 'Destino', 'Arrival date',
            'Fecha uso', 'Carrier ID', 'Vuelo operador', 'Sociedad', 'Periodo facturación', 'Mes facturación', 'Carrier', 'Carrier operador', 'Tipo de facturación',
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")

                # Procesa cada archivo en la ruta de lectura
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(xlsx_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Periodo facturación'] = 'Mensual'
                df['Mes facturación'] = mes_facturacion
                df['Carrier'] = 'MH'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Convierte la columna a datetime y luego a formato de fecha corta
                df['Bill Date'] = pd.to_datetime(df['Bill Date'], errors='coerce').dt.strftime('%d-%m-%Y')
                df['Service Rendered Date'] = pd.to_datetime(df['Service Rendered Date'], errors='coerce').dt.strftime('%d-%m-%Y')
                df['Departure date'] = pd.to_datetime(df['Departure date'], errors='coerce').dt.strftime('%d-%m-%Y')
                df['Arrival date'] = pd.to_datetime(df['Arrival date'], errors='coerce').dt.strftime('%d-%m-%Y')
                # Eliminar las filas donde la columna "Sold to Party" no tenga datos
                df = df.dropna(subset=['Sold to Party'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
####################################################################################################### 
def procesar_MGBA(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Transaction date', 'Transaction ID', 'Origen', 'Destino', 'City uplift', 'City discharge', 'Clase tarifa', 'Fecha uso',
            'Marketing airline', 'Operating airline', 'PNR REF', 'Clase cabina', 'Vuelo marketing', 'Vuelo operador', 'Cruce nro vlo',
            'Program', 'Distance', 'JB Segment', 'Billing rule', 'Factor', 'Billing currency', 'Monto Facturado USD', 'Billing partner',
            'Retro indicator', 'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Billing bonus AVIOS volume',
            'Billing other AVIOS volume', 'Total millas', 'Ticket number', 'Ticket sales date', 'Pax type', 'Categoria FFP', 'Nro pax frecuente',
            'Batch file reference', 'Transaction type', 'AVIOS transaction type', 'Periodo facturación', 'Mes facturación', 'Sociedad',
            'Carrier', 'Carrier operador', 'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")

                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(xlsx_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Asegurarse de que la columna 'Departure date' sea de tipo cadena
                df['Departure date'] = df['Departure date'].astype(str)

                # Cambia el formato de tiempo
                df['Departure date'] = pd.to_datetime(df['Departure date'], errors='coerce')
                df['Departure date'] = df['Departure date'].dt.strftime('%d-%m-%Y')

                # Añadir un 0 en el mes en caso de tener un solo dígito
                df['Departure date'] = df['Departure date'].apply(lambda x: '-'.join([part.zfill(2) for part in x.split('-')]))

                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 13] = df.iloc[:, 13].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 14)
                valores_buscados = df.iloc[:, 13].tolist()  # La columna O es la columna de interés
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 15)
                df.insert(14, 'Cruce nro vlo', resultados)

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MGAS(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Partner Cd', 'Opr Airline', 'Ptshp Desc', 'Status Cd', 'Nro pax frecuente', 'Nombre del pasajero', 'Categoria FFP', 'Txn Channel Cd',
            'Vuelo operador', 'Cruce nro de vuelo', 'Fecha uso', 'Clase tarifa', 'Trv Class Cd', 'Clase cabina', 'Origen', 'Destino',
            'DistanceBetweenAirports', 'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Total millas', 'Factor',
            'Net Amount', 'tax', 'Monto Facturado USD', 'Process Dt', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier',
            'Carrier operador', 'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(xlsx_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'AS'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Define las columnas que deseas eliminar si están presentes
                columnas_a_eliminar = ['MktPartnerCd', 'Counter','OrigCountry','DestCountry','MktPartnerCd']

                # Elimina las columnas que a veces están presentes y a veces no
                df = df.drop(columns=columnas_a_eliminar, errors='ignore')

                # Eliminar las filas donde la columna "A" sea igual a "Grand Total"
                if 'BillingPartnerCd' in df.columns:
                    # Eliminar las filas donde la columna 'BillingPartnerCd' sea igual a 'Grand Total'
                    df = df[df['BillingPartnerCd'] != 'Grand Total']
                else:
                    print("El dataframe no contiene la columna 'Grand Total'.")

                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 8] = df.iloc[:, 8].apply(remove_leading_zeros)


                # Realiza el cruce de números de vuelo usando la columna I (índice 8)
                valores_buscados = df.iloc[:, 8].tolist()  # La columna I es la columna de interés
                resultados = []

                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición J (índice 9)
                df.insert(9, 'Cruce nro de vuelo', resultados)

                # Añade una nueva columna con el encabezado 'tax' en la posición Z (índice 25)
                df.insert(25, 'tax', 1.075)

                        # Asegúrate de que las columnas sean numéricas
                df.iloc[:, 24] = pd.to_numeric(df.iloc[:, 24], errors='coerce')
                df.iloc[:, 25] = pd.to_numeric(df.iloc[:, 25], errors='coerce')

                # Añade una nueva columna con el encabezado 'Monto Facturado USD' en la posición AA (índice 26)
                df.insert(26, 'Monto Facturado USD', df.iloc[:, 24] * df.iloc[:, 25])

                # Define las columnas que deseas eliminar si están presentes
                columnas_a_eliminar = ['MktPartnerCd', 'Counter','OrigCountry','DestCountry','MktPartnerCd','BillingType']

                # Elimina las columnas que a veces están presentes y a veces no
                df = df.drop(columns=columnas_a_eliminar, errors='ignore')

                columnas_a_convertir = ['ProcessDt', 'Txn Dt', 'TxnDt', 'Process Dt']

                # Función para convertir el número de días a una fecha
                def convertir_numero_a_fecha(numero):
                    fecha_base = pd.Timestamp('1899-12-30')
                    if pd.isna(numero):
                        return np.nan
                    try:
                        fecha = fecha_base + pd.to_timedelta(numero, unit='D')
                        return fecha.strftime('%d-%m-%Y')
                    except Exception as e:
                        print(f"Error al convertir el número a fecha: {e}")
                        return np.nan

                def convertir_fecha(fecha):
                    try:
                        # Divide la cadena en partes
                        partes = fecha.split('-')

                        # Verificar que tenemos al menos 3 partes (día, mes, año)
                        if len(partes) != 3:
                            return None

                        # Asegúrate de que el día y el mes tengan dos dígitos
                        dia = partes[0].zfill(2)
                        mes = partes[1].zfill(2)
                        año = partes[2]

                        # Formatear la fecha como dd-mm-yyyy
                        fecha_formateada = f"{dia}-{mes}-{año}"
                        return fecha_formateada
                    except (ValueError, TypeError, IndexError, AttributeError):
                        return None


                # Aplicar la función a las columnas especificadas en el DataFrame
                def aplicar_conversion(valor):
                    if pd.isna(valor):
                        return np.nan
                    elif isinstance(valor, (float, int)):
                        return convertir_numero_a_fecha(int(valor))
                    elif isinstance(valor, (pd.Timestamp, pd.DatetimeIndex, pd._libs.tslibs.timestamps.Timestamp)):
                        # Maneja valores de tipo Timestamp o datetime
                        return valor.strftime('%d-%m-%Y')
                    elif isinstance(valor,str):
                        aux = pd.Timestamp(valor)
                        return aux.strftime('%d-%m-%Y')
                        
                    else:
                        print(f"Valor no cumple con ninguna condición: {valor}")
                        return None
    

                # Aplicar la función a las columnas especificadas y reemplazar los valores originales

                for columna in columnas_a_convertir:
                    if columna in df.columns:
                        df[columna] = df[columna].apply(lambda x: aplicar_conversion(x))
                    else:
                        print(f"Advertencia: La columna '{columna}' no se encuentra en el DataFrame.")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MGRJ(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero', 'Fecha uso', 'Vuelo operador', 'Origen', 'Destino', 'Clase cabina'
            , 'Filtro 1', 'Cupon','Filtro 2','Total millas','Clase tarifa', 'INSERT_DATE', 'Factor','Periodo facturación',
            'Monto facturado USD','Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador', 'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y conviértelos en la fecha
                nombre_sin_extension = os.path.splitext(os.path.basename(xlsx_file_path))[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'RJ'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                # Eliminar todas las filas que sean vacías en la columna B
                df = df[~df['NAME'].isnull() & (df['NAME'] != '')]

                # Fragmentar la columna E en dos columnas
                df['Filtro 1'] = df['MARKETING_FLT_NO'].str[:2]
                df['Vuelo operador'] = df['MARKETING_FLT_NO'].str[2:]

                # Insertar las nuevas columnas en la posición 4
                df.insert(4, 'Filtro 1', df.pop('Filtro 1'))
                df.insert(5, 'Vuelo operador', df.pop('Vuelo operador'))

                # Eliminar la columna original 'E'
                df = df.drop(columns=['MARKETING_FLT_NO'])
                df = df.drop(columns=['Filtro 1'])

                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 5] = df.iloc[:, 5].apply(remove_leading_zeros)

                def agregar_columna_monto_facturado(df):
                    # Asegúrate de que las columnas 'Net Amount' y 'Tax' sean numéricas
                    df['AWARD_MILES'] = pd.to_numeric(df['AWARD_MILES'], errors='coerce')
                    df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')

                    # Agrega la columna 'Monto facturado USD' en la posición AA (columna 26)
                    df.insert(16, 'Monto facturado USD', df['AWARD_MILES'].astype(float) * df['Rate'].astype(float))
                    return df

                # Llama a la función para agregar la columna 'Monto facturado USD'
                df = agregar_columna_monto_facturado(df)

                # Función para convertir la fecha de formato m/dd/yyyy a DD-MM-YYYY
                def convertir_formato_fecha(df, columna):
                    df[columna] = pd.to_datetime(df[columna], format='%m/%d/%Y').dt.strftime('%d-%m-%Y')
                    return df

                # Llamar a la función para convertir las fechas en la columna 'Fecha uso'
                df = convertir_formato_fecha(df, 'ACTIVITY_DATE')

                columnas_a_eliminar = ['']

                # Elimina las columnas que a veces están presentes y a veces no
                df = df.drop(columns=columnas_a_eliminar, errors='ignore')

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MGAY(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Nro pax frecuente', 'Categoria FFP', 'Fecha uso', 'Marketing Airline', 'Vuelo operador', 'Cruce nro vlo', 'Origen', 'Destino',
            'Clase cabina', 'Clase tarifa', 'Billing Category (N=Normal)', 'Operating Airline', 'Operating Flight No', 'Doc No', 'Billing %',
            'Cabin Class Bonus %', 'Tier Promotion %', 'Base millas', 'Bonus millas', 'Total millas', 'Factor', 'Monto Facturado USD',
            'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador', 'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'AY'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                df = df.drop([0, 1])

                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return

                df.iloc[:, 4] = df.iloc[:, 4].apply(remove_leading_zeros)
                columnas_a_eliminar = ['Ticket Number ','Coupon Number']

                # Elimina las columnas que a veces están presentes y a veces no
                df = df.drop(columns=columnas_a_eliminar, errors='ignore')

                # Realiza el cruce de números de vuelo usando la columna O (índice 14)
                valores_buscados = df.iloc[:, 4].tolist()  # La columna O es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 15)
                df.insert(5, 'Cruce nro vlo', resultados)

                   # Función para reemplazar '.' por '-' en una columna específica
                def reemplazar_punto_por_guion(df, columna):
                  df[columna] = df[columna].str.replace('.', '-')
                  return df

                # Llama a la función para reemplazar '.' por '-' en la columna ''
                df = reemplazar_punto_por_guion(df, 'Unnamed: 2')
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}") 
#######################################################################################################
def procesar_MGCX(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]
        
        # Función para eliminar filas donde la columna A esté vacía, contenga 'Date' o 'Time'
        def eliminar_filas_especificas_y_tres_mas(df):
            indices_a_eliminar = []
            i = 0
            while i < len(df):
                valor_a = str(df.iloc[i, 0]).strip()
                if pd.isnull(df.iloc[i, 0]) or valor_a == '' or valor_a == 'Date' or valor_a == 'Time':
                    indices_a_eliminar.extend(range(i, i + 4))
                    i += 4  # Saltar las filas que ya se van a eliminar
                else:
                    i += 1

            # Filtrar los índices que están fuera del rango del DataFrame
            indices_a_eliminar = [index for index in indices_a_eliminar if index < len(df)]

            # Eliminamos las filas identificadas
            df = df.drop(indices_a_eliminar).reset_index(drop=True)
            return df
        
        # Encabezados para el DataFrame
        encabezados = [
            "Nro pax frecuente","Pax type","Nombre del pasajero","Nro de ticket","Fecha uso","Porcentaje","Base millas","Bonus millas","Total millas","Status",
            "Factor","Monto Facturado USD","Periodo facturación","Mes Facturación","Sociedad","Carrier","Carrier operador","Tipo de facturación"
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Elimina las primeras 14 filas
                df = df.iloc[13:].reset_index(drop=True)

                # Elimina las últimas 6 filas
                df = df.iloc[:-6]

                # Fragmentar la columna 'A' (suponiendo que sea la primera columna)
                # Reemplaza 'A' con el nombre de la columna adecuada si es necesario
                df['frag1'] = df.iloc[:, 0].apply(lambda x: str(x)[0:11])
                df['frag2'] = df.iloc[:, 0].apply(lambda x: str(x)[11:16])
                df['frag3'] = df.iloc[:, 0].apply(lambda x: str(x)[16:50])
                df['frag4'] = df.iloc[:, 0].apply(lambda x: str(x)[50:68])
                df['frag5'] = df.iloc[:, 0].apply(lambda x: str(x)[68:78])
                df['frag6'] = df.iloc[:, 0].apply(lambda x: str(x)[78:86])
                df['frag7'] = df.iloc[:, 0].apply(lambda x: str(x)[86:104])
                df['frag8'] = df.iloc[:, 0].apply(lambda x: str(x)[104:128])
                df['frag9'] = df.iloc[:, 0].apply(lambda x: str(x)[128:135])
                df['frag10'] = df.iloc[:, 0].apply(lambda x: str(x)[135:136])

                # Eliminar espacios en columnas A hasta K usando trim
                columnas_a_trim = df.columns[:11]  # Columnas de A a K (0-indexed, por eso 11)
                for columna in columnas_a_trim:
                    df[columna] = df[columna].apply(lambda x: str(x).strip() if pd.notnull(x) else x)

                df['Factor'] = 0.007

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'CX'
                df['Carrier operador'] = 'LA'
                df['Tipo de facturación'] = 'JJ contenido en LA'

                  # Función para agregar una columna 'Monto facturado USD' con el producto de las columnas 'Net Amount' y 'Tax'
                def agregar_columna_monto_facturado(df):
                    # Asegúrate de que las columnas 'Net Amount' y 'Tax' sean numéricas
                    df['Factor'] = pd.to_numeric(df['Factor'], errors='coerce')
                    df['frag9'] = pd.to_numeric(df['frag9'], errors='coerce')

                    # Agrega la columna 'Monto facturado USD' en la posición AA (columna 26)
                    df.insert(12, 'Monto facturado USD', df['Factor'].astype(float) * df['frag9'].astype(float))
                    return df

                # Aplicar la función al DataFrame para eliminar filas vacías, con 'Date' o 'Time' y tres más
                df = eliminar_filas_especificas_y_tres_mas(df)
                df = agregar_columna_monto_facturado(df)

                # Redondear los valores de la columna 'A' a 2 decimales
                df['Monto facturado USD'] = df['Monto facturado USD'].round(2)

                # Eliminar la columna A ('Nro pax frecuente')
                df = df.drop(columns=[df.columns[0]])
                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
######## MI  ##########################################################################################
#######################################################################################################
def procesar_MIAM(ruta_archivo):

    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            ##shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIAY(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIBA(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIAS(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MICX(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIDL(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIIB(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIJL(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        # Función para eliminar los guiones bajos (_) de los encabezados
        def eliminar_guiones_bajos(df):
            df.columns = df.columns.str.replace('_', '', regex=False)
            return df
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                    # Aplica la función para eliminar guiones bajos de los encabezados
                df = eliminar_guiones_bajos(df)
            
                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'
            
                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_file_path)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]
            
                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'
            
                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion
            
                # Extrae el tercer y cuarto carácter del nombre del archivo para la columna 'Carrier'
                carrier = nombre_sin_extension[2:4]
            
                # Añade otras columnas
                df['Sociedad'] = ''
                df['Carrier'] = 'LA'
                df['Carrier operador'] = carrier
                df['Tipo de facturación'] = 'JJ contenido en LA'
            
                # Cambia el formato de la columna K (índice 11) y añádelo en la columna L (índice 10)
                def formatear_fecha(fecha):
                    if pd.isna(fecha):
                        return ''
                    fecha = str(int(fecha)).zfill(8)  # Asegura que la cadena tenga 8 caracteres, rellenando con ceros a la izquierda
                    return f'{fecha[:2]}-{fecha[2:4]}-{fecha[4:]}'
            
                # Inserta la columna formateada en la posición K (índice 10)
                df.insert(11, 'Fecha uso', df.iloc[:, 10].apply(formatear_fecha))
            
                # Verifica si el número de columnas coincide con los nuevos encabezados
                if len(df.columns) == len(encabezados):
                    # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                    df.columns = encabezados
                else:
                    print(f"Error: El número de columnas en el archivo {xlsx_files} no coincide con el número de nuevos encabezados")
                    print(f"Número de columnas en el DataFrame: {len(df.columns)}")
                    print(f"Número de nuevos encabezados: {len(encabezados)}")

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MILH(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            for xlsx_file in xlsx_files:
                xlsx_file_path = os.path.join(input_folder, xlsx_file)
                excel_file_path = os.path.join(output_folder, xlsx_file)
                
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIMH(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            for xlsx_file in xlsx_files:
                xlsx_file_path = os.path.join(input_folder, xlsx_file)
                excel_file_path = os.path.join(output_folder, xlsx_file)
                
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIQF(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            for xlsx_file in xlsx_files:
                xlsx_file_path = os.path.join(input_folder, xlsx_file)
                excel_file_path = os.path.join(output_folder, xlsx_file)
                
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIQR(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            for xlsx_file in xlsx_files:
                xlsx_file_path = os.path.join(input_folder, xlsx_file)
                excel_file_path = os.path.join(output_folder, xlsx_file)
                
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIRJ(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            for xlsx_file in xlsx_files:
                xlsx_file_path = os.path.join(input_folder, xlsx_file)
                excel_file_path = os.path.join(output_folder, xlsx_file)
                
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_MIVS(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'transaction id', 'Categoria FFP', 'transaction ff number', 'Nro pax frecuente', 'Nombre del pasajero', 'Apellido del pasajero',
            'name tx', 'lastname tx', 'billable partner name', 'marketing partner name', 'transaction date', 'Fecha uso', 'Vuelo marketing',
            'operating carrier', 'Vuelo operador', 'Clase tarifa', 'Clase cabina', 'Origen', 'Destino', 'flown distance', 'siebel points',
            'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Factor', 'currency', 'Monto Facturado USD',
            'country of residence', 'coalicion', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier', 'Carrier operador',
            'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            for xlsx_file in xlsx_files:
                xlsx_file_path = os.path.join(input_folder, xlsx_file)
                excel_file_path = os.path.join(output_folder, xlsx_file)
                
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")

    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'TRANSACTION ID', 'TRANSACTION DATE', 'COUPON CODE', 'Origen', 'Destino', 'CITY UPLIFT', 'CITY DISCHARGE',
            'Clase tarifa', 'PRE DEPARTURE CLASS', 'UPGRADE INDICATOR', 'Fecha uso', 'TAX PAID', 'FUEL SURCHARGES',
            'Monto Facturado USD', 'CURRENCY', 'MARKETING AIRLINE', 'OPERATING AIRLINE', 'BILLABLE PARTNER', 'PNR REF',
            'Clase cabina', 'PRE DEPARTURE CABIN', 'Vuelo marketing', 'Vuelo operador', 'Cruce nro vlo',
            'ORIGINAL OPERATING FLIGHT NUMBER', 'PNR CREATE DATE', 'PROGRAM', 'DISTANCE', 'IATA DISTANCE', 'IATA PRORATE',
            'BOOKING INDICATOR', 'CANCELLATION INDICATOR', 'RE-DEPOSIT DATE', 'BILLING METHOD', 'Ticket number',
            'TICKET COUPON NUMBER', 'TICKET STATUS', 'Fecha de emisión', 'DERIVED AWARD CODE', 'AWARD CODE',
            'AVIOS REDEEMED', 'RATE PER FLOWN', 'JB TYPE', 'PAX TYPE', 'MCR DISTANCE', 'PEAK INDICATOR',
            'SOURCE TRANSACTION REFERENCE', 'PROMOTION CODE', 'PROMOTIONAL AVIOS DISCOUNT', 'UNIQUE KEY',
            'Avios transaction type', 'Channel', 'Periodo facturación','Mes facturación', 'Sociedad', 'Tipo de facturación',
            'Carrier', 'Carrier operador'
        ]
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            for xlsx_file in xlsx_files:
                xlsx_file_path = os.path.join(input_folder, xlsx_file)
                excel_file_path = os.path.join(output_folder, xlsx_file)
                
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
######## PI  ##########################################################################################
#######################################################################################################
def procesar_PIBA(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'TRANSACTION ID', 'TRANSACTION DATE', 'COUPON CODE', 'Origen', 'Destino', 'CITY UPLIFT', 'CITY DISCHARGE',
            'Clase tarifa', 'PRE DEPARTURE CLASS', 'UPGRADE INDICATOR', 'Fecha uso', 'TAX PAID', 'FUEL SURCHARGES',
            'Monto Facturado USD', 'CURRENCY', 'MARKETING AIRLINE', 'OPERATING AIRLINE', 'BILLABLE PARTNER', 'PNR REF',
            'Clase cabina', 'PRE DEPARTURE CABIN', 'Vuelo marketing', 'Vuelo operador', 'Cruce nro vlo',
            'ORIGINAL OPERATING FLIGHT NUMBER', 'PNR CREATE DATE', 'PROGRAM', 'DISTANCE', 'IATA DISTANCE', 'IATA PRORATE',
            'BOOKING INDICATOR', 'CANCELLATION INDICATOR', 'RE-DEPOSIT DATE', 'BILLING METHOD', 'Ticket number',
            'TICKET COUPON NUMBER', 'TICKET STATUS', 'Fecha de emisión', 'DERIVED AWARD CODE', 'AWARD CODE',
            'AVIOS REDEEMED', 'RATE PER FLOWN', 'JB TYPE', 'PAX TYPE', 'MCR DISTANCE', 'PEAK INDICATOR',
            'SOURCE TRANSACTION REFERENCE', 'PROMOTION CODE', 'PROMOTIONAL AVIOS DISCOUNT', 'UNIQUE KEY',
            'Avios transaction type', 'Channel', 'Periodo facturación','Mes facturación', 'Sociedad', 'Tipo de facturación',
            'Carrier', 'Carrier operador'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade una nueva columna con el encabezado 'Mes facturación'
                df['Mes facturación'] = mes_facturacion

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Carrier'] = 'BA'
                df['Carrier operador'] = 'LA'


                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 22] = df.iloc[:, 22].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 22].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(23, 'Cruce nro vlo', resultados)

                def convertir_varias_columnas_a_fecha_corta(df, columnas):
                    for columna in columnas:
                        df[columna] = pd.to_datetime(df[columna], dayfirst=True).dt.strftime('%d-%m-%Y')
                    return df

                # Aplicar la función a las columnas
                columnas_a_convertir = ['TRANSACTION DATE', 'DEPARTURE DATE', 'PNR CREATE DATE', 'TICKET ISSUE DATE']
                df = convertir_varias_columnas_a_fecha_corta(df, columnas_a_convertir)

                # Bota la columna que a veces viene o a veces no
                if 'Commercial flag' in df.columns:
                    df = df.drop(columns=['Commercial flag'])

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_PICX(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'APPROVAL CODE', 'Fecha de emisión', 'Ticket number', 'CONJUNCTION TICKET NO', 'Fecha uso',
            'Partner 1', 'Vuelo operador', 'Origen', 'Destino', 'Clase tarifa', 'PROM CODE', 'PRORATE RATIO',
            'DISCOUNTED RATIO', 'Monto Facturado USD', 'PACKAGE CODE', 'PACKAGE DESCRIPTION', 'Periodo facturación',
            'Sociedad', 'Tipo de facturación', 'Mes facturación', 'Carrier', 'Carrier operador'
        ]

        def verificar_y_eliminar_fila_reporte(df):
            # Verifica si la primera fila en la columna A contiene "REPORT DATE"
            if "REPORT DATE" in str(df.iloc[0, 0]):
                # Elimina la primera fila
                df = df.iloc[1:]
                # Reasigna los encabezados utilizando la nueva primera fila
                df.columns = df.iloc[0]
                df = df[1:]
            return df

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)
                
                # Verifica y elimina la fila de reporte si es necesario
                df = verificar_y_eliminar_fila_reporte(df)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'


                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Mes facturación'] = mes_facturacion
                df['Carrier'] = 'CX'
                df['Carrier operador'] = 'LA'

                def fragmentar_columna(df, columna, nueva_columna1, nueva_columna2, separador='/'):
                    # Divide la columna en dos nuevas columnas utilizando el separador
                    nuevas_columnas = df[columna].str.split(separador, expand=True)

                    # Asigna los valores a las nuevas columnas en el DataFrame original
                    df[nueva_columna1] = nuevas_columnas[0]
                    df[nueva_columna2] = nuevas_columnas[1]
                    # Obtén la posición de la columna original
                    posicion = df.columns.get_loc(columna)

                    # Inserta las nuevas columnas en la posición de la columna original
                    df.insert(posicion, nueva_columna1, df.pop(nueva_columna1))
                    df.insert(posicion + 1, nueva_columna2, df.pop(nueva_columna2))

                    # Elimina la columna original
                    df.drop(columns=[columna], inplace=True)

                    return df

                df = fragmentar_columna(df, 'FLIGHT SECTOR', 'Origen', 'Destino')

                def convertir_a_fecha_corta(df, columna):
                    df[columna] = pd.to_datetime(df[columna]).dt.strftime('%d-%m-%Y')
                    return df

                df = convertir_a_fecha_corta(df, 'TICKET ISS DATE')
                df = convertir_a_fecha_corta(df, 'FLIGHT DATE')

                # Define las columnas que deseas eliminar si están presentes
                columnas_a_eliminar = ['MILES', 'PRORATED MILES']

                # Elimina las columnas que a veces están presentes y a veces no
                df = df.drop(columns=columnas_a_eliminar, errors='ignore')
                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_PIIB(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Partner Cd', 'Opr Airline', 'Ptshp Desc', 'Status Cd', 'Nro pax frecuente', 'Nombre del pasajero', 'Categoria FFP', 'Txn Channel Cd',
            'Vuelo operador', 'Cruce nro de vuelo', 'Fecha uso', 'Clase tarifa', 'Trv Class Cd', 'Clase cabina', 'Origen', 'Destino',
            'DistanceBetweenAirports', 'Base millas', 'Bonus millas', 'Extra bonus', 'Promoción bonus', 'Other bonus', 'Total millas', 'Factor',
            'Net Amount', 'tax', 'Monto Facturado USD', 'Process Dt', 'Periodo facturación', 'Mes facturación', 'Sociedad', 'Carrier',
            'Carrier operador', 'Tipo de facturación'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_PIJL(ruta_archivo):

    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'CUTOFF DATE', 'PARTNER', 'TYPE', 'AWARD CARRIER', 'AWARD CODE', 'AWARD DESCRIPTION',
            'Nro pax frecuente', 'Nombre del pasajero', 'Fecha de emisión', 'Nro de ticket', 'PNR',
            'ISSUE(A) / REFUND(D)', 'FLIGHT DATE', 'Fecha uso', 'Carrier operador', 'Vuelo operador',
            'cruce nro vlo', 'Origen', 'Destino', 'PTR', 'BILLING MILES', 'TPM', 'PRORATED MILES',
            'PRORATION RATE', 'Monto Facturado USD', 'DEDUCT MILES', 'Periodo facturación', 'Sociedad',
            'Tipo de facturación', 'Mes facturación','Carrier'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Mes facturación'] = mes_facturacion
                df['Carrier'] = 'JL'

                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 14] = df.iloc[:, 14].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 14].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(15, 'Cruce nro vlo', resultados)

                def convertir_y_añadir_fecha_corta(df, columna_origen, nueva_columna,posicion):
                    # Convertir la columna de origen a tipo datetime y formatear a 'dd-mm-yyyy'
                    df[nueva_columna] = pd.to_datetime(df[columna_origen], format='%Y%m%d').dt.strftime('%d-%m-%Y')

                    # Insertar la nueva columna en la posición especificada
                    cols = list(df.columns)
                    cols.insert(posicion, cols.pop(cols.index(nueva_columna)))
                    df = df[cols]

                    return df

                df = convertir_y_añadir_fecha_corta(df,'FLIGHT DATE','Fecha uso', 13)

                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_PIQR(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            'Sending Carrier','Program','Member ID','Carrier','Ticket number','Cupón','Fecha de emisión','Coupon Status','Farebasis',
            'Designator','Nombre del pasajero','Apellido del pasajero','Marketing Carrier','Vuelo marketing','Operating Carrier','Vuelo operador',
            'Cruce nro vlo','Fecha uso','Origen','Destino','Clase cabina','Clase tarifa','Itinerary','Award Code','Tkt Status','Monto Facturado USD',
            'Billing Currency','Base millas','Billable Partner','Billing Month','TPM','PNR','IATA TPM','Total TPM','Rejection Reason Code',
            'Billing Status','Remarks','PNR Creation Date','Periodo facturación','Sociedad','Tipo de facturación','Mes facturación','Carrier operador'
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Mes facturación'] = mes_facturacion
                df['Carrier'] = 'QR'

                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 15] = df.iloc[:, 15].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 15].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(16, 'Cruce nro vlo', resultados)

                # Definir la función para convertir números a fechas
                def convertir_a_fecha(numero):
                    fecha_base = pd.to_datetime('1899-12-30')
                    if pd.isna(numero) or isinstance(numero, pd.Timestamp): # Check if it's a Timestamp and return NaN if so
                        return np.nan
                    return (fecha_base + pd.to_timedelta(numero, unit='D')).strftime('%d-%m-%Y')

                # Asumir que df es tu DataFrame
                # Aplicar la función a las columnas 'Tkt Issue Date' y 'FlightDate'
                df['Tkt Issue Date '] = df['Tkt Issue Date '].apply(convertir_a_fecha)
                df['FlightDate'] = df['FlightDate'].apply(convertir_a_fecha)
                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_PIAY(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            "Transaction ID", "Sequence Number", "Nro pax frecuente", "Farebasis",
            "Marketing Airline Code", "Vuelo operador", "Cruce nro vlo", "Origen",
            "Destino", "Fecha uso", "Clase tarifa", "Fecha de emisión",
            "Ticket number", "Cupón", "Redeemed Points", "Base millas",
            "Weighted Miles", "Proration Percentage", "Factor", "Monto Facturado USD",
            "Periodo facturación", "Sociedad", "Tipo de facturación", "Mes facturación",
            "Carrier", "Carrier operador"
        ]

        def establecer_encabezados_correctos(df):
            # Itera sobre cada fila del DataFrame
            for index, row in df.iterrows():
                # Verifica si "Transaction ID" está en la primera columna de la fila actual
                if "Transaction ID" in str(row.iloc[0]):
                    # Elimina todas las filas anteriores a la fila que contiene "Transaction ID"
                    df = df.iloc[index:]
                    # Usa esta fila como los encabezados del DataFrame
                    df.columns = df.iloc[0]
                    # Elimina la fila de encabezados del cuerpo de datos
                    df = df[1:]
                    break
            return df

        # Función para convertir la fecha de 'dd.mm.yyyy' a 'dd-mm-yyyy'
        def convertir_fecha(fecha):
            if isinstance(fecha, str):
                return fecha.replace('.', '-')
            return fecha
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Verifica y elimina la fila de reporte si es necesario
                df = establecer_encabezados_correctos(df)

                # Añade una nueva columna al final del DataFrame con el encabezado 'Periodo facturación'
                df['Periodo facturación'] = 'Mensual'

                # Extrae los últimos 8 caracteres del nombre del archivo antes del '.xlsx' y convírtelos en la fecha
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Elimina la última fila del DataFrame
                df = df.iloc[:-1]

                # Añade otras columnas
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Mes facturación'] = mes_facturacion
                df['Carrier'] = 'AY'
                df['Carrier operador'] = 'LA'

                # Cambia el formato de la columna I a número entero y elimina ceros a la izquierda
                def remove_leading_zeros(x):
                    try:
                        return str(int(x))
                    except (ValueError, TypeError):
                        return x

                df.iloc[:, 5] = df.iloc[:, 5].apply(remove_leading_zeros)

                # Realiza el cruce de números de vuelo usando la columna O (índice 22)
                valores_buscados = df.iloc[:, 5].tolist()  # La columna 22 es la columna de interés
                resultados = []
                for valor_buscado in valores_buscados:
                    valor_encontrado = ""
                    for fila in datos_origen:
                        if fila[4] == valor_buscado:  # Comparando con valores en la columna E de hoja origen
                            valor_encontrado = fila[5]  # Obtener valor de la columna F de hoja origen
                            break
                    resultados.append(valor_encontrado)

                # Añade la columna de resultados en la posición P (índice 23)
                df.insert(6, 'Cruce nro vlo', resultados)

                # Elimina la columna O (índice 14)
                df.drop(df.columns[14], axis=1, inplace=True)

                # Aplica la conversión de fecha a toda la columna de interés
                df['Flight Date'] = df['Flight Date'].apply(convertir_fecha)

                # Si deseas convertir la columna a tipo datetime
                df['Date Award Issued'] = df['Date Award Issued'].apply(convertir_fecha)
                
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
######## PG ###########################################################################################
#######################################################################################################
def procesar_PGDL(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]

        # Encabezados para el DataFrame
        encabezados = [
            "Partner", "Ticket number", "Cupón", "Fecha uso", "Origen",
            "Destino", "Farebasis", "Paid Class", "Flown Class", "GCMs", "Monto Facturado USD",
            "RPM", "Unbillable miles", "Unbillable Amount", "Notes", "Validación",
            "Periodo facturación", "Sociedad", "Tipo de facturación", "Mes facturación",
            "Carrier", "Carrier operador"
        ]

        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                def convertir_fecha(fecha):
                    if isinstance(fecha, str):
                        return fecha.replace('.', '-')
                    return fecha
                
                nombre_sin_extension = os.path.splitext(xlsx_files)[0]
                fecha_str = nombre_sin_extension[-8:]  # Los últimos 8 caracteres representan la fecha
                año = fecha_str[:4]
                mes = fecha_str[4:6]
                día = fecha_str[6:]

                # Formatea la fecha como 'dd-mm-yyyy'
                mes_facturacion = f'{día}-{mes}-{año}'

                # Elimina la última fila del DataFrame
                df = df.iloc[:-1]

                # Añade otras columnas solo si no están presentes
                if 'RPM' not in df.columns:
                    df['RPM'] = ''

                if 'Unbillable miles' not in df.columns:
                    df['Unbillable miles'] = ''

                if 'Unbillable Amount' not in df.columns:
                    df['Unbillable Amount'] = ''

                if 'Notes' not in df.columns:
                    df['Notes'] = ''

                if 'Validation ' not in df.columns:
                    df['Validación'] = ''

                df['Periodo facturación'] = 'Mensual'
                df['Sociedad'] = ''
                df['Tipo de facturación'] = 'JJ contenido en LA'
                df['Mes facturación'] = mes_facturacion
                df['Carrier'] = 'LA'
                df['Carrier operador'] = 'DL'


                def transformar_fecha_corta(fecha_str):
                    fecha_str = str(fecha_str)  # Convertir a cadena
                    año = fecha_str[:4]
                    mes = fecha_str[4:6]
                    día = fecha_str[6:8]
                    return f'{día}-{mes}-{año}'

                df['Flight Date'] = df['Flight Date'].apply(transformar_fecha_corta)
   
                # Reemplaza los encabezados del DataFrame con los nuevos encabezados
                df.columns = encabezados

                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
def procesar_TMP(ruta_archivo):
    try:
        # Autentica al usuario
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)

        # Definiciones de rutas
        input_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/INPUT'
        output_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/OUTPUT'
        processed_folder = '/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP/OLA/HISTORICO'

        # ID del archivo de Google Sheets que se usará para el cruce
        sheet_id_origen = '1J2Inx7za56hyQsbYpczueu7KQtOxx8nYyDHnB7cIitE'
        sheet_name_origen = 'Datos'

        # Asegúrate de que la carpeta de salida exista
        os.makedirs(output_folder, exist_ok=True)

        # Abre la hoja de cálculo de origen y selecciona la hoja
        hoja_origen = gc.open_by_key(sheet_id_origen).worksheet(sheet_name_origen)
        datos_origen = hoja_origen.get_all_values()

        # Obtener la lista de archivos Excel en la carpeta de entrada
        xlsx_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

        # Función para renombrar los encabezados
        def renombrar_encabezados(columnas):
            return [col.replace('_', ' ') for col in columnas]
        
        def convert_xlsx_to_excel(xlsx_file_path, excel_file_path):
            try:
                logging.info(f"Procesando archivo: {os.path.basename(xlsx_file_path)}")
                
                # Lee el archivo de Excel y conviértelo en un DataFrame de pandas
                df = pd.read_excel(xlsx_file_path, dtype=str)

                # Renombrar los encabezados
                df.columns = renombrar_encabezados(df.columns)

                # Guardar el archivo como XLSX en la carpeta OUTPUT
                nombre_archivo_xlsx = xlsx_files.replace('.csv', '.xlsx')
                ruta_archivo_xlsx = os.path.join(output_folder, nombre_archivo_xlsx)
                
                
                # Guarda el DataFrame modificado en un nuevo archivo en la ruta de guardado
                df.to_excel(excel_file_path, index=False)
                logging.info(f'Archivo convertido y guardado como: {excel_file_path}')

            except Exception as e:
                logging.error(f"Error al procesar el archivo {os.path.basename(xlsx_file_path)}: {e}")

        def move_file_to_processed(xlsx_file_path):
            #shutil.move(xlsx_file_path, os.path.join(processed_folder, os.path.basename(xlsx_file_path)))
            logging.info(f'Archivo {os.path.basename(xlsx_file_path)} movido a la carpeta de Historico.')

        for xlsx_file in xlsx_files:
            xlsx_file_path = os.path.join(input_folder, xlsx_file)
            excel_file_path = os.path.join(output_folder, os.path.splitext(xlsx_file)[0] + '.xlsx')
            convert_xlsx_to_excel(xlsx_file_path, excel_file_path)
            move_file_to_processed(xlsx_file_path)

    except Exception as e:
        logging.error(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
        print(f"Error al procesar archivos en la ruta {ruta_archivo}: {e}")
#######################################################################################################
######## INTRAHOLDING #################################################################################
#######################################################################################################  
#######################################################################################################
######## MG ###########################################################################################
#######################################################################################################


