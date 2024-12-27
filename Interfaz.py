#@title Escoger Opcion { display-mode: "form" }

import logging
from IPython.display import display, clear_output
import ipywidgets as widgets
import sys
# Agregar el directorio al sys.path para importar el módulo de lógica
sys.path.append('/content/drive/Shareddrives/Proyectos/Colabs/Automatizacion_Archivos_FFP')

import Logicas_Automatizacion as logica  # Importa el archivo de lógicas de automatización
logging.basicConfig(filename='selecciones.log', level=logging.INFO, format='%(asctime)s - %(message)s')
#@title Montar Drive { display-mode: "form" }
#Cuando monta tu unidad de drive te saldra MOUNTED.

class Automator:
    def __init__(self, carriers_por_proceso):
        self.carriers_por_proceso = carriers_por_proceso
        self.setup_widgets()

    def setup_widgets(self):
        self.tipo_widget = widgets.Dropdown(
            options=self.carriers_por_proceso.keys(),
            value='OLA',
            description='Tipo:',
        )

        self.accion_widget = widgets.Dropdown(
            options=["1. Procesar Archivos", "2. Consolidar"],
            value="1. Procesar Archivos",
            description='Acción:',
        )

        self.proceso_widget = widgets.Dropdown(
            options=sorted(self.carriers_por_proceso[self.tipo_widget.value].keys()),
            value='MG',
            description='Proceso:',
        )

        self.tipo_widget.observe(self.update_proceso_options, 'value')
        self.accion_widget.observe(self.update_visibility, 'value')
        self.update_visibility()

        display(self.tipo_widget, self.accion_widget, self.proceso_widget)

        self.button = widgets.Button(description="Ejecutar")
        self.button.on_click(self.on_button_clicked)
        display(self.button)

    def update_proceso_options(self, change):
        self.proceso_widget.options = sorted(self.carriers_por_proceso[change['new']].keys())

    def update_visibility(self, *args):
        if self.accion_widget.value == "2. Consolidar":
            self.proceso_widget.layout.display = 'none'
        else:
            self.proceso_widget.layout.display = ''

    def on_button_clicked(self, b):
        clear_output()
        display(self.tipo_widget, self.accion_widget, self.proceso_widget, self.button)

        Tipo = self.tipo_widget.value
        Accion = self.accion_widget.value
        Proceso = self.proceso_widget.value

        print("Acción:", Accion)
        print("Tipo:", Tipo)
        print("Proceso:", Proceso)

        if Accion == "1. Procesar Archivos":
            print("Procesando archivos...")
            logica.procesar_archivos(Tipo, Proceso, self.carriers_por_proceso[Tipo][Proceso])
            print("Procesamiento completado.")
            logging.info(f"Acción: {Accion}, Tipo: {Tipo}, Proceso: {Proceso}")

        elif Accion == "2. Consolidar":
            print("Consolidando archivos...")
            # Aquí puedes agregar la llamada a la función de consolidar si es necesario
            print("Consolidación completada.")

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

automatizador = Automator(carriers_por_proceso)