import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuración de la carpeta a monitorear
MONITORED_FOLDER = "C:/sftp"  # Cambia esto a tu carpeta local
OUTPUT_DIR = "D:/PUCP/chamba/testFolder/output"  # Carpeta para guardar archivos procesados
C_PROGRAM_PATH = "D:/PUCP/chamba/testFolder/DWDataReader/DWDataReaderAdapted.exe"  # Ruta al programa en C
COLUMNAS = "0,1,2,3,4,5,6,7"  # Configuración de columnas para el procesamiento

class DXDFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".dxd"):
            print(f"Archivo .dxd detectado: {event.src_path}")
            process_penultimate_file()

def process_files(input_file, output_folder, columnas):
    try:
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        print(f"Procesando archivo: {input_file}")
        result = subprocess.run(
            [C_PROGRAM_PATH, input_file, columnas, output_folder],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"Error ejecutando el programa en C:\n{result.stderr}")
        else:
            print(f"Archivo procesado exitosamente:\n{result.stdout}")
    except Exception as e:
        print(f"Error procesando el archivo {input_file}: {e}")

def process_penultimate_file():
    # Obtén todos los archivos .dxd ordenados por fecha de creación
    dxd_files = sorted(
        [os.path.join(MONITORED_FOLDER, f) for f in os.listdir(MONITORED_FOLDER) if f.endswith(".dxd")],
        key=os.path.getctime
    )

    if len(dxd_files) < 2:
        print("No hay suficientes archivos para procesar el penúltimo.")
        return

    # Selecciona el penúltimo archivo
    penultimate_file = dxd_files[-2]
    print(f"Procesando el penúltimo archivo: {penultimate_file}")

    # Procesa el archivo
    process_files(penultimate_file, OUTPUT_DIR, COLUMNAS)

def main():
    print(f"Monitoreando la carpeta: {MONITORED_FOLDER}")
    event_handler = DXDFileHandler()
    observer = Observer()
    observer.schedule(event_handler, MONITORED_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)  # Mantener el programa corriendo
    except KeyboardInterrupt:
        print("Deteniendo el monitoreo...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
