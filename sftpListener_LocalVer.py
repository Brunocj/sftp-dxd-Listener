import os
import time
import subprocess
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuración de la carpeta a monitorear
MONITORED_FOLDER = "C:/sftp"  # Cambia esto a tu carpeta local
OUTPUT_DIR = "C:/sftp/output"  # Carpeta para guardar archivos procesados
C_PROGRAM_PATH = "D:/PUCP/chamba/testFolder/DWDataReader/DWDataReaderAdapted.exe"  # Ruta al programa en C
COLUMNAS = "0,1,2,3,4,5,6,7"  # Configuración de columnas para el procesamiento
API_ENDPOINT = "http://localhost:5000/uploadToDb"
AUTH_HEADER = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoidXBsb2FkZXIiLCJwZXJtaXNzaW9ucyI6WyJ1cGxvYWQiXSwiaWF0IjoxNzM1NjgwMTg5fQ.joHGO4pFHbkKoHVATzo0HcEwqE82Fg0oKQ2midlP8RY"

# Bandera global para decidir si se suben los archivos
UPLOAD_TO_DB = False

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
            return True
    except Exception as e:
        print(f"Error procesando el archivo {input_file}: {e}")
    return False

def upload_to_db(file_path):
    """
    Envía la ruta del archivo al endpoint mediante una solicitud HTTP POST.
    """
    try:
        headers = {
            "Authorization": AUTH_HEADER,
            "Content-Type": "application/json"
        }
        data = {"filePath": file_path}
        print(f"Enviando archivo a la base de datos:\nHeaders: {headers}\nBody: {data}")
        response = requests.post(API_ENDPOINT, json=data, headers=headers)
        response.raise_for_status()
        print(f"Archivo subido exitosamente: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error al subir el archivo a la base de datos: {e}")

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
    if process_files(penultimate_file, OUTPUT_DIR, COLUMNAS):
        # Busca el archivo resultante en el directorio de salida
        file_to_search = os.path.splitext(os.path.basename(penultimate_file))[0]
        matching_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(file_to_search) and f.endswith(".txt")]

        if matching_files:
            output_file_path = os.path.join(OUTPUT_DIR, matching_files[0])
            print(f"Archivo procesado generado: {output_file_path}")
            
            # Subir el archivo a la base de datos si está habilitado
            if UPLOAD_TO_DB:
                upload_to_db(output_file_path)
            else:
                print("El archivo no se subió a la base de datos.")
        else:
            print(f"No se encontró ningún archivo generado que coincida con: {file_to_search}")

def main():
    global UPLOAD_TO_DB

    print("¿Deseas subir automáticamente los archivos procesados a la base de datos?")
    user_choice = input("Escribe 's' para sí o 'n' para no: ").strip().lower()
    UPLOAD_TO_DB = user_choice == 's'

    if UPLOAD_TO_DB:
        print("Los archivos procesados se subirán automáticamente a la base de datos.")
    else:
        print("Los archivos procesados NO se subirán automáticamente a la base de datos.")

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
