import os
import time
import subprocess
import paramiko
from queue import Queue
from threading import Thread

# Configuración del servidor SFTP
SFTP_HOST = "200.16.2.59"      # Dirección IP o nombre del servidor SFTP
SFTP_PORT = 22                # Puerto estándar de SFTP
SFTP_USER = "WS_Monitoreo"    # Nombre de usuario
SFTP_PASS = "patrimonio2018"  # Contraseña
REMOTE_DIR = "/dir/L" 
OUTPUT_DIR = "D:/PUCP/chamba/testFolder/output"
TEMP_DIR = "D:/PUCP/chamba/testFolder"
COLUMNAS = "0,1,2,3,4,5,6,7"

# Ruta al programa en C
C_PROGRAM_PATH = "D:/PUCP/chamba/testFolder/DWDataReader/DWDataReaderAdapted.exe"

memory_queue = []
previous_files = set()
file_queue = Queue()


def connect_sftp():
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp
    except Exception as e:
        print(f"Error conectando al servidor SFTP: {e}")
        raise


def initialize_detected_files():
    global previous_files
    try:
        sftp = connect_sftp()
        try:
            sftp.chdir(REMOTE_DIR)
            previous_files = set(file for file in sftp.listdir() if file.endswith(".dxd"))
            print("Archivos iniciales detectados:")
            for file in previous_files:
                print(f" - {file}")
        finally:
            sftp.close()
    except Exception as e:
        print(f"Error al cargar archivos iniciales: {e}")


def memory_handler(file):
    """
    Maneja la memoria simulada con archivos descargados.
    Procesa el primer archivo cuando llegue el segundo archivo.
    """
    global memory_queue
    memory_queue.append(file)
    print(f"Archivo añadido a memory_queue: {file}")
    print(f"Estado actual de memory_queue: {memory_queue}")

    if len(memory_queue) >= 2:  # Procesar cuando haya al menos dos archivos
        # Extraer y procesar el archivo más antiguo
        file_to_process = memory_queue.pop(0)
        file_queue.put((file_to_process, OUTPUT_DIR, COLUMNAS))
        print(f"Archivo movido de memory_queue a file_queue: {file_to_process}")

def update_file(file_name):
    """
    Descarga la última versión del archivo desde el servidor SFTP.
    """
    try:
        sftp = connect_sftp()
        try:
            sftp.chdir(REMOTE_DIR)
            local_path = os.path.join(TEMP_DIR, file_name)
            print(f"Actualizando archivo: {file_name}")
            sftp.get(file_name, local_path)  # Descarga el archivo nuevamente
            print(f"Archivo actualizado: {local_path}")
        finally:
            sftp.close()
    except Exception as e:
        print(f"Error al actualizar el archivo {file_name}: {e}")



def detect_new_files():
    global previous_files
    try:
        sftp = connect_sftp()
        try:
            sftp.chdir(REMOTE_DIR)
            current_files = set(file for file in sftp.listdir() if file.endswith(".dxd"))
            new_files = current_files - previous_files

            for file in new_files:
                print(f"Nuevo archivo detectado: {file}")
                local_path = os.path.join(TEMP_DIR, file)
                sftp.get(file, local_path)
                print(f"Archivo descargado: {local_path}")
                memory_handler(local_path)

            previous_files = current_files
        finally:
            sftp.close()
    except Exception as e:
        print(f"Error al detectar nuevos archivos: {e}")


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


def process_from_queue():
    while True:
        task = file_queue.get()
        if task is None:
            break
        input_file, output_folder, columnas = task
        try:
            # Extraer solo el nombre del archivo
            file_name = os.path.basename(input_file)

            # Actualizar el archivo antes de procesarlo
            update_file(file_name)

            # Procesar el archivo actualizado
            process_files(input_file, output_folder, columnas)

            # Eliminar el archivo después de procesarlo
            if os.path.exists(input_file):
                os.remove(input_file)
                print(f"Archivo eliminado después del procesamiento: {input_file}")
        except Exception as e:
            print(f"Error al procesar archivo {input_file}: {e}")
        finally:
            file_queue.task_done()



if __name__ == "__main__":
    print("Seleccione una opción:")
    print("1. Monitorear el servidor SFTP.")
    print("2. Procesar todos los archivos .dxd en la carpeta local.")
    choice = input("Ingrese su elección (1 o 2): ")

    worker_thread = Thread(target=process_from_queue, daemon=True)
    worker_thread.start()

    if choice == "1":
        try:
            initialize_detected_files()
            print("Iniciando monitoreo del servidor SFTP...")
            while True:
                detect_new_files()
                time.sleep(10)
        except KeyboardInterrupt:
            print("Deteniendo monitoreo...")
            file_queue.put(None)
            worker_thread.join()
    elif choice == "2":
        print("Procesando archivos locales...")
        local_files = [f for f in os.listdir(TEMP_DIR) if f.endswith(".dxd")]
        for file in local_files:
            file_queue.put((os.path.join(TEMP_DIR, file), OUTPUT_DIR, COLUMNAS))
        file_queue.put(None)
        worker_thread.join()
    else:
        print("Opción inválida. Terminando el programa.")
        file_queue.put(None)
        worker_thread.join()
