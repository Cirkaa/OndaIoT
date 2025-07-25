
import serial
import pymongo
import re
from datetime import datetime
import time

# --- Serial Port Configuration ---
SERIAL_PORT = '/dev/serial0'
BAUD_RATE = 115200

# --- MongoDB Configuration ---
MONGO_URI = "mongodb://admin:adminpassword@mongodb:27017/" # Con usuario y contraseña
DATABASE_NAME = "stm32_data"
COLLECTION_NAME = "random_numbers"
BATCH_SIZE = 1000

def main():
    print(f"Attempting to connect to serial port: {SERIAL_PORT} at {BAUD_RATE} baud...")
    try:
        ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print("Serial port opened successfully.")
    except serial.SerialException as e:
        print(f"Error opening serial port: {e}")
        print("Please check if the serial port is correct and not in use.")
        print("For Raspberry Pi, you might need to disable serial console:")
        print("  sudo raspi-config -> 3 Interface Options -> P6 Serial Port -> No (login shell), Yes (serial port hardware)")
        return

    print(f"Attempting to connect to MongoDB: {MONGO_URI}...")
    client = None # Inicializa client a None para el finally
    try:
        # Conectar a MongoDB con usuario y contraseña
        client = pymongo.MongoClient(MONGO_URI)
        # Prueba una operación simple para verificar la autenticación
        client.admin.command('ping') # Intenta hacer un ping a la base de datos admin
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Successfully connected to MongoDB database '{DATABASE_NAME}' and collection '{COLLECTION_NAME}'.")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error connecting to MongoDB: {e}")
        print("Please ensure your MongoDB Docker container is running and accessible.")
        print("Also, check your MONGO_URI, username, and password.")
        return
    except pymongo.errors.OperationFailure as e:
        print(f"MongoDB Authentication Error: {e}")
        print("Please check your username and password in MONGO_URI.")
        return


    data_batch = []
    print(f"Starting to receive data from STM32. Batching every {BATCH_SIZE} numbers...")

    try:
        while True:
            if ser.in_waiting > 0:
                line = ser.readline().decode('utf-8').strip()
                # Descomenta la siguiente línea para depurar y ver lo que recibe la Raspberry Pi
                # print(f"Received RAW: '{line}'")

                # *** CAMBIO CRUCIAL AQUÍ: Nueva expresión regular para tres números ***
                match = re.search(r'Numeros aleatorios: (\d+), (\d+), (\d+)', line)
                if match:
                    try:
                        # Captura los tres números
                        number1 = int(match.group(1))
                        number2 = int(match.group(2))
                        number3 = int(match.group(3))

                        timestamp = datetime.now()
                        data_batch.append({
                            "numbers": [number1, number2, number3], # Almacena los tres números en un array
                            "timestamp": timestamp
                        })
                        # Descomenta la siguiente línea para depurar y ver qué se añade al lote
                        # print(f"Added numbers {number1}, {number2}, {number3} to batch ({len(data_batch)}/{BATCH_SIZE})")

                        if len(data_batch) >= BATCH_SIZE:
                            print(f"\n--- Batch size {BATCH_SIZE} reached! Inserting into MongoDB ---")
                            try:
                                collection.insert_many(data_batch)
                                print(f"Successfully inserted {len(data_batch)} documents into MongoDB.")
                                data_batch = [] # Clear the batch
                            except Exception as e:
                                print(f"Error inserting into MongoDB: {e}")
                            print("--- Waiting for next batch ---\n")
                    except ValueError:
                        print(f"Could not convert one of the numbers in line: '{line}' to an integer. Skipping.")
                # Descomenta el siguiente bloque 'else' para depurar líneas no esperadas por el regex
                # else:
                #     print(f"No match found for line: '{line}'")

            time.sleep(0.01) # Pequeño retardo para no saturar la CPU
    except KeyboardInterrupt:
        print("\nScript terminated by user (Ctrl+C).")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if ser.is_open:
            ser.close()
            print("Serial port closed.")
        if client:
            client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    main()
