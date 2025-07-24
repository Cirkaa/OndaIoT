import serial
import pymongo
import re
from datetime import datetime
import time

# --- Serial Port Configuration ---
SERIAL_PORT = '/dev/ttyS0'
BAUD_RATE = 115200

# --- MongoDB Configuration ---

MONGO_URI = "mongodb://admin:adminpassword@localhost:27017/" # Con usuario y contraseña
DATABASE_NAME = "stm32_data"
COLLECTION_NAME = "random_numbers"
BATCH_SIZE = 100

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
                # print(f"Received: {line}") # Puedes descomentar para debugging

                match = re.search(r'Numero aleatorio: (\d+)', line)
                if match:
                    try:
                        number = int(match.group(1))
                        timestamp = datetime.now()
                        data_batch.append({
                            "number": number,
                            "timestamp": timestamp
                        })
                        # print(f"Added number {number} to batch ({len(data_batch)}/{BATCH_SIZE})") # Puedes descomentar

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
                        print(f"Could not convert '{match.group(1)}' to an integer.")
                # else: # Puedes descomentar para depurar líneas no esperadas
                    # print(f"No number found in line: '{line}'")

            time.sleep(0.01)
    except KeyboardInterrupt:
        print("\nScript terminated by user.")
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
