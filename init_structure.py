from google.cloud import storage
from datetime import datetime

def create_prefix(client, bucket_name, prefix):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(prefix + "/")  # Objeto vacío para simular carpeta
    blob.upload_from_string("")       # Crea el prefijo en GCS
    print(f"Creado: {prefix}/")

def initialize_structure(bucket_name):
    client = storage.Client()

    current_year = datetime.now().year

    base_paths = ["raw/ipc", "raw/inpc"]

    for base in base_paths:
        for year in range(2010, current_year + 1):
            for month in range(1, 13):
                prefix = f"{base}/{year}/{month:02d}"
                create_prefix(client, bucket_name, prefix)

    print("Estructura inicial creada.")

if __name__ == "__main__":
    initialize_structure("data-lake-ar-raw")
