from google.cloud import storage

def create_bucket(bucket_name, location="us-central1"):
    print("Ejecutando script de creación de bucket...")

    client = storage.Client()

    # Verificar si ya existe
    if client.lookup_bucket(bucket_name):
        print(f"El bucket '{bucket_name}' ya existe.")
        return

    # Crear bucket correctamente con ubicación y storage class
    bucket = storage.Bucket(client, name=bucket_name)
    bucket.storage_class = "STANDARD"

    new_bucket = client.create_bucket(bucket, location=location)

    print(
        f"Bucket creado: {new_bucket.name} | Región: {new_bucket.location} | Clase: {new_bucket.storage_class}"
    )

if __name__ == "__main__":
    create_bucket("data-lake-ar-raw")
