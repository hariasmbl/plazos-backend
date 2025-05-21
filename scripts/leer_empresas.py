import os

def previsualizar_txt(path, n=10):
    if not os.path.exists(path):
        print("⚠️ El archivo no existe en la ruta especificada.")
        return

    with open(path, 'r', encoding='utf-8') as f:
        for i in range(n):
            print(f.readline().strip())

# Ruta absoluta al archivo
if __name__ == "__main__":
    ruta = r"C:\Users\Damsoft\Desktop\Plazos\Otros docs\PUB_EMPRESAS.txt"  # Ajusta si el nombre es distinto
    previsualizar_txt(ruta)
