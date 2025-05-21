import pandas as pd

archivo = "data/20-07-01 20-12-31.xls"  # Ajusta el nombre si es distinto

# Leer sin encabezado
df = pd.read_excel(archivo, header=None)

# Ver las primeras 15 filas
print(df.head(15))

# Ver nombres de hojas
xls = pd.ExcelFile(archivo)
print("\nHojas disponibles:", xls.sheet_names)
