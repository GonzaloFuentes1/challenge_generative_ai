import pandas as pd
import os
from langdetect import detect, LangDetectException 

print("--- Iniciando Script para mezclar csvs ---")

archivo_canon = "canon_basic.csv"
archivo_pintores = "pintores_latam_4671Entities.csv"
archivo_salida = "tripletas_completas.csv"

columnas_estandar = ['entidad', 'relacion', 'valor']

if not os.path.exists(archivo_canon) or not os.path.exists(archivo_pintores):
    print(f"Error: Asegúrate de tener los archivos .csv")
else:
    df_canon = pd.read_csv(archivo_canon, index_col=False)
    df_pintores = pd.read_csv(archivo_pintores, index_col=False)

    print(f"Columnas en {archivo_canon}: {df_canon.columns.tolist()}")
    print(f"Columnas en {archivo_pintores}: {df_pintores.columns.tolist()}")

    try:
        df1 = df_canon[columnas_estandar].copy()

        columnas_pintores = ['entidad', 'relacion', 'valor_es']
        df2 = df_pintores[columnas_pintores].copy()

        df2 = df2.rename(columns={'valor_es': 'valor'})

        df_completo = pd.concat([df1, df2], ignore_index=True)

        df_completo = df_completo.dropna()
        df_completo = df_completo.drop_duplicates()

        def es_espanol(texto):
            
            if not isinstance(texto, str) or len(texto.strip()) < 3:
                 return False
            try:
                return detect(texto) == 'es'
            except LangDetectException:
                return False
            except Exception as e:
                return False
            
        df_completo['es_espanol'] = df_completo['valor'].apply(es_espanol)
        
        df_filtrado_es = df_completo[df_completo['es_espanol'] == True].copy()

        df_filtrado_es = df_filtrado_es.drop(columns=['es_espanol'])
        
        print(f"Filtrado completo. Se conservaron {len(df_filtrado_es)} tripletas en español.")

        df_filtrado_es.to_csv(archivo_salida, index=False, encoding='utf-8')

        print(f"\n¡Éxito! Se estandarizaron y mezclaron los CSVs.")
        print(f"Se guardó el archivo '{archivo_salida}' con {len(df_filtrado_es)} tripletas únicas,\
               limpias y en español.")
        print("-------------------------------------------------")
    
    except KeyError as e:
        print(f"\nError: Falta una columna estándar después de renombrar.")
        print(f"Asegúrate de que ambos CSVs tengan (o se puedan renombrar a): {columnas_estandar}")
        print(f"Detalle del error: {e}")
        print("-------------------------------------------------")