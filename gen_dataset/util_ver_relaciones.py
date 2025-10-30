import pandas as pd

print("--- Viendo todas las relaciones únicas ---")

try:
    df = pd.read_csv("tripletas_completas.csv")
    relaciones_unicas = df['relacion'].unique()
    
    print(f"Encontradas {len(relaciones_unicas)} relaciones únicas en tu dataset:")
    
    for r in relaciones_unicas:
        print(f"  - '{r}'")
        
    print("\nCopia estas relaciones en el 'MAPA_PREGUNTAS' de tu script '2_generador_qa.py'")
    
except FileNotFoundError:
    print("Error: No se encuentra 'tripletas_completas.csv'. Ejecuta el script 1 primero.")