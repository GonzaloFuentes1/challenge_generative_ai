import pandas as pd
import json
import os
import sys
try:
    from unidecode import unidecode # Aún necesario para normalizar blacklist/mapa
except ImportError:
    print("Error: Necesitas 'unidecode'. Instálalo con: pip install unidecode")
    exit()

print("--- Iniciando Script: Generador de QA Individual (SIN filtro idioma) ---")

# --- MAPA Y BLACKLIST (Asegúrate de que estén completos) ---
MAPA_PREGUNTAS = {
    # Relaciones en español (de pintores_latam)
    'lugar de nacimiento': 'el lugar de nacimiento',
    'instancia de': 'un tipo de',
    'lugar de fallecimiento': 'el lugar de fallecimiento',
    'educado en': 'dónde fue educado',
    'ocupación': 'la ocupación',
    'país de nacionalidad': 'el país de nacionalidad',
    'movimiento': 'el movimiento artístico/cultural',
    'premio recibido': 'el premio recibido',
    'género': 'el género',
    'sello discográfico': 'el sello discográfico',
    'pareja': 'la pareja',
    'influenciado por': 'quién lo influenció',
    'forma parte de': 'de qué forma parte',
    'obra destacada': 'la obra destacada',
    'instrumento': 'el instrumento que toca',
    
    # Relaciones en inglés (principalmente de canon_basic)
    'author': 'el autor',
    'country of origin': 'el país de origen',
    'country of citizenship': 'el país de ciudadanía',
    'country': 'el país',
    'located in the administrative territorial entity': 'la ubicación administrativa',
    'instance of': 'un tipo de', # Duplicado intencional
    'headquarters location': 'la ubicación de la sede',
    'location': 'la ubicación',
    'occupation': 'la ocupación', # Duplicado intencional
    'position held': 'el puesto ocupado',
    'member of political party': 'el partido político',
    'place of birth': 'el lugar de nacimiento', # Duplicado intencional
    'date of birth': 'la fecha de nacimiento',
    'publication date': 'la fecha de publicación',
    'date of death': 'la fecha de muerte',
    'place of death': 'el lugar de muerte', # Duplicado intencional
    'founded by': 'el fundador',
    'child': 'el hijo/a',
    'member of': 'miembro de',
    'religion': 'la religión',
    'spouse': 'el/la cónyuge', # Duplicado intencional
    'field of work': 'el campo de trabajo',
    'cause of death': 'la causa de muerte',
    'currency': 'la moneda',
    'height': 'la altura',
    'notable work': 'la obra destacada', # Duplicado intencional
    'movement': 'el movimiento artístico/cultural', # Duplicado intencional
    'population': 'la población',
    'place of burial': 'el lugar de sepultura',
    'manner of death': 'la forma de muerte',
    'official language': 'el idioma oficial',
    'family': 'la familia',
    'director': 'el director',
    'screenwriter': 'el guionista',
    'native language': 'el idioma nativo',
    # Añade MÁS relaciones aquí si son relevantes
}

BLACKLIST_RELACIONES_RAW = {
    'Chile', '16 de marzo de 1935', 'chileno', 'doma',
    'lugar de nacimiento', 'instancia de', 'lugar de fallecimiento', 'educado en',
    'ocupación', 'país de nacionalidad', 'movimiento', 'premio recibido',
    'género', 'sello discográfico', 'pareja', 'influenciado por',
    'forma parte de', 'obra destacada', 'instrumento'
}
BLACKLIST_RELACIONES = {unidecode(r.lower()) for r in BLACKLIST_RELACIONES_RAW}

# --- Leer argumentos ---
if len(sys.argv) != 3:
    print("Error: Debes proporcionar el nombre del CSV de entrada y el JSON de salida.")
    print("Uso: python generador_qa_individual.py <archivo_entrada.csv> <archivo_salida.json>")
    exit()

input_csv_file = sys.argv[1]
output_json_file = sys.argv[2]

# --- Procesamiento ---
dataset_qa = []
filas_ignoradas_blacklist = 0
filas_ignoradas_formato = 0

if not os.path.exists(input_csv_file):
    print(f"Error: No se encuentra el archivo de entrada '{input_csv_file}'.")
else:
    print(f"Procesando archivo: {input_csv_file}")
    # Asumimos UTF-8 porque los scripts de merge guardan en UTF-8
    df = pd.read_csv(input_csv_file, index_col=False, encoding='utf-8') 
    
    # Verificar que existan las columnas estándar
    columnas_necesarias = ['entidad', 'relacion', 'valor']
    if not all(col in df.columns for col in columnas_necesarias):
         print(f"Error: El CSV debe contener las columnas {columnas_necesarias}.")
         print(f"Columnas encontradas: {df.columns.tolist()}")
         exit()
         
    # --- Generación de QA (SIN FILTRO IDIOMA) ---
    print("Generando pares Pregunta/Respuesta...")
    for index, row in df.iterrows():
        entidad = str(row['entidad'])
        relacion = str(row['relacion'])
        valor = str(row['valor']) # Ya debería ser el valor en español (o inglés si es USA)
        
        relacion_norm = unidecode(relacion.lower())

        # Ignorar si está en blacklist o es formato inválido
        if relacion_norm in BLACKLIST_RELACIONES:
            filas_ignoradas_blacklist += 1
            continue
        if '%' in relacion or len(relacion) < 2 or len(relacion) > 80:
            filas_ignoradas_formato += 1
            continue
            
        plantilla_relacion = MAPA_PREGUNTAS.get(relacion, relacion) 
        pregunta = f"¿Cuál es {plantilla_relacion} de {entidad}?"
        
        respuesta_correcta = valor
        
        dataset_qa.append({
            "pregunta": pregunta,
            "respuesta_correcta": respuesta_correcta
        })

    # Guardar el JSON
    with open(output_json_file, 'w', encoding='utf-8') as f:
        json.dump(dataset_qa, f, ensure_ascii=False, indent=4)

    print(f"\n¡Éxito! Se ha creado '{output_json_file}' con {len(dataset_qa)} pares QA.")
    print(f"(Se ignoraron {filas_ignoradas_blacklist} filas por blacklist, {filas_ignoradas_formato} por formato)")
    print("-------------------------------------------------")