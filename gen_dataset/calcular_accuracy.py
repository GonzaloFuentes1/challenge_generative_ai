import json
import sys
import os

print("--- Calculando Accuracy desde archivo JSON ---")

# --- Leer argumento: nombre del archivo JSON ---
if len(sys.argv) != 2:
    print("Error: Debes proporcionar el nombre del archivo JSON de resultados.")
    print("Uso: python calcular_accuracy.py <archivo_resultados.json>")
    exit()

input_json_file = sys.argv[1]

# --- Procesar el JSON ---
if not os.path.exists(input_json_file):
    print(f"Error: No se encuentra el archivo '{input_json_file}'.")
else:
    try:
        with open(input_json_file, 'r', encoding='utf-8') as f:
            resultados = json.load(f)

        if not isinstance(resultados, list) or len(resultados) == 0:
            print("Error: El archivo JSON está vacío o no tiene el formato esperado (una lista de resultados).")
            exit()

        total_preguntas = 0
        correctas = 0

        # Contar las correctas
        for item in resultados:
            if isinstance(item, dict) and "evaluacion" in item:
                total_preguntas += 1
                if item["evaluacion"] == "CORRECTO":
                    correctas += 1
            else:
                print(f"Advertencia: Se encontró un item sin formato esperado: {item}")


        # Calcular y mostrar el puntaje
        if total_preguntas > 0:
            puntaje = (correctas / total_preguntas) * 100
            # Extraer nombre del modelo del nombre del archivo (aproximado)
            try:
                 nombre_modelo = input_json_file.split('real_')[-1].replace('.json', '')
            except:
                 nombre_modelo = "Modelo Desconocido"

            print(f"\n📊 Resultados para: {input_json_file}")
            print(f"   Modelo (aprox): {nombre_modelo}")
            print(f"   Total Evaluado: {total_preguntas} preguntas")
            print(f"   Respuestas Correctas: {correctas}")
            print(f"   Accuracy: {puntaje:.1f}%")
            print("-------------------------------------------------")
        else:
            print("No se encontraron evaluaciones válidas en el archivo.")

    except json.JSONDecodeError:
        print(f"Error: El archivo '{input_json_file}' no es un JSON válido.")
    except Exception as e:
        print(f"Error inesperado procesando el archivo: {e}")