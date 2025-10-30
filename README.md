# Challenge Generative AI

## Descripción
Tarea de evaluación de modelos de IA generativa en conocimientos de Latinoamérica. Se recolectaron tríos entidad-relación-valor desde fuentes culturales y se evaluó el conocimiento de varios datasets en el notebook Desafío.ipynb.

## Modelos evaluados
- TinyLlama
- Qwen/Qwen3-4B-Instruct-2507  
- Meta Llama-3.1-8B-Instruct

## Datasets
- gonza_qa.json
- usa_qa.json
- tomy_qa.json
- justo_qa.json

## Uso
1. Ejecutar `1_merge_csv.py` para fusionar datos
2. Ejecutar `2_generador_qa.py` para generar preguntas sobre un csv
3. Evaluar modelos en `Desafío.ipynb`

## Instalación
```bash
pip install vllm unidecode langdetect pandas tqdm transformers huggingface_hub
```