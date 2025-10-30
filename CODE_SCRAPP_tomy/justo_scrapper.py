"""
scripts/wikidata_to_triples.py

Versión robusta del extractor de tripletas <entidad, relación, valor> desde Wikidata
centrado en cocinas latinoamericanas.

Mejoras:
- reintentos automáticos en errores de red y 5xx/429,
- backoff exponencial ante JSONDecodeError o contenido no-JSON,
- batches más pequeños por defecto (evita respuestas muy grandes),
- guardado intermedio de resultados (CSV) cada N batches,
- compatibilidad con Jupyter/Colab (parse_known_args).
"""
from typing import List, Dict, Tuple, Any, Optional
import argparse
import time
import requests
import pandas as pd
from tqdm import tqdm
import json
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
USER_AGENT = "jsolis2016-wikidata-cuisine-extractor/1.1 (mailto:youremail@example.com)"

DEFAULT_COUNTRY_QIDS = [
    "Q419",  # Perú
    "Q414",  # Argentina
    "Q298",  # Chile
    "Q96",   # México
    "Q739",  # Colombia
    "Q736",  # Ecuador
    "Q750",  # Bolivia
    "Q77",   # Uruguay
    "Q753",  # Paraguay
    "Q717",  # Venezuela
    "Q800",  # Costa Rica
    "Q774",  # Guatemala
    "Q783",  # Honduras
    "Q794",  # El Salvador
    "Q811",  # Nicaragua
    "Q804",  # Panamá
    "Q241",  # Cuba
    "Q786",  # República Dominicana
]

def create_session(retries: int = 5, backoff_factor: float = 0.3, status_forcelist: Optional[List[int]] = None) -> requests.Session:
    session = requests.Session()
    headers = {"User-Agent": USER_AGENT}
    session.headers.update(headers)
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def safe_get_json(session: requests.Session, url: str, params: dict, expected_content_substr: Optional[str] = None,
                  timeout: int = 60, max_attempts: int = 5, initial_delay: float = 0.5) -> dict:
    """
    Realiza GET y devuelve JSON, con reintentos en errores HTTP y JSONDecodeError.
    Si la respuesta no tiene Content-Type JSON, reintenta (para capturar HTML de error).
    """
    attempt = 0
    delay = initial_delay
    last_text_sample = None
    while attempt < max_attempts:
        attempt += 1
        try:
            resp = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            print(f"[warning] RequestException attempt {attempt}/{max_attempts}: {e}. Reintentando en {delay}s...")
            time.sleep(delay)
            delay *= 2
            continue

        # status code retry handled by session Retry, but double-check:
        if resp.status_code >= 500 or resp.status_code == 429:
            print(f"[warning] HTTP {resp.status_code} attempt {attempt}/{max_attempts}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2
            continue

        content_type = resp.headers.get("Content-Type", "").lower()
        text = resp.text

        # Save a small sample for debugging if needed
        last_text_sample = text[:2000]

        # If expected_content_substr provided, require it in content-type
        if expected_content_substr and expected_content_substr not in content_type:
            # slice BEFORE conversion to !r to avoid f-string syntax errors
            snippet = last_text_sample[:300] if last_text_sample else ""
            print(f"[warning] Unexpected Content-Type: {content_type} (expecting {expected_content_substr}). "
                  f"Response snippet: {snippet!r}")
            # retry after sleep
            time.sleep(delay)
            delay *= 2
            continue

        # Try parsing JSON
        try:
            return resp.json()
        except json.JSONDecodeError as jde:
            snippet = last_text_sample if last_text_sample is not None else ""
            print(f"[warning] JSONDecodeError attempt {attempt}/{max_attempts}: {jde}. Response snippet: {snippet!r}")
            # retry with backoff
            time.sleep(delay)
            delay *= 2
            continue

    # If we exit loop, raise informative error
    raise RuntimeError(f"Failed to get valid JSON from {url} after {max_attempts} attempts. Last response snippet:\n{last_text_sample}")

def run_sparql(session: requests.Session, query: str, sleep: float = 0.1, max_attempts: int = 6) -> Dict[str, Any]:
    headers = {"Accept": "application/sparql-results+json"}
    params = {"query": query, "format": "json"}
    data = safe_get_json(session, WIKIDATA_SPARQL_URL, params=params, expected_content_substr="application/sparql-results+json",
                         timeout=60, max_attempts=max_attempts, initial_delay=0.5)
    time.sleep(sleep)
    return data

def get_items_for_countries(session: requests.Session, country_qids: List[str], limit: int = None) -> List[Dict[str, str]]:
    values = " ".join(f"wd:{c}" for c in country_qids)
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
    SELECT DISTINCT ?item ?itemLabel ?country ?countryLabel WHERE {{
      VALUES ?country {{ {values} }}
      ?item (wdt:P495|wdt:P17) ?country .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "es,en". }}
    }}
    {limit_clause}
    """
    data = run_sparql(session, query)
    results = []
    for b in data.get("results", {}).get("bindings", []):
        item_uri = b["item"]["value"]
        qid = item_uri.rsplit("/", 1)[-1]
        item_label = b.get("itemLabel", {}).get("value", qid)
        country_uri = b["country"]["value"]
        country_qid = country_uri.rsplit("/", 1)[-1]
        country_label = b.get("countryLabel", {}).get("value", country_qid)
        results.append({"qid": qid, "label": item_label, "country_qid": country_qid, "country_label": country_label})
    return results

def chunk_list(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def wbgetentities(session: requests.Session, ids: List[str], props: str = "labels|claims", languages: str = "es|en",
                  max_attempts: int = 6) -> Dict[str, Any]:
    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": "|".join(ids),
        "props": props,
        "languages": languages,
        "languagefallback": "1",
    }
    data = safe_get_json(session, WIKIDATA_API_URL, params=params, expected_content_substr="application/json",
                         timeout=60, max_attempts=max_attempts, initial_delay=0.5)
    time.sleep(0.1)
    return data

def extract_triples_from_entities(entities_data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    triples = []
    property_ids = set()
    value_qids = set()
    entities = entities_data.get("entities", {})
    for eid, ent in entities.items():
        subject_qid = eid
        subject_label = subject_qid
        if ent.get("labels"):
            if "es" in ent["labels"]:
                subject_label = ent["labels"]["es"]["value"]
            elif "en" in ent["labels"]:
                subject_label = ent["labels"]["en"]["value"]
        claims = ent.get("claims", {}) or {}
        for pid, statements in claims.items():
            property_ids.add(pid)
            for st in statements:
                mainsnak = st.get("mainsnak", {})
                datavalue = mainsnak.get("datavalue")
                if not datavalue:
                    continue
                dv = datavalue
                typ = dv.get("type")
                obj_repr = None
                obj_type = None
                if typ == "wikibase-entityid":
                    qid = dv["value"].get("id")
                    obj_repr = qid
                    obj_type = "item"
                    value_qids.add(qid)
                elif typ == "string":
                    obj_repr = dv["value"]
                    obj_type = "string"
                elif typ == "monolingualtext":
                    obj_repr = dv["value"].get("text")
                    obj_type = "monolingualtext"
                elif typ == "time":
                    obj_repr = dv["value"].get("time")
                    obj_type = "time"
                elif typ == "quantity":
                    obj_repr = str(dv["value"].get("amount"))
                    obj_type = "quantity"
                elif typ == "globecoordinate":
                    lat = dv["value"].get("latitude")
                    lon = dv["value"].get("longitude")
                    obj_repr = f"{lat},{lon}"
                    obj_type = "globecoordinate"
                elif typ == "url":
                    obj_repr = dv["value"]
                    obj_type = "url"
                else:
                    obj_repr = str(dv.get("value"))
                    obj_type = typ or "unknown"

                triples.append({
                    "subject_qid": subject_qid,
                    "subject_label": subject_label,
                    "relation_pid": pid,
                    "object": obj_repr,
                    "object_type": obj_type,
                    "country_qid": "",
                    "country_label": "",
                    "source": f"https://www.wikidata.org/wiki/{subject_qid}"
                })

    return triples, list(property_ids), list(value_qids)

def fetch_labels_for_ids(session: requests.Session, ids: List[str], batch_size: int = 25) -> Dict[str, str]:
    labels = {}
    ids_unique = list(dict.fromkeys(ids))
    batches = chunk_list(ids_unique, batch_size)
    for batch in tqdm(batches, desc="Resolviendo labels (batches)"):
        try:
            data = wbgetentities(session, batch, props="labels", languages="es|en")
        except Exception as e:
            print(f"[error] wbgetentities para labels falló en batch {batch[:5]}...: {e}. Se omiten estas ids.")
            for x in batch:
                labels[x] = x
            continue
        ents = data.get("entities", {})
        for eid, ent in ents.items():
            lab = None
            if ent.get("labels"):
                if "es" in ent["labels"]:
                    lab = ent["labels"]["es"]["value"]
                elif "en" in ent["labels"]:
                    lab = ent["labels"]["en"]["value"]
            labels[eid] = lab if lab else eid
    return labels

def save_partial_csv(rows: List[dict], path: str):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)

def main(argv: List[str] = None):
    parser = argparse.ArgumentParser(description="Extrae tripletas de Wikidata para cocinas latinoamericanas (robusto)")
    parser.add_argument("--countries", type=str, default=",".join(DEFAULT_COUNTRY_QIDS),
                        help="Lista de QIDs de países separados por comas.")
    parser.add_argument("--limit", type=int, default=None, help="Límite de items devueltos por SPARQL (útil para pruebas).")
    parser.add_argument("--output_csv", type=str, default="triples_simple.csv", help="Archivo CSV de salida.")
    parser.add_argument("--detailed_csv", type=str, default="triples_detailed.csv", help="CSV detallado (opcional).")
    parser.add_argument("--sleep", type=float, default=0.1, help="Delay entre requests para respetar rate limits.")
    parser.add_argument("--batch_size", type=int, default=25, help="Tamaño de batch para wbgetentities (reducir si hay problemas).")
    parser.add_argument("--retries", type=int, default=5, help="Número máximo de reintentos HTTP/JSON por request.")
    parser.add_argument("--backoff", type=float, default=0.3, help="Factor de backoff para reintentos HTTP.")
    parser.add_argument("--save_every", type=int, default=5, help="Guardar CSV parcial cada N batches (0 = no guardar).")

    if argv is None:
        args, unknown = parser.parse_known_args()
        if unknown:
            print(f"Ignorando argumentos desconocidos (probablemente del kernel): {unknown}")
    else:
        args = parser.parse_args(argv)

    country_qids = [c.strip() for c in args.countries.split(",") if c.strip()]
    print(f"Usando países: {country_qids}")
    session = create_session(retries=args.retries, backoff_factor=args.backoff)

    # 1) Obtener items mediante SPARQL
    try:
        items = get_items_for_countries(session, country_qids, limit=args.limit)
    except Exception as e:
        print(f"[fatal] Error al ejecutar SPARQL: {e}")
        raise

    print(f"Encontrados {len(items)} items (candidatos).")
    if not items:
        print("No se encontraron items. Revisa los QIDs de países o incrementa el límite.")
        return

    # 2) Batch: obtener claims y labels básicos para cada item
    qids = [it["qid"] for it in items]
    all_triples: List[dict] = []
    all_property_ids = set()
    all_value_qids = set()

    batches = chunk_list(qids, args.batch_size)
    for i, batch in enumerate(tqdm(batches, desc="Descargando entidades (wbgetentities)")):
        try:
            ent_data = wbgetentities(session, batch, props="labels|claims", languages="es|en", max_attempts=args.retries)
        except Exception as e:
            print(f"[error] wbgetentities falló para batch {i+1}/{len(batches)}: {e}. Se salta este batch.")
            continue

        try:
            triples, pids, vqids = extract_triples_from_entities(ent_data)
        except Exception as e:
            print(f"[error] Extracción de tripletas falló para batch {i+1}: {e}. Se salta este batch.")
            continue

        all_triples.extend(triples)
        all_property_ids.update(pids)
        all_value_qids.update(vqids)

        # Guardado intermedio
        if args.save_every and args.save_every > 0 and ((i + 1) % args.save_every == 0):
            print(f"Guardando progreso parcial tras {i+1} batches...")
            partial_rows = []
            for t in all_triples:
                partial_rows.append({
                    "subject_qid": t["subject_qid"], "subject_label": t.get("subject_label", ""),
                    "relation_pid": t["relation_pid"], "relation_label": "",
                    "object": t["object"], "object_label": "", "object_qid": (t["object"] if t["object_type"]=="item" else ""),
                    "object_type": t["object_type"], "country_qid": t.get("country_qid",""),
                    "country_label": t.get("country_label",""), "source": t.get("source","")
                })
            save_partial_csv(partial_rows, args.detailed_csv + ".partial")
            print(f"CSV parcial guardado: {args.detailed_csv}.partial")

    print(f"Tripletas preliminares: {len(all_triples)}")
    print(f"Propiedades detectadas: {len(all_property_ids)}")
    print(f"Valores (QIDs) detectados: {len(all_value_qids)}")

    # 3) Añadir country info desde la lista inicial (map)
    qid2country = {it["qid"]: (it["country_qid"], it["country_label"]) for it in items}
    for t in all_triples:
        subj = t["subject_qid"]
        if subj in qid2country:
            t["country_qid"], t["country_label"] = qid2country[subj]

    # 4) Obtener labels para propiedades y valores
    ids_to_label = list(all_property_ids) + list(all_value_qids)
    print("Resolviendo labels para propiedades y valores (batches)...")
    labels_map = fetch_labels_for_ids(session, ids_to_label, batch_size=args.batch_size)

    # 5) Construir filas finales (subject_label, relation_label, object_value)
    rows = []
    for t in all_triples:
        subj_label = t.get("subject_label") or labels_map.get(t["subject_qid"], t["subject_qid"])
        rel_pid = t["relation_pid"]
        rel_label = labels_map.get(rel_pid, rel_pid)
        obj = t["object"]
        obj_type = t["object_type"]
        if obj_type == "item" and isinstance(obj, str) and obj in labels_map:
            obj_label = labels_map.get(obj, obj)
            obj_out = obj_label
            obj_qid = obj
        else:
            obj_label = obj
            obj_out = obj
            obj_qid = ""
        rows.append({
            "subject_qid": t["subject_qid"],
            "subject_label": subj_label,
            "relation_pid": rel_pid,
            "relation_label": rel_label,
            "object": obj_out,
            "object_label": obj_label,
            "object_qid": obj_qid,
            "object_type": obj_type,
            "country_qid": t.get("country_qid", ""),
            "country_label": t.get("country_label", ""),
            "source": t.get("source", "")
        })

    df = pd.DataFrame(rows)
    df_simple = df[["subject_label", "relation_label", "object", "country_label", "source"]].copy()
    df_simple.to_csv(args.output_csv, index=False)
    print(f"CSV simple escrito: {args.output_csv}")
    df.to_csv(args.detailed_csv, index=False)
    print(f"CSV detallado escrito: {args.detailed_csv}")
    print("Proceso completo.")

if __name__ == "__main__":
    main()
