# -*- coding: utf-8 -*-
"""
Convierte un KG en JSON (con nodes/edges) a un CSV de tripletas con:
- relacion (property_label en español)
- valor (label humano, preferencia ES y además todos los idiomas)
Requiere: pip install SPARQLWrapper pandas
Uso:
  python json_to_csv_wikidata_labels.py input.json output.csv
"""

import sys
import json
import time
from collections import defaultdict
from typing import Dict, List, Tuple, Iterable
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON as SP_JSON

ENDPOINT = "https://query.wikidata.org/sparql"

def sparql(query: str, throttle_s: float = 0.2) -> dict:
    s = SPARQLWrapper(ENDPOINT)
    s.setQuery(query)
    s.setReturnFormat(SP_JSON)
    res = s.query().convert()
    if throttle_s > 0:
        time.sleep(throttle_s)
    return res

def chunked(iterable, n):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def fetch_all_lang_labels_for_Q(qids, batch_size=200):
    out = defaultdict(dict)
    for group in chunked(qids, batch_size):
        values = " ".join(f"wd:{q}" for q in group)
        query = f"""
        SELECT ?item ?label (LANG(?label) AS ?lang) WHERE {{
          VALUES ?item {{ {values} }}
          ?item rdfs:label ?label .
          FILTER(LANG(?label) != "")
        }}
        """
        data = sparql(query)
        for b in data["results"]["bindings"]:
            qid = b["item"]["value"].split("/")[-1]
            lab = b["label"]["value"]
            lang = b["lang"]["value"]
            out[qid][lang] = lab
    return dict(out)

def fetch_spanish_labels_for_P(pids, batch_size=300):
    out_es = {}
    missing = set(pids)

    for group in chunked(list(missing), batch_size):
        values = " ".join(f"wd:{p}" for p in group)
        q = f"""
        SELECT ?p ?label WHERE {{
          VALUES ?p {{ {values} }}
          ?p rdfs:label ?label .
          FILTER(LANG(?label) = "es")
        }}
        """
        data = sparql(q)
        for b in data["results"]["bindings"]:
            pid = b["p"]["value"].split("/")[-1]
            out_es[pid] = b["label"]["value"]
            if pid in missing:
                missing.remove(pid)

    if missing:
        for group in chunked(list(missing), batch_size):
            values = " ".join(f"wd:{p}" for p in group)
            q = f"""
            SELECT ?p ?label WHERE {{
              VALUES ?p {{ {values} }}
              ?p rdfs:label ?label .
              FILTER(LANG(?label) = "en")
            }}
            """
            data = sparql(q)
            for b in data["results"]["bindings"]:
                pid = b["p"]["value"].split("/")[-1]
                out_es[pid] = b["label"]["value"]
                if pid in missing:
                    missing.remove(pid)

    for pid in missing:
        out_es[pid] = pid

    return out_es

def json_to_triplets_csv(input_json, output_csv):
    with open(input_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    id2label = {n["id"]: n.get("label", n["id"]) for n in nodes}
    qids_targets, qids_subjects, pids_props = set(), set(), set()

    for e in edges:
        src, tgt, pid = e.get("source", ""), e.get("target", ""), e.get("property_pid", "")
        if src.startswith("Q"):
            qids_subjects.add(src)
        if tgt.startswith("Q"):
            qids_targets.add(tgt)
        if pid.startswith("P"):
            pids_props.add(pid)

    labels_Q_targets = fetch_all_lang_labels_for_Q(sorted(qids_targets)) if qids_targets else {}
    labels_Q_subjects = fetch_all_lang_labels_for_Q(sorted(qids_subjects)) if qids_subjects else {}
    labels_P_es = fetch_spanish_labels_for_P(sorted(pids_props)) if pids_props else {}

    def prefer_es(labels):
        if not labels:
            return ""
        return labels.get("es") or labels.get("en") or next(iter(labels.values()))

    rows = []
    for e in edges:
        src, tgt, pid = e.get("source", ""), e.get("target", ""), e.get("property_pid", "")
        relacion = labels_P_es.get(pid, e.get("property_label", pid))
        sujeto_qid = src
        sujeto_es = prefer_es(labels_Q_subjects.get(sujeto_qid, {})) or id2label.get(src, sujeto_qid)
        if tgt.startswith("Q"):
            valor_qid = tgt
            valor_labels = labels_Q_targets.get(valor_qid, {})
            valor_es = prefer_es(valor_labels)
            valor = valor_es or id2label.get(tgt, valor_qid)
        elif tgt.startswith("L:"):
            valor_qid = ""
            valor = tgt[2:]
            valor_es = valor
        else:
            valor_qid = tgt
            valor = id2label.get(tgt, tgt)
            valor_es = valor
        rows.append({
            "sujeto_qid": sujeto_qid,
            "entidad": sujeto_es,
            "relacion": relacion,
            "valor": valor,
            "valor_es": valor_es,
            "valor_qid": valor_qid
        })

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"✅ CSV generado: {output_csv} ({len(df)} filas)")
    return df
