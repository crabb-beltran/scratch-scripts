#!/usr/bin/env python3
"""
Generador DL 2.0 (v5): ingestas (artifacts) y modelos std (data-models) por sistema origen. UN SOLO COMANDO:

    python gen_ingestas.py                 # todas las tablas de in/new_migration_tables_to_dl2.csv
    python gen_ingestas.py <sistema>       # solo un sistema origen (código de entrada; también acepta el nombre DL 2.0)
    python gen_ingestas.py --sin-prueba    # no genera la prueba de conexión (permanente en dev y prd; por defecto una por sistema)
    El script SOLO LEE los repos: todo lo que genera queda en out/. Para llevar a la rama de trabajo las tablas LISTO
    (y la prueba de conexión, si su tabla base está LISTO):
    bash out/<sistema>/out/copiar_a_repos.sh   (lo ejecuta el usuario cuando decide; valida que no sea una rama protegida)

scripts/
  in/                          catálogos y lista de trabajo (se editan); cliente.yaml = valores del cliente (local, no se versiona)
    new_migration_tables_to_dl2.csv   tabla_origen;sistema_origen;dominio;subdominio;extractor  (tablas a generar;
                                      extractor = cipher | jdbc, regla TL: corp → cipher, resto → jdbc)
    policy_tags*.csv · dominios_operacionales.csv · sinonimos.csv (opcional) · config.yaml (ramas DL 1.0)
  out/<sistema>/
    in/   <sis>_tablas.csv (una fila por tabla) · <sis>_columnas.csv (una fila por columna: único archivo con Gobierno)
          · <sis>_ddl.csv (opcional) · .estado.json (estado interno del script, oculto: no se edita)
    out/  ESTADO.md · trazabilidad_dl1.csv (SENSIBLE: valores de conexión DL 1.0)
          artifacts/co/data_source/<sis>/<tabla>/*  · artifacts/co/variables.yaml   (rutas del repo destino)
          artifacts/co/data_source/<sis>/all_tables/<sis>_all_tables.yaml  (prueba de conexión PERMANENTE en dev y prd: sale con las tablas)
          retirar_prueba_conexion.sh   (solo si se decide retirar la prueba; ver ESTADO.md «Prueba de conexión»)
          models/<dbt_proyecto>/models/std_<dominio>/<tabla>/* · models/.../std_<dominio>/__sources.yml

Por sistema: scan (columnas DL 1.0 + DDL, tipos, policy tags, estado_gobierno) → conexión DL 1.0 (rama productiva)
→ render. Idempotente: conserva lo editado a mano y lo resuelto por gobierno. Una tabla queda LISTO cuando no le
queda nada POR_CONFIRMAR, ninguna columna incluida pendiente de Gobierno y sus llaves existen en los ambientes de desarrollo y producción del variables.yaml de la rama de trabajo.
Repos: por defecto ../../<repos_raiz> de in/cliente.yaml (o --repos / DL2_REPOS). Nunca se escriben.
"""
import argparse, contextlib, copy, csv, hashlib, io, json, os, re, shutil, subprocess, sys, unicodedata
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
import yaml

VERSION, GEN, MARCA = "6.8.3", "v1", "dl2-gen"   # v6.8 (26-nov-2026)
HERE = Path(__file__).resolve().parent
IN, OUT = HERE / "in", HERE / "out"          # in: catálogos y lista · out/<sistema>/{in,out}
LISTA = IN / "new_migration_tables_to_dl2.csv"
CLIENTE_YAML = IN / "cliente.yaml"

# Valores propios del cliente: se leen de in/cliente.yaml (local, git-ignored). Sin ese archivo se usan estos valores
# ficticios (el módulo importa, pero el generador exige el archivo real al ejecutarse; ver cliente.ejemplo.yaml).
CLIENTE_DEFECTO = {
    "repos_raiz": "acme-repos", "dir_artifacts": "acme-artifacts", "dir_models": "acme-models",
    "dir_legacy": "acme-legacy", "rama_prd_legacy": "legacy-prod", "dbt_proyecto": "acme_dbt",
    "bu": "xx", "ind": "yy", "comp_id": "acme", "team_to_report": "acmeops", "team_to_report_dev": "acmeops",
    "equipo": "acme_ingestas", "email_key": "EMAIL_ALERTAS",
    "env_dev": "env-dev", "env_prd": "env-prd", "ramas_protegidas": ["env-dev", "env-prd", "env-pre-prd"],
    "ambiente_sufijo": {"env-prd": "prd", "env-dev": "qa"}, "dbt_env": {"env-dev": "dev", "env-prd": "prd"},
    "recursos": {"LND_BUCKET": "acme-lnd-{s}-{amb}", "RAW_BUCKET": "acme-raw-{s}-{amb}", "RAW_DATASET": "raw_{sis}"},
    "ods_owner_regex": "^(USR_A|USR_B)", "catalogo_sistemas": {"SISTEMAX": "sistemax"},
    "pt_others": "PT_IU_OTHERS", "pt_cuarentena": "POLICY_TAG_CUARENTENA",
    "topicos": {"jdbc": ["START_TOPIC", None], "cipher": ["START_TOPIC_CIPHER", "START_TOPIC_CIPHER_PROJECT"]},
    "palabras_vacias_extra": [],   # opcional: palabras adicionales que el clasificador ignora
    "sistema_nombre": {},          # opcional: {codigo_sistema: nombre DL 2.0}; todo lo emitido (carpetas, llaves, recursos, modelos) usa el nombre
    "nombres_anteriores": {},      # opcional: {codigo_sistema: [nombres de recurso usados antes]} para migrar valores de buckets/dataset
    "aprobacion_total": False,     # True: solo `aprobado` pasa a LISTO (con False, `sugerido` también pasa)
    "umbral_borrado": 0.2,         # CDC: fracción de borrados sobre lo vigente a partir de la cual el modelo aborta
    "prefijo_dag": "xx_",          # prefijo del id del DAG de extracción en Composer: <prefijo><sistema>_<tabla>-extraction
    # opcional: prueba de conexión permanente. minutos_antes = minutos antes de la primera extracción del sistema;
    # scheduling no vacío fuerza ese cron (en lugar del calculado)
    "prueba_conexion": {"minutos_antes": 30, "scheduling": ""},
}

def cargar_cliente():
    cfg = dict(CLIENTE_DEFECTO)
    if CLIENTE_YAML.exists():
        datos = yaml.safe_load(CLIENTE_YAML.read_text(encoding="utf-8")) or {}
        cfg.update({k: v for k, v in datos.items() if k in CLIENTE_DEFECTO})
        pc = datos.get("prueba_conexion")                                        # v6.7: se fusiona con el valor por defecto
        cfg["prueba_conexion"] = {**CLIENTE_DEFECTO["prueba_conexion"], **(pc if isinstance(pc, dict) else {})}
        if datos.get("recurso_nombre") and not datos.get("sistema_nombre"):      # alias heredado (v6.2-v6.4)
            cfg["sistema_nombre"] = datos["recurso_nombre"]
            print("  AVISO: `recurso_nombre` de in/cliente.yaml es un alias heredado: ahora define el nombre DL 2.0 del sistema "
                  "(carpetas, llaves, recursos y modelos); renombrarlo a `sistema_nombre`")
    return cfg

CLI = cargar_cliente()
PEND, OTHERS, CUARENTENA = "POR_CONFIRMAR", CLI["pt_others"], CLI["pt_cuarentena"]
ENV_DEV, ENV_PRD = CLI["env_dev"], CLI["env_prd"]
DBT = CLI["dbt_proyecto"]
ODS_OWNER = re.compile(CLI["ods_owner_regex"], re.I)
STG = re.compile(r"^stg_(orc|sql|db2|flt)_(.+)$", re.I)
TECH = {"orc": "oracle", "sql": "sqlserver", "db2": "db2", "flt": "archivo"}
MARK = re.compile(r"\{([A-Z][A-Z0-9_\-]*)\}|<([A-Z][A-Z0-9_]*)>")
KEYREF = re.compile(r"\{([A-Z][A-Z0-9_\-]*)\}")
PH = re.compile(r"<[a-z][^<>{}\n]*>")
PII_TAG = re.compile(r"^(PT_(NC|SC|HC)_|POR_CONFIRMAR$)")   # sin definir se trata como sensible
VARIANTS = ("JDBC", "CIFRADO")
ENV_USER, ENV_PASS = "extractor.env#source.datasource.username", "extractor.env#source.datasource.password"
CAMPOS_LLAVE = {"conexion": ("source.address", "extractor.host"), "owner_db": ("@owner",),
                "usuario": (ENV_USER,), "password": (ENV_PASS,)}
# Tópico de inicio por variante: (llave del tópico, llave del proyecto del tópico o None = PROJECT_ID)
TOPICO = {False: tuple(CLI["topicos"]["jdbc"]), True: tuple(CLI["topicos"]["cipher"])}
CONN = {"source.address", "extractor.host", "extractor.port", "extractor.sid", "@owner", ENV_USER, ENV_PASS}

# Columnas de <sis>_tablas.csv en su orden: (nombre, quién la llena, valor por defecto). `tabla` = <sistema>_<tabla> (llave con
# columnas.csv); `estado` (LISTO / BORRADOR / SIN FUENTE) y `sugerencias` (solo lectura) las escribe el script en cada corrida.
TABLAS = [
    ("tabla_origen", "excel", ""), ("sistema_origen", "excel", ""), ("dominio", "excel", ""), ("subdominio", "excel", ""),
    ("tabla", "scan", ""), ("estado", "scan", ""), ("ticket", "manual", PEND),
    ("extractor", "scan", "jdbc"), ("conexion", "manual", PEND), ("owner_db", "manual", PEND),
    ("usuario", "manual", PEND), ("password", "manual", PEND), ("tabla_fuente", "scan", ""),
    ("estrategia", "manual", PEND), ("llave_unica", "manual", PEND), ("columna_control", "manual", ""),
    ("ventana_dias", "manual", "3"), ("scheduling", "scan", "35 06 * * *"),
    ("raw_table", "scan", ""), ("dbt_model", "scan", ""),
    ("periodicidad", "manual", "daily"), ("available_time", "manual", "0900"), ("permanence", "manual", "3y"),
    ("ontology", "manual", PEND), ("data_owner", "manual", PEND), ("data_steward", "manual", PEND),
    ("data_custodian", "manual", PEND), ("owner", "manual", PEND), ("sugerencias", "scan", ""),
]
# Estado interno por tabla: no va en el CSV, vive en out/<sis>/in/.estado.json (se une a la fila al leer, se separa al escribir).
# active_since se fija en la primera generación y se conserva.
ESTADO_CAMPOS = ("caso", "sistema", "tecnologia", "extractor_fuente", "llaves_sugeridas", "valores_sugeridos", "dl1_detalle",
                 "cobertura_ddl", "estrategia_sugerida", "evidencia_dl2", "conexion_dl2_actual", "dl2_lee_ods", "dl2_gen",
                 "evidencia_v1", "origen_v1_fuera_ods", "active_since")
FILA = TABLAS + [(k, "scan", "") for k in ESTADO_CAMPOS]      # fila en memoria: columnas del CSV + estado interno
# <sis>_columnas.csv: único archivo con Gobierno. Script: identidad, tag_sugerido, motivo. Equipo/Gobierno: el resto.
COLUMNAS = ["tabla", "orden", "columna", "origen", "incluir", "expresion", "tipo_std", "descripcion", "policy_tag",
            "tag_sugerido", "estado_gobierno", "motivo", "responsable", "ticket", "fecha_resolucion", "observaciones"]
NUEVA = "nueva_ddl"          # columna de la fuente que DL 1.0 no extraía: se trae marcada y se escala
CATALOGO = dict(CLI["catalogo_sistemas"])   # sistema_origen del Excel -> código de sistema
# Dominio y subdominio operacionales ya están definidos (6-nov): se usan tal cual vienen, en inglés o en español,
# sin traducir. Solo se valida que sirvan como label y nombre de dataset de BigQuery.
LABEL = re.compile(r"[a-z0-9_-]{1,63}")   # con catalogos/dominios_operacionales.csv se valida contra la taxonomía
FIJOS = {"csv": {"separator": "|", "quotechar": '"', "escapechar": "\\", "quote_mode": "ALL"},
         "batch_size": 1000, "encoding": "ISO_8859_1", "resources": {"cpu": "400m", "memory": "512Mi"},
         "retention": "3y", "team_to_report_dev": CLI["team_to_report_dev"]}
# Recursos por sistema con nombre por convención DL 2.0 (se sugieren con valor; su creación se solicita en Terraform).
AMBIENTE = dict(CLI["ambiente_sufijo"])               # sufijo de recursos por bloque de variables.yaml
RECURSOS = dict(CLI["recursos"])
# Nombre DL 2.0 del sistema (decisión TL 20-nov): TODO lo emitido a out/<sis>/out/{artifacts,models} usa este nombre (carpetas,
# archivos, source.system, labels, llaves de variables.yaml, buckets, dataset raw, modelos dbt, source raw_<nombre>). La
# ENTRADA sigue por código: argumento CLI, out/<sis>/ e in/, DL 1.0, DDL y la lista de tablas.
SISTEMA_NOMBRE = dict(CLI["sistema_nombre"] or {})
# Nombres de recurso usados antes de v6.5 (solo para migrar VALORES de buckets/dataset con la convención vieja).
NOMBRES_ANTERIORES = {k: tuple(v) for k, v in (CLI["nombres_anteriores"] or {}).items()}
NOTA_RENOMBRE = "# renombrado (TL 20-nov): confirmar que el recurso existe"
# CDC (bitácora de cambios): si los borrados de una corrida superan esta fracción de las filas vigentes, el modelo aborta.
UMBRAL_BORRADO = float(CLI["umbral_borrado"])
ESTRATEGIAS = {"snapshot": "snapshot", "completa": "snapshot", "full": "snapshot", "incremental": "incremental",
               "cdc_hash": "cdc_hash", "cdc": "cdc_hash", "hash": "cdc_hash"}
NUEVAS_CARGA = {"estrategia", "estrategia_sugerida", "columna_control", "ventana_dias"}   # validación propia (no genérica)
MODIFICACION = re.compile(r"(FEC|FCH|FECHA|DATE|DT).*(MOD|ACT|UPD|ULT|CAMB|AUD)|(MOD|ACT|UPD|ULT|CAMB|AUD).*"
                          r"(FEC|FCH|FECHA|DATE|DT)|LAST_UPD|UPDATED_AT", re.I)
FECHA_INFO = "$FECHA_INFORMACION"
PREFIJO_DAG = CLI["prefijo_dag"]   # id del DAG de extracción: <prefijo><sis>_<tabla>-extraction
# Prueba de conexión permanente (v6.7): su schedule se calcula PRUEBA_MINUTOS_ANTES minutos antes de la primera extracción del
# sistema; con PRUEBA_SCHEDULING no vacío se fuerza ese cron (in/cliente.yaml: prueba_conexion.minutos_antes / .scheduling).
PRUEBA_MINUTOS_ANTES = int(CLI["prueba_conexion"]["minutos_antes"])
PRUEBA_SCHEDULING = str(CLI["prueba_conexion"].get("scheduling") or "")
CONTROL = re.compile(r"\b(SYSDATE|SYSTIMESTAMP|CURRENT_DATE|CURRENT_TIMESTAMP|GETDATE)\b", re.I)  # columna de control
DINAMICO = re.compile(r"var\(|target\.|env_var\(")
# Contacto técnico: el correo de alertas va por llave de variables.yaml (dato sensible, distinto por ambiente);
# el label tech_focal_mail lleva el nombre del equipo (BigQuery no admite correos en labels).
EMAIL_KEY, EQUIPO = CLI["email_key"], CLI["equipo"]
FALLBACK = {"extractor.job_namespace": "{K8S-NAMESPACE}", "extractor.env#source.datasource.secretProject": "{SM_PROJECT_ID}"}

# Gobierno de columnas: con False, `sugerido` (coincidencia confiable del catálogo) no bloquea y solo pendiente / en revisión
# bloquean; con True (CLI aprobacion_total) solo `aprobado` pasa a LISTO.
APROBACION_TOTAL = bool(CLI["aprobacion_total"])
TEAM_REPORT = CLI["team_to_report"]        # label/campo team_to_report: configuración, ya no va en el CSV de tablas


# ----------------------------------------------------------------------------- nombre DL 2.0 del sistema (v6.5)
RAMA_PRD_DL2, RAMA_DEV_DL2, PROTEGIDAS, DBT_DIR = ENV_PRD, ENV_DEV, tuple(CLI["ramas_protegidas"]), DBT

def _cambian():
    """{código: nombre} solo de los sistemas cuyo nombre DL 2.0 difiere del código."""
    return {c: n for c, n in SISTEMA_NOMBRE.items() if c and n and c != n}

def nombre_sis(sis):
    """Código de sistema (entrada) -> nombre DL 2.0 (todo lo que se emite a out/<sis>/out/{artifacts,models})."""
    return SISTEMA_NOMBRE.get(sis, sis)

def codigo_sis(x):
    """Nombre DL 2.0 (admisiones) o código (admiprod) -> código de entrada. Otro valor se devuelve igual."""
    for c, n in _cambian().items():
        if str(x).lower() == n.lower():
            return c
    return x

def _caso(orig, nuevo):
    return nuevo.upper() if orig.isupper() else (nuevo[:1].upper() + nuevo[1:] if orig[:1].isupper() else nuevo)

def renombrar(txt):
    """Cambia el código de sistema anterior por su nombre DL 2.0 en un texto (palabra completa: no toca letras ni dígitos
    pegados; `_`, `-`, `.` y `/` sí delimitan). Conserva mayúsculas: ADMIPROD_X -> ADMISIONES_X. Idempotente."""
    if not isinstance(txt, str):
        return txt
    for c, n in _cambian().items():
        txt = re.sub(rf"(?<![A-Za-z0-9]){re.escape(c)}(?![A-Za-z0-9])", lambda m, n=n: _caso(m.group(0), n), txt, flags=re.I)
    return txt

def nombres_viejos(sis):
    return (sis,) + tuple(NOMBRES_ANTERIORES.get(sis, ()))

def tipo_recurso(k, sis):
    """Sufijo de RECURSOS (LND_BUCKET, RAW_BUCKET, RAW_DATASET) si la llave k es un recurso del sistema sis; si no, ''."""
    S, K = sis.upper(), str(k).upper()
    for suf in RECURSOS:
        if K == f"{S}_{suf}":
            return suf
    return "RAW_DATASET" if K == f"RAW_{S}_DATASET" and "RAW_DATASET" in RECURSOS else ""

def valor_recurso(suf, nom, amb):
    return RECURSOS[suf].format(s=nom.replace("_", "-"), sis=nom, amb=amb)

def convertir_recurso(clave, valor):
    """Llave de recurso (nombre anterior) con valor de la convención vieja (código o nombre anterior) -> valor de la
    convención nueva con el mismo ambiente; None si la llave no es de recurso o el valor no sigue la convención vieja."""
    for sis, nuevo in _cambian().items():
        suf = tipo_recurso(clave, sis)
        if not suf:
            continue
        for amb in sorted(set(AMBIENTE.values())):
            if any(str(valor) == valor_recurso(suf, v, amb) for v in nombres_viejos(sis)):
                return valor_recurso(suf, nuevo, amb)
    return None

def envs_renombrados(envs):
    """Vista de {ambiente: {llave: valor}} con las llaves renombradas (así quedan en la copia de out/). Si la llave nueva
    ya existe en el bloque se deja la nueva y la vieja se conserva con su nombre (se reporta para retiro)."""
    out = {}
    for env, b in envs.items():
        nb = {}
        for k, v in b.items():
            nk = renombrar(k)
            if nk != k and nk in b:
                nb[k] = v
                continue
            nv = convertir_recurso(k, str(v)) if nk != k and v not in (None, "") else None
            nb[nk] = v if nv is None else nv
        out[env] = nb
    return out

def _val_com(rest):
    """Resto de una línea `llave:` -> (espacio inicial, comilla, valor, comentario con su espacio)."""
    vp = strip_comment(rest)
    cmt = rest[len(vp):]
    lead = vp[:len(vp) - len(vp.lstrip())]
    v = vp.strip()
    if len(v) >= 2 and v[0] in "'\"" and v[-1] == v[0]:
        return lead, v[0], (v[1:-1].replace("''", "'") if v[0] == "'" else v[1:-1]), cmt
    return lead, "", v, cmt

def _renombra_linea(l, k, nk, info):
    m = re.match(r"^(\s*['\"]?)([\w.\-]+)(['\"]?\s*:)(.*)$", l)
    pre, sep, rest = m.group(1), m.group(3), m.group(4)
    lead, q, val, cmt = _val_com(rest)
    nv = convertir_recurso(k, val) if val else None
    info["llaves"].append((k, nk))
    if nv is None:
        if val and any(tipo_recurso(k, c) for c in _cambian()):
            info["recursos_otro_valor"].append(nk)          # llave de recurso con valor fuera de la convención: se conserva
        return pre + nk + sep + rest
    info["recursos"].append(nk)
    qq = q or "'"
    tail = (cmt.rstrip() + " " + NOTA_RENOMBRE) if cmt.strip() else ("   " + NOTA_RENOMBRE)
    return pre + nk + sep + lead + qq + (nv.replace("'", "''") if qq == "'" else nv) + qq + tail

def renombrar_regiones(lines, regiones, info):
    """Renombra EN SITIO (misma línea, mismo valor, mismo comentario) cada llave con el código anterior dentro de las
    regiones [(ini, fin)] y el título de grupo `# <código>`. Llaves de recurso con valor de la convención vieja:
    valor nuevo + nota. Si la llave nueva ya existe en la región se deja y la vieja se anota en info['viejas']."""
    lines = list(lines)
    cam = {c.lower() for c in _cambian()}
    for ini, fin in regiones:
        ya = {llave_de(l) for l in lines[ini:fin]} - {None}
        titulos = {l.strip().lstrip("#").strip().lower() for l in lines[ini:fin] if l.lstrip().startswith("#")}
        for j in range(ini, fin):
            l = lines[j]
            k = llave_de(l)
            if k:
                nk = renombrar(k)
                if nk == k:
                    continue
                if nk in ya:
                    info["viejas"].append(k)
                    continue
                lines[j] = _renombra_linea(l, k, nk, info)
            elif l.lstrip().startswith("#"):
                t = l.strip().lstrip("#").strip()
                if t.lower() in cam and renombrar(t) != t and renombrar(t).lower() not in titulos:
                    lines[j] = l.replace(t, renombrar(t), 1)
                    info["titulos"] += 1
    return lines

def _equiv_renombre(o, n):
    """¿La línea n es la línea o con solo un renombre permitido (llave, título o valor de recurso + nota)?"""
    if o == n:
        return True
    ko, kn = llave_de(o), llave_de(n)
    if ko and kn:
        if ko == kn or renombrar(ko) != kn or o.split(ko, 1)[0] != n.split(kn, 1)[0]:
            return False
        ro, rn = o.split(ko, 1)[1], n.split(kn, 1)[1]
        if ro == rn:
            return True
        l1, q1, v1, c1 = _val_com(ro.split(":", 1)[1])
        l2, q2, v2, c2 = _val_com(rn.split(":", 1)[1])
        nv = convertir_recurso(ko, v1)
        return (nv is not None and v2 == nv and l1 == l2 and (q1 or "'") == q2
                and c2 == ((c1.rstrip() + " " + NOTA_RENOMBRE) if c1.strip() else ("   " + NOTA_RENOMBRE)))
    if o.lstrip().startswith("#") and n.lstrip().startswith("#"):
        t = o.strip().lstrip("#").strip()
        return t.lower() in {c.lower() for c in _cambian()} and n == o.replace(t, renombrar(t), 1)
    return False

def verificar_renombre(base, lines):
    """Control del renombre en sitio: mismas líneas, en el mismo orden, y cada una igual o con un renombre permitido.
    -> texto del problema o ''."""
    if len(base) != len(lines):
        return "cambió la cantidad de líneas"
    mal = next((i for i, (o, n) in enumerate(zip(base, lines)) if not _equiv_renombre(o, n)), None)
    return "" if mal is None else f"línea {mal + 1} alterada fuera de lo permitido"

def info_renombre():
    return {"llaves": [], "recursos": [], "recursos_otro_valor": [], "viejas": [], "titulos": 0, "residuales": [], "fuentes": []}

def residuales(texto, donde):
    """Líneas del texto que aún nombran el código anterior (tras el renombre): se reportan en ESTADO.md."""
    rx = re.compile("|".join(rf"(?<![A-Za-z0-9]){re.escape(c)}(?![A-Za-z0-9])" for c in _cambian()), re.I) if _cambian() else None
    return [f"{donde} línea {i}" for i, l in enumerate(texto.splitlines(), 1) if rx and rx.search(l)]


# ----------------------------------------------------------------------------- utilidades
def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")

def read_csv(path):
    raw = Path(path).read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")          # CSV guardado por Excel en Windows
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return []
    dialect = csv.Sniffer().sniff(lines[0], delimiters=";,\t")
    return [{norm(k): (v or "").strip() for k, v in r.items() if k} for r in csv.DictReader(lines, dialect=dialect)]

def write_csv(path, rows, cols):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=";", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def load_refyaml(text):
    """YAML con marcadores {LLAVE} sin comillas: los protege para que no se lean como mapas."""
    safe = re.sub(r"(?<![{$])\{([A-Z][A-Z0-9_\-]*)\}(?!\})", r"${\1}", text)
    try:
        doc = yaml.safe_load(safe) or {}
    except Exception:
        return {}
    def back(x):
        if isinstance(x, dict):
            return {k: back(v) for k, v in x.items()}
        if isinstance(x, list):
            return [back(v) for v in x]
        return re.sub(r"\$\{([A-Z][A-Z0-9_\-]*)\}", r"{\1}", x) if isinstance(x, str) else x
    return back(doc)

def load_yaml(p):
    try:
        return load_refyaml(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return {}

def env_blocks(artifacts, archivo=None):
    doc = load_yaml(Path(archivo) if archivo else Path(artifacts) / "co/variables.yaml")
    root = doc.get("enviroments") or doc.get("environments") or doc
    out = {}
    for it in (root if isinstance(root, list) else [root]):
        if isinstance(it, dict):
            out.update({k: v for k, v in it.items() if isinstance(v, dict)})
    return out

def envs_dl2(artifacts, archivo=None):
    """Bloques de variables.yaml con las llaves del nombre DL 2.0 (vista renombrada; el archivo del repo no se toca)."""
    return envs_renombrados(env_blocks(artifacts, archivo))

def model_vars(models):
    v = dict(load_yaml(Path(models) / "dbt_project.yml").get("vars") or {})
    amb = v.pop("ambientes", {}) or {}
    return {renombrar(k) for k in set(v).union(*[set(b or {}) for b in amb.values()])}

def get(d, path, default=None):
    for k in path.split("."):
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d

def put(d, path, value):
    ks = path.split(".")
    for k in ks[:-1]:
        if not isinstance(d.get(k), dict):
            d[k] = {}
        d = d[k]
    d[ks[-1]] = value

def get_ref(d, path):
    if "#" in path:
        base, key = path.split("#", 1)
        return next((i.get("value") for i in (get(d, base) or []) if isinstance(i, dict) and i.get("key") == key), None)
    return get(d, path)

def put_ref(d, path, value):
    if "#" not in path:
        return put(d, path, value)
    base, key = path.split("#", 1)
    lst = get(d, base)
    if not isinstance(lst, list):
        lst = []
        put(d, base, lst)
    item = next((i for i in lst if isinstance(i, dict) and i.get("key") == key), None)
    if item:
        item["value"] = value
    else:
        lst.append({"key": key, "value": value})

def exists_ref(d, path):
    if "#" in path:
        return isinstance(get(d, path.split("#", 1)[0]), list)
    return get(d, path) is not None

def flat_refs(doc, pre=""):
    """{ruta: valor} de las hojas con marcador {LLAVE}; listas key/value como ruta#key."""
    out = {}
    if isinstance(doc, dict):
        for k, v in doc.items():
            out.update(flat_refs(v, f"{pre}.{k}" if pre else str(k)))
    elif isinstance(doc, list):
        for v in doc:
            if isinstance(v, dict) and "key" in v and "value" in v:
                out.update(flat_refs(v["value"], f"{pre}#{v['key']}"))
    elif isinstance(doc, str) and KEYREF.search(doc) and "\n" not in doc:
        out[pre] = doc
    return out

def code(v):
    """Etiqueta del Excel o código -> código de sistema (entrada). El nombre DL 2.0 (admisiones) también se acepta."""
    v = (v or "").strip()
    if v in CATALOGO:
        return CATALOGO[v]
    return codigo_sis(v if re.fullmatch(r"[a-z0-9_]+", v) else (PEND if not v else norm(v)))

def operacional(v):
    return (v or "").strip() or PEND

def checks_dominio(t, models):
    """Pendientes de dominio/subdominio: formato de label y carpeta std_<dominio> existente en data-models."""
    out = []
    for k in ("dominio", "subdominio"):
        v = t[k]
        if filled(v) and not LABEL.fullmatch(v):
            out.append(f"{k} '{v}' no sirve como label/dataset (minúsculas, números, _ o -, máx. 63): "
                       "usar el código operacional definido, sin traducir")
    d = t["dominio"]
    if filled(d) and LABEL.fullmatch(d) and not (Path(models) / f"{DBT}/models" / f"std_{d}").is_dir():
        out.append(f"dominio '{d}' sin carpeta std_{d} en data-models: confirmar el dominio "
                   "(uno nuevo requiere dataset, __sources.yml y models: en dbt_project.yml)")
    return out

TAXO = {}

def load_taxonomia():
    """Taxonomía oficial de datasets del cliente: catalogos/dominios_operacionales.csv."""
    if "filas" not in TAXO:
        p = next((x for x in (IN / "dominios_operacionales.csv", HERE / "catalogos" / "dominios_operacionales.csv")
                  if x.exists()), None)
        TAXO["filas"] = read_csv(p) if p else []
    return TAXO["filas"]

def resolver_dominio(t, models):
    """Valida dominio/subdominio contra la taxonomía oficial. Acepta, tal como vienen, el nombre, el dataset o el
    sharing dataset. Labels (domain/subdomain en YAML, JSON y modelo) = Domain/SubDomain Sharing Dataset, los nombres
    de la capa prd que reemplaza las maestras; la carpeta y el dataset std siguen el Domain Dataset (std_customer).
    Sin taxonomía: solo formato y carpeta."""
    filas = load_taxonomia()
    if not filas:
        return checks_dominio(t, models)
    d_in, s_in, out = t["dominio"], t["subdominio"], []
    kd = lambda f: {norm(x) for x in (f["domain"], f["domain_dataset"], f["domain_dataset"].removeprefix("std_"),
                                      f["domain_sharing_dataset"])}
    ks = lambda f: {norm(x) for x in (f["subdomain"], f["subdomain_dataset"], f["subdomain_dataset"].removeprefix("prd_"),
                                      f["subdomain_sharing_dataset"])}
    doms = [f for f in filas if filled(d_in) and norm(d_in) in kd(f)]
    if filled(d_in) and not doms:
        return [f"dominio '{d_in}' no está en la taxonomía oficial (dominios_operacionales.csv)"]
    if doms:
        t["dominio"] = doms[0]["domain_dataset"].removeprefix("std_")          # carpeta/dataset std_<dominio>
        t["dominio_label"] = doms[0]["domain_sharing_dataset"]                  # label domain
        sub = next((f for f in doms if filled(s_in) and norm(s_in) in ks(f)), None)
        if sub:
            t["subdominio"] = sub["subdomain_sharing_dataset"]
        elif filled(s_in):
            out.append(f"subdominio '{s_in}' no pertenece al dominio {doms[0]['domain']} en la taxonomía oficial")
        if not (Path(models) / f"{DBT}/models" / f"std_{t['dominio']}").is_dir():
            out.append(f"std_{t['dominio']} (oficial) sin carpeta en data-models: dominio nuevo, requiere dataset, "
                       "__sources.yml y models: en dbt_project.yml (coordinar con TL)")
    return out

def label_safe(v):
    """Valor válido como label de BigQuery: minúsculas, números, _ y -, máx. 63. Los marcadores <...>/{...} no se tocan.
    Un buzón (equipo@dominio.com) queda equipo_dominio_com."""
    v = str(v or "")
    if not v or MARK.fullmatch(v) or PH.fullmatch(v) or v == PEND:
        return v
    v = re.sub(r"[^a-z0-9_-]+", "_", unicodedata.normalize("NFKD", v).encode("ascii", "ignore").decode().lower())
    return v.strip("_")[:63]


def ref(v):
    """LLAVE o {LLAVE} -> {LLAVE}; PEND se conserva."""
    v = (v or "").strip()
    if not v or v == PEND:
        return PEND
    return v if v.startswith(("{", "<")) else "{" + v + "}"

def key_of(v):
    m = KEYREF.fullmatch((v or "").strip())
    return m.group(1) if m else ""

def filled(v):
    return bool(v) and v != PEND

def templ(v, sis, name):
    return v.replace(name.upper(), "<NAME>").replace(name, "<name>").replace(sis.upper(), "<SIS>").replace(sis, "<sis>")

def fill(t, sis, name):
    return t.replace("<NAME>", name.upper()).replace("<name>", name).replace("<SIS>", sis.upper()).replace("<sis>", sis)

class Lit(str):
    pass

class QStr(str):
    """Valor string que se emite SIEMPRE entre comillas (las llaves de los mapas quedan planas)."""

def _rep_qstr(d, v):
    doble = "'" in v or any(ch in v for ch in "\n\r\t\x85\u2028\u2029") or any(not ch.isprintable() for ch in v)
    return d.represent_scalar("tag:yaml.org,2002:str", str(v), style='"' if doble else "'")

yaml.add_representer(Lit, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", s, style="|"), Dumper=yaml.SafeDumper)
yaml.add_representer(QStr, _rep_qstr, Dumper=yaml.SafeDumper)
yaml.add_representer(str, lambda d, v: d.represent_scalar(
    "tag:yaml.org,2002:str", v, style="'" if re.fullmatch(r"[0-9][0-9_.:]*", v) else None), Dumper=yaml.SafeDumper)

def citar(x):
    """Envuelve en QStr todo valor str (mapas y listas, a cualquier profundidad); las llaves, int, bool, None y los
    bloques literales (Lit) quedan como están."""
    if isinstance(x, dict):
        return {k: citar(v) for k, v in x.items()}
    if isinstance(x, list):
        return [citar(v) for v in x]
    return QStr(x) if type(x) is str else x

def dump(d):
    out = yaml.safe_dump(citar(d), sort_keys=False, allow_unicode=True, width=4096)
    return re.sub(r"'\{\{ var\(''([A-Z0-9_]+)''\) \}\}'", lambda m: '"{{ var(\'%s\') }}"' % m.group(1), out)

def tiene_marca(path):
    try:
        return MARCA in Path(path).read_text(encoding="utf-8", errors="replace")[:4000]
    except Exception:
        return False


# ----------------------------------------------------------------------------- catálogo de policy tags
STOP = set(("de del la el los las en con sin un una por para como es no que se al y a su sus o e lo "
            "dato datos cliente clientes banco sistema especifico generico individual consolidado total "
            "actual numerico usar tag institucional general asociada asociado").split()) | set(CLI["palabras_vacias_extra"])
ABREV = {"num": "numero", "nro": "numero", "nmro": "numero", "numr": "numero", "fec": "fecha", "fch": "fecha",
         "fecha": "fecha", "cta": "cuenta", "ctas": "cuenta", "cod": "codigo", "tel": "telefono", "telf": "telefono",
         "dir": "direccion", "dire": "direccion", "ident": "identificacion", "idnt": "identificacion",
         "idntfccion": "identificacion", "nom": "nombre", "nmb": "nombre", "nomb": "nombre", "ape": "apellido",
         "apel": "apellido", "vlr": "valor", "val": "valor", "sdo": "saldo", "mto": "monto", "doc": "documento",
         "docu": "documento", "est": "estado", "cli": "cliente", "clte": "cliente", "nac": "nacimiento",
         "ced": "cedula", "suc": "sucursal", "oper": "operacion", "mov": "movimiento", "trx": "transaccion",
         "tip": "tipo", "corr": "correo", "cel": "celular", "tarj": "tarjeta", "pol": "poliza", "hip": "hipoteca",
         "ing": "ingreso", "venc": "vencimiento", "aper": "apertura", "prom": "promedio", "disp": "disponible",
         "naci": "nacionalidad", "ciu": "ciudad", "emp": "empresa", "ocup": "ocupacion", "prod": "producto",
         "act": "actividad", "egr": "egreso", "dpto": "departamento", "mpio": "municipio", "resid": "residencia"}
RANGO = {"HC": 0, "SC": 1, "NC": 2, "IU": 4}
ATRIBUTO = {"tipo", "clase", "origen", "flag", "estado", "motivo", "canal", "indicador", "categoria"}
DEBIL = {"origen", "status", "serie", "cargo", "plazo", "meses", "vigencia", "digital", "proceso", "sistema", "llave"}
STOP_MIN = {"de", "del", "la", "el", "los", "las", "al", "y", "o", "en", "a"}

def stem(w):
    return w[:-1] if len(w) > 4 and w.endswith("s") else w

def frase(texto, stop=STOP_MIN):
    """Texto -> tupla de alternativas por palabra (ejecutivo/empleado = una palabra con 2 opciones)."""
    texto = re.sub(r"\([^)]*\)", " ", texto)
    out = []
    for w in re.split(r"\s+|_", texto.strip()):
        alts = {stem(norm(x)) for x in w.split("/") if norm(x)} - stop
        if alts:
            out.append(frozenset(alts))
    return tuple(out)

# Palabras frecuentes en nombres de columna que no están en el catálogo: solo sirven para partir nombres pegados.
DOMINIO = set("""solicitud persona oficina principal otro segundo primer primero tercer numero codigo motivo cancelacion
canal tipo tramite estado movil terminal referido referidor externo dia tiempo residencia fiscal pais departamento
municipio barrio extension proveedor inversion entidad rentista capital rango fuerza militar asesor creador descripcion
costo gasto egreso ingreso actividad economica financiera correspondencia auto retenedor origen documento expedicion
lugar valor fecha hora usuario registro modificacion creacion version anterior nuevo total mensual anual eps tin ciiu
nivel educativo profesion conyuge hijo personas cargo dependiente vivienda arriendo propia familiar""".split())
# Término de columna -> palabras del catálogo equivalentes. El tag lo sigue decidiendo el catálogo (claves,
# ejemplos y exclusiones); se amplía con in/sinonimos.csv (termino;equivale_a) sin tocar el script.
SINONIMOS = {"barrio": "direccion", "departamento": "region", "municipio": "ciudad", "residencia": "domicilio",
             "extension": "telefono", "tin": "nit", "ciiu": "actividad economica", "rentista": "ocupacion",
             "militar": "empleo", "egreso": "capacidad pago", "gasto": "capacidad pago", "salario": "sueldo",
             "pais": "nacionalidad", "profesion": "ocupacion", "conyuge": "estado civil"}
# IDs de entidades internas (llaves surrogadas): sin otra coincidencia -> PT_IU_OTHERS.
ID_INTERNO = {"solicitud", "financiera", "oficina", "correspondencia", "registro", "proceso", "actividad", "lote",
              "carga", "secuencia", "consecutivo", "movil", "transaccion_interna", "version"}
ID_PERSONA = {"persona", "cliente", "titular", "deudor", "codeudor", "beneficiario"}
# Columnas formadas solo por estas palabras = código/atributo de proceso: se sugiere Others y se escala para confirmar.
PROCESO = {"tipo", "estado", "motivo", "codigo", "canal", "terminal", "cancelacion", "tramite", "solicitud", "persona",
           "flag", "indicador", "clase", "categoria", "origen", "referido", "referidor", "externo", "interno"}
LEX = {"vocab": set(), "sin": dict(SINONIMOS), "memo": {}}

def _dist1(a, b):
    """True si a y b difieren en una edición (inserción, borrado, sustitución o transposición)."""
    if a == b or abs(len(a) - len(b)) > 1:
        return a == b
    i = 0
    while i < min(len(a), len(b)) and a[i] == b[i]:
        i += 1
    return (a[i + 1:] == b[i + 1:] or a[i + 1:] == b[i:] or a[i:] == b[i + 1:]
            or (a[i + 2:] == b[i + 2:] and a[i:i + 2] == b[i:i + 2][::-1]))

def _fuzzy(w):
    hits = {v for v in LEX["vocab"] if len(v) >= 6 and abs(len(v) - len(w)) <= 1 and _dist1(w, v)}
    return next(iter(hits)) if len(hits) == 1 else None

def segmentar(t):
    """SEGUNDOAPELLDO -> [segundo, apellido]: menor número de palabras del vocabulario; tolera un error
    de tipeo en palabras de 6+ letras. Si no cubre todo el nombre, lo deja igual."""
    V = LEX["vocab"]
    if len(t) < 6 or t in V or not V:
        return [t]
    if t in LEX["memo"]:
        return LEX["memo"][t]
    n, best = len(t), [None] * (len(t) + 1)
    best[0] = (0, [])
    for i in range(n):
        if best[i] is None:
            continue
        for j in range(i + 2, min(n, i + 16) + 1):
            w = t[i:j]
            if w in V:
                cand = (best[i][0] + 1, best[i][1] + [w])
            elif j - i >= 6 and (f := _fuzzy(w)):
                cand = (best[i][0] + 2, best[i][1] + [f])
            else:
                continue
            if best[j] is None or cand[0] < best[j][0]:
                best[j] = cand
    LEX["memo"][t] = out = best[n][1] if best[n] else [t]
    return out

def tokens(col):
    base = []
    for t in (x for x in norm(col).split("_") if x):
        base.append(t)
        if t.startswith("id") and len(t) > 4:                               # IDPERSONA -> persona
            base.append(t[2:])
            t = t[2:]
        partes = segmentar(t)
        if len(partes) > 1 or partes[0] != t:
            base += partes
    base += [ABREV[t] for t in base if t in ABREV]
    T = {stem(t) for t in base}
    for t in list(base):
        for k in (t, stem(t)):
            if k in LEX["sin"]:
                T |= {stem(w) for w in LEX["sin"][k].split()}
    return T

def palabras(col):
    """Palabras del nombre ya partidas (sin el prefijo id): IDPAISRESIDENCIAFISCAL -> pais, residencia, fiscal."""
    out = []
    for t in (x for x in norm(col).split("_") if x):
        t = t[2:] if t.startswith("id") and len(t) > 4 else t
        out += segmentar(t)
    return out

def en(w, S):
    return any(w == e or stem(w) == e or (len(e) >= 6 and _dist1(w, e)) or (len(e) >= 6 and w.startswith(e)) for e in S)

def sinonimos_usados(col, why):
    usadas = {stem(w) for a in frase(why.split(":", 1)[-1]) for w in a}
    out = []
    for t in palabras(col):
        k = t if t in LEX["sin"] else stem(t) if stem(t) in LEX["sin"] else None
        if k and {stem(w) for w in LEX["sin"][k].split()} & usadas:
            out.append(f"{k}→{LEX['sin'][k]}")
    return sorted(set(out))

def palabra_ok(alts, T):
    for w in alts:
        if len(w) <= 2:
            continue                                         # "id", "ip": solo coincidencia exacta del nombre
        if w in T or any(len(t) >= 4 and (w.startswith(t) or t.startswith(w)) and len(w) >= 4 for t in T):
            return True
    return False

def frase_ok(fr, T):
    return bool(fr) and all(palabra_ok(a, T) for a in fr)

def load_catalogo(path):
    p = Path(path) if path else next((x for x in (IN / "policy_tags.csv", HERE / "catalogos" / "policy_tags.csv")
                                      if x.exists()), IN / "policy_tags.csv")
    if not p.exists():
        sys.exit(f"Falta el catálogo de policy tags: {p}")
    cat, nombres = {}, {}
    for r in read_csv(p):
        pt = (r.get("nomenclatura") or "").strip().upper()
        if not pt.startswith("PT_"):        # POLICY_TAG_CUARENTENA no existe como llave: no se asigna
            continue
        split = lambda v: [x.strip() for x in re.split(r",", v or "") if x.strip() and x.strip() != "$"]
        cat[pt] = {"nombre": r.get("policy_tag", ""), "clasif": r.get("clasificacion", ""),
                   "ejemplos": split(r.get("ejemplos_de_campos")), "claves": split(r.get("palabras_clave")),
                   "excluir_txt": r.get("no_clasificar_aqui", ""), "excluir": []}
        nombres[norm(r.get("policy_tag", ""))] = pt
    for pt, d in cat.items():                                # "concepto → Policy Tag destino"
        for m in re.finditer(r"([^.→]+?)\s*(?:→|->)\s*([^.]+)", d["excluir_txt"]):
            dest_n = norm(m.group(2))
            dest = nombres.get(dest_n) or next((v for k, v in sorted(nombres.items(), key=lambda x: -len(x[0]))
                                                if k and k in dest_n), None)
            alts = [frase(x, STOP | STOP_MIN) for x in re.split(r"\s+o\s+", m.group(1))]
            d["excluir"].append((alts, dest, m.group(1).strip()))
    voc = set(DOMINIO) | {k for k in ABREV if len(k) >= 3} | set(ABREV.values())
    sp = next((x for x in (IN / "sinonimos.csv", HERE / "catalogos" / "sinonimos.csv") if x.exists()), None)
    if sp:
        LEX["sin"].update({norm(r["termino"]).replace("_", ""): norm(r["equivale_a"]).replace("_", " ")
                           for r in read_csv(sp) if r.get("termino") and r.get("equivale_a")})
    voc |= set(LEX["sin"])
    for d in cat.values():
        for it in d["ejemplos"] + d["claves"] + [d["nombre"]]:
            voc |= {w for a in frase(it) for w in a if len(w) >= 3}
    LEX["vocab"] = voc | {w + "s" for w in voc} | {w + "es" for w in voc if not w.endswith("s")}
    LEX["memo"] = {}
    ret = {}
    rp = p.with_name("policy_tags_retiradas.csv")
    if rp.exists():
        ret = {r["tag_retirado"].lstrip("#").upper(): r["reemplazo"].upper() for r in read_csv(rp) if r.get("tag_retirado")}
    print(f"  catálogo: {len(cat)} policy tags · {sum(len(d['excluir']) for d in cat.values())} exclusiones · "
          f"{len(ret)} retirados")
    return {"tags": cat, "retiradas": ret}

def clasificar(col, cat):
    """-> (tag sugerido, regla, observación). Others solo si nada más coincide; empate -> el más restrictivo."""
    T, c = tokens(col), norm(col)
    plano = c.replace("_", "")
    scores = []
    for pt, d in cat["tags"].items():
        best, why = 0, ""
        for kind, items, base in (("ejemplo", d["ejemplos"], 30), ("clave", d["claves"], 20)):
            for it in items:
                n, fr = norm(it), frase(it)
                if n == c:
                    s = 100
                elif len(fr) == 1 and len(n) <= 2:
                    s = 0
                elif frase_ok(fr, T):
                    s = base + 5 * len(fr)
                elif len(n.replace("_", "")) >= 6 and n.replace("_", "") in plano:
                    s = base + 5 * len(fr) if len(fr) > 1 else 10   # palabras pegadas: FECHANACIMIENTO
                else:
                    s = 0
                if s > best:
                    best, why = s, f"{kind}:{it}"
        if best:
            scores.append((best, pt, why))
    # exclusión de cualquier tag con 2+ palabras ("País de residencia → POC-Address") = evidencia fuerte
    rutas, simples = {}, {}
    for pt, d in cat["tags"].items():
        for alts, dest, texto in d["excluir"]:
            for fr in alts:
                largas = tuple(a for a in fr if any(len(w) > 2 for w in a))
                if not largas or not dest or any(len(w) <= 2 for a in fr for w in a):
                    continue
                if len(largas) > 1 and frase_ok(largas, T):
                    rutas[dest] = texto
                elif len(largas) == 1 and frase_ok(largas, T):
                    simples.setdefault(dest, texto)
    for dest, texto in rutas.items():
        scores = [s for s in scores if s[1] != dest] + [(max(60, max((s[0] for s in scores if s[1] == dest), default=0)),
                                                          dest, f"exclusión:{texto}")]
    ent = [w for t in c.split("_") if t.startswith("id") and len(t) > 4 for w in segmentar(t[2:])]
    if not scores or all(norm(s[2].split(":", 1)[-1]) in DEBIL for s in scores):
        if ent and all(en(w, ID_INTERNO | ID_PERSONA | {"origen"}) for w in ent):
            if any(en(w, ID_PERSONA) for w in ent):
                return OTHERS, "id interno", "identificador de persona: escalar (puede ser el documento -> ID document)"
            return OTHERS, "id interno", "llave interna (ID) sin dato del cliente"
        pal = [w for w in palabras(col) if len(w) > 3 or w in LEX["vocab"]]      # APS_, ATL_ sin quitar
        if pal and all(en(w, PROCESO) for w in pal):
            return OTHERS, "atributo de proceso", "código/atributo de proceso: escalar para confirmar Others"
    if not scores:
        return "", "", "sin coincidencia en catálogo"
    # Others es el último recurso: compite solo con coincidencia exacta o si nada más coincide
    oth = next((s for s in scores if s[1] == OTHERS), None)
    especificos = [s for s in scores if s[1] != OTHERS]
    if not especificos or (oth and oth[0] == 100 and max(s[0] for s in especificos) < 100):
        especificos = [oth]
    top = max(s[0] for s in especificos)
    empatados = sorted((s for s in especificos if s[0] == top), key=lambda s: RANGO.get(s[1][3:5], 3))
    _, tag, why = empatados[0]
    obs = []
    if len({s[1] for s in empatados}) > 1:
        guia = [s for s in empatados if s[1] in simples]
        if len(guia) == 1:                                   # "Renta o ingresos → Economic capacity"
            _, tag, why = guia[0]
            obs.append(f"desempate por exclusión '{simples[tag]}'")
        else:
            obs.append("ambiguo: " + " | ".join(s[1] for s in empatados))
    sin = sinonimos_usados(col, why)
    if sin:
        obs.append("por sinónimo (" + ", ".join(sin) + "): validar")
    if oth and tag != OTHERS and oth[0] >= top:
        obs.append(f"también coincide con {OTHERS} ({oth[2]})")
    for alts, dest, texto in cat["tags"][tag]["excluir"]:
        for fr in alts:
            largas = tuple(a for a in fr if any(len(w) > 2 for w in a))
            cortas = [w for a in fr for w in a if len(w) <= 2]
            if not fr:
                continue
            if largas and frase_ok(largas, T) and not cortas and (len(largas) > 1 or len(next(iter(largas[0]))) >= 4):
                if dest:
                    return dest, f"exclusión de {tag}: '{texto}'", "; ".join(obs)
                obs.append(f"exclusión de {tag} sin destino: '{texto}'")
            elif cortas and set(cortas) & set(norm(col).split("_")):
                obs.append(f"verificar exclusión de {tag}: '{texto}'" + (f" -> {dest}" if dest else ""))
    palabra = norm(why.split(":", 1)[-1])
    if why.startswith("clave:") and palabra in DEBIL:
        obs.append(f"coincidencia débil ({why}): escalar")
    attr = sorted((T & ATRIBUTO) - set(palabra.split("_")))
    if attr and tag != OTHERS:
        obs.append(f"atributo ({', '.join(attr)}) de {cat['tags'][tag]['nombre']}: verificar si es el dato o un catálogo")
    if any(t.startswith("id") for t in norm(col).split("_")) and tag != OTHERS:
        obs.append("identificador (ID): verificar si es llave interna (Others) o dato del cliente")
    return tag, why, "; ".join(dict.fromkeys(obs))

def normalizar_tag(tag, cat):
    t = (tag or "").strip().strip("<>").lstrip("#").upper()
    if t in cat["retiradas"]:
        new = cat["retiradas"][t]
        return (new, f"{t} retirado -> {new}") if new != PEND else (PEND, f"{t} retirado sin reemplazo: escalar")
    if t.startswith("PT_IU_") and t not in cat["tags"]:
        return OTHERS, f"{t} retirado -> {OTHERS}"
    return t, ""


# ----------------------------------------------------------------------------- índices y convención
def index_dl2(artifacts, envs):
    idx, infos, base = {}, [], Path(artifacts) / "co/data_source"
    for y in sorted(base.rglob("*.yaml")):
        if y.parent.name == "all_tables":             # v6.7: la prueba de conexión no es tabla hermana ni entra en la convención
            continue
        text = y.read_text(encoding="utf-8", errors="replace")
        doc = load_refyaml(text)
        frm = re.search(r"FROM\s+\{([A-Z0-9_]+)\}\.([A-Z0-9_$#]+)", text, re.I)
        owner = frm.group(1) if frm else ""
        vals = [str(v.get(owner) or v.get(renombrar(owner), "")) for v in envs.values()] if owner else []
        js = next(iter(sorted(y.parent.glob("*.json"))), None)
        refs = flat_refs(doc)
        if owner:
            refs["@owner"] = "{%s}" % owner
        info = {
            "yaml": y.relative_to(artifacts).as_posix(),
            "json": js.relative_to(artifacts).as_posix() if js else "",
            "sistema": y.relative_to(base).parts[0], "tabla": y.parent.name,
            "raw_table": get(doc, "gcp.bigquery.raw.raw_table", "") or "",
            "dbt_models": [s["dbt_model"] for s in (get(doc, "gcp.bigquery.trf_steps", []) or [])
                           if isinstance(s, dict) and s.get("dbt_model")],
            "scheduling": get(doc, "gcp.composer.extraction.scheduling", "") or "",
            "extractor": "cipher" if get(doc, "extractor.type") == "cipher" else "jdbc",
            "owner_key": owner, "from_table": frm.group(2).upper() if frm else "", "refs": refs,
            "lee_ods": bool(frm and frm.group(2).upper().startswith("STG_")) or any(ODS_OWNER.match(x) for x in vals),
            "marca": MARCA in text[:4000],
        }
        infos.append(info)
        for k in {info["from_table"], str(info["raw_table"]).upper()} - {""}:
            idx.setdefault(k, info)
    return idx, infos

def convencion(infos):
    """Plantilla de valor más usada por ruta, con <SIS>/<NAME> en lugar del sistema/tabla (solo ingestas que no leen ODS)."""
    c = defaultdict(Counter)
    for i in infos:
        if not i["lee_ods"]:
            for p, v in i["refs"].items():
                c[p][templ(v, i["sistema"], f"{i['sistema']}_{i['tabla']}")] += 1
    return {p: cnt.most_common(1)[0][0] for p, cnt in c.items()}

def refs_base(sis, name, caso, hit, infos, conv):
    """Referencias por ruta: convención < tabla hermana del mismo sistema < archivo existente (B: sin conexión)."""
    sis, name = nombre_sis(codigo_sis(sis)), renombrar(name)      # lo emitido usa el nombre DL 2.0
    out = {p: fill(v, sis, name) for p, v in conv.items()}
    src = {p: "convención" for p in out}
    sib = next((i for i in infos if codigo_sis(i["sistema"]) == codigo_sis(sis) and not i["lee_ods"] and i is not hit), None)
    if sib:
        sname = f"{sib['sistema']}_{sib['tabla']}"
        for p, v in sib["refs"].items():
            out[p], src[p] = fill(templ(v, sib["sistema"], sname), sis, name), "hermano"
    if hit:
        for p, v in hit["refs"].items():
            if not (caso == "B" and p in CONN):
                out[p], src[p] = renombrar(v), "existente"
    return out, src

def split_cols(texto):
    """Separa por comas fuera de paréntesis y comillas: TO_CHAR(SYSDATE, 'YYYY') AS X es una sola columna."""
    out, buf, depth, q = [], "", 0, None
    for ch in str(texto or ""):
        if q:
            q = None if ch == q else q
        elif ch in "'\"":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append(buf.strip()); buf = ""
            continue
        buf += ch
    out.append(buf.strip())
    return [x for x in out if x]

def col_alias(x):
    """'EXPR AS ALIAS' -> (ALIAS, EXPR); 'COL' -> (COL, '')."""
    m = re.match(r"(?s)(.+?)\s+AS\s+([\w$#]+)$", x.strip(), re.I)
    return (m.group(2), m.group(1).strip()) if m else (x.strip(), "")

def index_v1(ingdfw):
    idx = {}
    for c in sorted(Path(ingdfw).rglob("cookiecutter.json")):
        try:
            d = json.loads(c.read_text(encoding="utf-8"))
        except Exception:
            continue
        rel = c.relative_to(ingdfw).parts
        src = next((v for v in d.values() if isinstance(v, dict) and "cols" in v), {})
        tn = str(d.get("table_name") or "")
        qm = re.search(r"FROM\s+[\w$#]+\.([\w$#]+)", str(src.get("query") or ""), re.I)
        info = {
            "path": "/".join(rel[:-1]), "tecnologia": rel[0], "tabla": tn.split(".")[-1].upper(),
            "carpeta": rel[1] if len(rel) > 2 else "",
            "cols": [col_alias(x)[0] for x in split_cols(src.get("cols"))],
            "exprs": {col_alias(x)[0].upper(): col_alias(x)[1] for x in split_cols(src.get("cols")) if col_alias(x)[1]},
            "tipos": [x.strip() for x in str(src.get("cols_data_type") or "").split(",") if x.strip()],
            "cifradas": [x.strip() for x in str(get(d, "encryption.encrypt_cols", "") or "").split(",")
                         if x.strip() and x.strip() != "--"],
            "incremental": str(d.get("is_incremental_load", "")).lower() == "true",
        }
        for k in {tn.split(".")[-1].upper(), (qm.group(1).upper() if qm else ""),
                  str(d.get("raw_bigquery_table_name") or "").upper(), c.parent.name.upper()} - {""}:
            idx.setdefault(k, []).append(info)
    return idx

def json_schema(artifacts, rel):
    try:
        d = json.loads((Path(artifacts) / rel).read_text(encoding="utf-8"))
        return [c for c in d.get("schema", []) if isinstance(c, dict) and c.get("name")]
    except Exception:
        return []

def std_type(col, v1_type):
    t = (v1_type or "").upper()
    if t and t != "STRING":
        return {"NUMBER": "NUMERIC", "INTEGER": "INT64", "TIMESTAMP": "DATETIME"}.get(t, t)
    c = col.upper()
    if re.search(r"(^|_)(FEC|FECHA|DATE)", c):
        return "DATE"
    if re.search(r"VALOR|MONTO|SALDO|VLR|IMPORTE|CUPO|TASA|INGRESO", c):
        return "NUMERIC"
    return "STRING"

def resolver(row, envs, idx, infos, conv):
    """Localiza la tabla en DL 2.0 y arma sistema/tabla/caso base."""
    nombre = row["tabla_origen"].rpartition(".")[2]
    m = STG.match(nombre)
    base = m.group(2) if m else nombre
    hit = idx.get(nombre.upper()) or idx.get(base.upper())
    # v6.8.1: el sistema lo define la lista y la tabla es el nombre de la tabla de origen tal cual (no quita stg_ ni hereda
    # la carpeta de una ingesta existente; el prefijo stg_ solo sirve para buscar en DL 1.0 / DL 2.0)
    sistema = code(row["sistema_origen"]) if (row.get("sistema_origen") or "").strip() else (codigo_sis(hit["sistema"]) if hit else PEND)
    tabla = norm(re.sub(rf"^{re.escape(sistema)}_", "", nombre, flags=re.I))
    return nombre, m, base, hit, sistema, tabla


# ----------------------------------------------------------------------------- repos
def repos(a):
    root = Path(a.repos or os.environ.get("DL2_REPOS") or HERE.parent.parent / CLI["repos_raiz"])
    for k, d in (("artifacts", CLI["dir_artifacts"]), ("models", CLI["dir_models"]),
                 ("ingdfw", CLI["dir_legacy"])):
        p = Path(getattr(a, k, None) or root / d)
        if not p.is_dir():
            sys.exit(f"No encuentro el repo {d} en {p}: usar --repos <carpeta> o --{k} <ruta>")
        setattr(a, k, p)
    t = Path(a.tablas_pos or a.tablas or "")
    a.tablas = t if t.exists() or t.parent != Path(".") else IN / t.name   # nombre suelto -> in/

def contexto(a):
    envs = envs_dl2(a.artifacts)
    idx, infos = index_dl2(a.artifacts, envs)
    return envs, idx, infos, convencion(infos), load_catalogo(a.catalogo)


# ----------------------------------------------------------------------------- DDL de la fuente y defaults del repo
SURROGATE = re.compile(r"identificador unico|consecutivo|secuencia", re.I)
SIGLAS = {"CIIU", "TIN", "EPS", "IVR", "ID", "CTS", "ATM", "PIN", "NIT", "RUT", "DANE"}

def load_ddl(p):
    """out/<sis>/in/<sis>_ddl.csv (exportado del diccionario de la fuente con ddl_<sis>.sql, solo metadatos):
    table_name;column_id;column_name;data_type;data_precision;data_scale;nullable;comments;pk_position."""
    out = {}
    for r in (read_csv(p) if p.exists() else []):
        t, c = (r.get("table_name") or "").upper(), (r.get("column_name") or "").upper()
        if not t or not c:
            continue
        d = out.setdefault(t, {"cols": {}, "orden": [], "pk": []})
        d["cols"][c] = r
        d["orden"].append((int(r.get("column_id") or 0), c))
        if (r.get("pk_position") or "").strip().isdigit():
            d["pk"].append((int(r["pk_position"]), c))
    for d in out.values():
        d["orden"] = [c for _, c in sorted(d["orden"])]
        d["pk"] = [c for _, c in sorted(d["pk"])]
    return out

ENTEROS = {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"}
DECIMALES = {"NUMBER", "NUMERIC", "DECIMAL", "DEC", "MONEY", "SMALLMONEY", "FLOAT", "REAL", "DOUBLE", "DECFLOAT",
             "BINARY_DOUBLE", "BINARY_FLOAT"}

def ddl_tipo(d):
    """Tipo std desde el DDL. Oracle, SQL Server y DB2 (Oracle DATE trae hora pero DL 1.0 la extraía como fecha)."""
    completo = (d.get("data_type") or "").upper()
    t, esc = completo.split("(")[0].strip(), (d.get("data_scale") or "").strip()
    pre = (d.get("data_precision") or "").strip()
    if t == "DATE":
        return "DATE"
    if "TIME ZONE" in completo or t == "DATETIMEOFFSET":          # v6.8.3: con zona horaria -> TIMESTAMP (DATETIME la pierde)
        return "TIMESTAMP"
    if t.startswith("TIMESTAMP") or t in ("DATETIME", "DATETIME2", "SMALLDATETIME"):
        return "DATETIME"
    if t in ENTEROS:
        return "INT64"
    if t in DECIMALES:
        p = int(pre) if pre.isdigit() else None
        s = int(esc) if re.fullmatch(r"-?\d+", esc) else None
        if s == 0 and t in ("NUMBER", "NUMERIC", "DECIMAL", "DEC"):
            # v6.8.3: INT64 solo hasta 18 dígitos (NUMBER(38,0) desborda y SAFE_CAST lo deja NULL sin error)
            return "INT64" if p and p <= 18 else ("BIGNUMERIC" if p and p > 29 else "NUMERIC")
        if (s is not None and s > 9) or (p and s is not None and p - s > 29):
            return "BIGNUMERIC"                                     # v6.8.3: NUMERIC admite 29 enteros y 9 decimales
        return "NUMERIC"
    return "STRING"

def ddl_desc(coment):
    """'FECHA DE NACIMIENTO' -> 'Fecha de nacimiento' (conserva siglas como CIIU, TIN, EPS)."""
    w = [x if x.strip("()=/,.") in SIGLAS else x.lower() for x in coment.split()]
    txt = " ".join(w)
    return txt[:1].upper() + txt[1:]

def con_ddl(col, coment, sugt, regla, obs0, cat, surrogates):
    """Usa el comentario del DDL cuando el nombre no basta: llaves internas y clasificación por comentario."""
    if regla == "id interno" and "escalar" in obs0 and (SURROGATE.search(norm(coment).replace("_", " ")) or col in surrogates):
        return OTHERS, "id interno (DDL)", f"llave interna según DDL ('{coment}')"
    if not sugt:
        texto = re.sub(r"\bidentificador\b", "codigo", norm(coment))
        t2, r2, o2 = clasificar(texto, cat)
        if t2 and t2 != OTHERS:
            return t2, f"comentario DDL:{r2}", "; ".join(x for x in (o2, f"por comentario DDL '{coment}': validar") if x)
    return sugt, regla, obs0

def defaults_repo(artifacts):
    """Valor más usado de ontology y owner_project en los JSON del repo (sin vacíos, literales ni personas)."""
    cnt = defaultdict(Counter)
    for j in (Path(artifacts) / "co/data_source").rglob("*.json"):
        try:
            lab = json.loads(j.read_text(encoding="utf-8")).get("labels") or {}
        except Exception:
            continue
        for k in ("ontology", "owner_project"):
            v = str(lab.get(k) or "").strip()
            if v and v != k and "@" not in v and (LABEL.fullmatch(v) or re.fullmatch(r"<[A-Z][A-Z0-9_]*>", v)):
                cnt[k][v] += 1
    return {k: c.most_common(1)[0][0] for k, c in cnt.items() if c}


def sugerir_estrategia(ddl_t, llave_unica):
    """Estrategia de carga sugerida desde el DDL de la fuente (solo sugiere: la decisión es manual, en `estrategia`).
    Llave = PK del DDL o llave_unica llena; marca de cambio = columna DATE/TIMESTAMP/DATETIME cuyo nombre sugiere
    fecha de modificación (la primera en el orden del DDL). Sin DDL: vacío."""
    if not ddl_t:
        return ""
    pk = ddl_t.get("pk") or ([x for x in llave_unica.split("|") if x.strip()] if filled(llave_unica) else [])
    if not pk:
        return "snapshot (sin llave primaria: CDC no aplica)"
    cand = next((c for c in ddl_t["orden"]
                 if ddl_tipo(ddl_t["cols"][c]) in ("DATE", "DATETIME") and MODIFICACION.search(c)), "")
    return (f"incremental:{cand} (llave + fecha de modificación)" if cand
            else "cdc_hash (llave sin fecha de modificación)")


# ----------------------------------------------------------------------------- CSV de trabajo: tablas, columnas y estado interno
ESTADO_JSON = ".estado.json"      # out/<sis>/in/.estado.json: estado interno del script por tabla (nadie lo edita)
ESTADOS_GOB = ("sugerido", "pendiente", "en revisión", "aprobado")   # escritura exacta de estado_gobierno
GOB_MANUAL = ("responsable", "ticket", "fecha_resolucion", "observaciones")   # los llena el equipo / Gobierno: se conservan


def estado_gob(v):
    """estado_gobierno con variantes de mayúsculas, tildes o guion bajo -> sugerido | pendiente | en revisión | aprobado.
    Valor vacío o no reconocido -> ''."""
    return {"sugerido": "sugerido", "pendiente": "pendiente", "en_revision": "en revisión", "revision": "en revisión",
            "aprobado": "aprobado", "aprobada": "aprobado"}.get(norm(v), "")


def armar_sugerencias(r):
    """Texto de solo lectura con lo que el script sugirió o encontró de la tabla (el detalle vive en .estado.json)."""
    partes = [f"{t}: {r[k]}" for t, k in (("estrategia sugerida", "estrategia_sugerida"), ("llaves sugeridas", "llaves_sugeridas"),
                                          ("valores sugeridos", "valores_sugeridos"), ("DL 1.0", "dl1_detalle"),
                                          ("DDL", "cobertura_ddl"), ("extractor", "extractor_fuente")) if r.get(k)]
    ev = [f"{t}={r[k]}" for t, k in (("dl2", "evidencia_dl2"), ("conexión dl2", "conexion_dl2_actual"), ("dl1", "evidencia_v1"),
                                     ("dl1 fuera de ODS", "origen_v1_fuera_ods"), ("lee_ods", "dl2_lee_ods"),
                                     ("dl2_gen", "dl2_gen")) if r.get(k)]
    return " || ".join(partes + ([f"evidencia: {'; '.join(ev)}"] if ev else []))


def leer_tablas(tpath):
    """Filas en memoria de <sis>_tablas.csv: las columnas del CSV + el estado interno de .estado.json (por tabla).
    Lee también el formato anterior (47 columnas, con el estado interno en el propio CSV) sin perder lo manual.
    En memoria `tabla` es el nombre corto y `sistema` viene del estado interno; en el CSV `tabla` es <sistema>_<tabla>."""
    tpath = Path(tpath)
    maestra = tpath.stem.removesuffix("_tablas")
    jp = tpath.with_name(ESTADO_JSON)
    try:
        side = json.loads(jp.read_text(encoding="utf-8")) if jp.exists() else {}
    except (json.JSONDecodeError, OSError):
        side = {}
    filas, avisos = [], []
    for r in (read_csv(tpath) if tpath.exists() else []):
        if "sistema" in r or "caso" in r:                    # formato anterior
            if "@" in (r.get("email_on_failure") or ""):
                avisos.append(f"{r.get('tabla_origen')}: correo literal en email_on_failure ignorado (se usa la llave {EMAIL_KEY})")
            if filled(r.get("team_to_report")) and r["team_to_report"] != TEAM_REPORT:
                avisos.append(f"{r.get('tabla_origen')}: team_to_report '{r['team_to_report']}' ignorado (ahora es configuración)")
        else:
            key = r.get("tabla", "")
            sc = side.get(key) if isinstance(side.get(key), dict) else {}
            r.update({k: str(sc.get(k) or "") for k in ESTADO_CAMPOS})
            r["sistema"] = r["sistema"] or (maestra if key else "")
            r["tabla"] = key.removeprefix(f"{r['sistema']}_") if r["sistema"] else key
        for campo in CAMPOS_LLAVE:                           # llaves de conexión con el código anterior -> nombre DL 2.0
            if r.get(campo):
                r[campo] = renombrar(r[campo])
        filas.append(r)
    for x in avisos:
        print(f"  aviso (migración tablas.csv): {x}")
    return filas


def escribir_tablas(tpath, filas):
    """<sis>_tablas.csv (columnas de TABLAS; `tabla` = <sistema>_<tabla>, llave con columnas.csv; `sugerencias` de solo
    lectura) y .estado.json (ESTADO_CAMPOS por tabla)."""
    tpath, out, side = Path(tpath), [], {}
    for r in filas:
        key = f"{r['sistema']}_{r['tabla']}" if r.get("sistema") and r.get("tabla") else ""
        out.append({**{k: r.get(k, "") for k, _, _ in TABLAS}, "tabla": key, "sugerencias": armar_sugerencias(r)})
        if key:
            side[key] = {k: str(r.get(k) or "") for k in ESTADO_CAMPOS}
    write_csv(tpath, out, [k for k, _, _ in TABLAS])
    tpath.with_name(ESTADO_JSON).write_text(json.dumps(side, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def importar_escalamiento(prev_cols, esc_rows, cat):
    """Migración (una vez) de <sis>_pt_por_confirmar.csv a <sis>_columnas.csv: tag_definido válido -> policy_tag con estado
    aprobado; estado en revisión -> estado_gobierno; responsable, ticket, fecha_resolucion y observaciones se completan
    donde columnas.csv no tiene valor. -> cantidad de filas importadas."""
    n = 0
    for e in esc_rows:
        t, c = e.get("tabla", ""), (e.get("columna") or "").upper()
        if not t or not c:
            continue
        old = prev_cols[t].setdefault(c, {"tabla": t, "columna": e.get("columna", ""), "incluir": "sí", "origen": "",
                                           "descripcion": e.get("descripcion", ""), "tipo_std": e.get("tipo_std", ""),
                                           "policy_tag": e.get("tag_actual", ""), "tag_sugerido": e.get("tag_sugerido", ""),
                                           "motivo": e.get("motivo", "")})
        definido = normalizar_tag(e.get("tag_definido"), cat)[0] if e.get("tag_definido") else ""
        if definido and definido in cat["tags"]:
            old["policy_tag"], old["estado_gobierno"] = e["tag_definido"].strip(), "aprobado"   # el scan lo normaliza y deja la nota
        else:
            if definido:
                old["_definido_invalido"] = definido
            if estado_gob(e.get("estado")) == "en revisión" and estado_gob(old.get("estado_gobierno")) != "aprobado":
                old["estado_gobierno"] = "en revisión"
        for k in GOB_MANUAL:
            if e.get(k) and not old.get(k):
                old[k] = e[k]
        n += 1
    return n


def con_gobierno(c):
    """¿La fila de columnas.csv lleva decisiones de Gobierno que no se deben perder?"""
    return any(c.get(k) for k in GOB_MANUAL) or estado_gob(c.get("estado_gobierno")) in ("aprobado", "en revisión")


# ----------------------------------------------------------------------------- scan
def cmd_scan(a):
    repos(a)
    tpath = a.tablas
    cpath = tpath.with_name(tpath.name.replace("_tablas", "_columnas") if "_tablas" in tpath.name
                            else tpath.stem + "_columnas.csv")
    epath = tpath.with_name(tpath.name.replace("_tablas", "_pt_por_confirmar") if "_tablas" in tpath.name
                            else tpath.stem + "_pt_por_confirmar.csv")           # solo para migrar (ya no se escribe)
    ppath = tpath.with_name(".pendientes_script.json")   # columnas que el script dejó en pendiente (no las del usuario)
    try:
        pend_script = set(json.loads(ppath.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        pend_script = None                                   # sin registro: se asume que todo pendiente es del script
    pend_nuevo = set()
    envs, idx, infos, conv, cat = contexto(a)
    tags_env = {k for v in envs.values() for k in v if k.startswith(("PT_", "POLICY_TAG"))}
    v1 = index_v1(a.ingdfw)
    prev_cols = defaultdict(dict)
    if cpath.exists():
        for c in read_csv(cpath):
            if "tag_sugerido" not in c:                                   # formato anterior: tag_catalogo
                c["tag_sugerido"] = c.get("tag_catalogo", "")
            prev_cols[c.get("tabla", "")][c.get("columna", "").upper()] = c
    migrados = importar_escalamiento(prev_cols, read_csv(epath), cat) if epath.exists() else 0
    maestra = tpath.stem.replace("_tablas", "")
    ddl, drepo = load_ddl(tpath.with_name(f"{maestra}_ddl.csv")), defaults_repo(a.artifacts)
    rows, cols_out, est, tablas_cols = [], [], Counter(), []
    for r in leer_tablas(tpath):
        if not r.get("tabla_origen"):
            continue
        row = {k: (r.get(k) or d) for k, _, d in FILA}
        nombre, m, base, hit, sistema, tabla = resolver(row, envs, idx, infos, conv)
        hits1 = v1.get(nombre.upper(), [])
        origen_v1 = [h for k in {base.upper(), re.sub(r"^[A-Z0-9]+_", "", base.upper())}
                     for h in v1.get(k, []) if h["carpeta"].lower() != "ods"]
        lee_ods = bool(hit and hit["lee_ods"])
        src1 = (hits1 or origen_v1 or [{}])[0]
        caso = ("B" if lee_ods else "A") if hit else ("C" if hits1 or origen_v1 else "D")
        auto = {
            "caso": caso, "sistema": sistema, "tabla": tabla,
            "raw_table": tabla,                                            # v6.8.1: raw_table = tabla
            "dbt_model": "|".join([f"{nombre_sis(sistema)}_{tabla}_hist"] + [renombrar(m_) for m_ in (hit or {}).get("dbt_models") or []
                                   if not re.search(r"_(hist|current)$", m_)]),   # v6.8.1: _hist (+ _current en CDC) + otros existentes (_aggr…)
            # v6.8.2: tabla_fuente = tabla de origen tal cual (la que lee el FROM); tecnología = la de esa tabla en DL 1.0
            # (stg_sql_* puede vivir en el ODS Oracle); el prefijo stg_ solo se usa si DL 1.0 no la tiene
            "tecnologia": hits1[0]["tecnologia"] if hits1 else (TECH.get(m.group(1).lower()) if m else src1.get("tecnologia", "")),
            "tabla_fuente": re.sub(rf"^{re.escape(sistema)}_", "", nombre, flags=re.I).upper(),
            "scheduling": (hit or {}).get("scheduling") or "35 06 * * *",
            "evidencia_dl2": (hit or {}).get("yaml", "") + (            # v6.8.1: la ingesta existente usa otros nombres
                f" (nombres existentes distintos: carpeta '{hit['tabla']}', raw '{hit['raw_table']}': renombrar o retirar)"
                if hit and (hit["tabla"].lower() != tabla or str(hit["raw_table"]).lower() != tabla) else ""),
            "conexion_dl2_actual": " | ".join(key_of(hit["refs"].get(p, "")) for p in ("source.address", "extractor.host", "@owner")
                                              if hit and hit["refs"].get(p)),
            "dl2_lee_ods": "sí" if lee_ods else "no", "dl2_gen": "sí" if hit and hit["marca"] else "no",
            "evidencia_v1": " | ".join(h["path"] for h in hits1[:3]),
            "origen_v1_fuera_ods": " | ".join(h["path"] for h in origen_v1[:3]),
        }
        for k, quien, _ in FILA:
            if quien == "scan" and k in auto and (
                    k not in ("tabla_fuente", "scheduling") or not filled(r.get(k))):
                row[k] = auto[k]
        row["extractor"], row["extractor_fuente"] = elegir_extractor(
            getattr(a, "extractores", {}).get(row["tabla_origen"].upper(), ""), hit)
        # llaves de conexión: si no se llenaron, se sugieren por existente / hermano / convención
        refs, srcs = refs_base(sistema, f"{sistema}_{tabla}", caso, hit, infos, conv)
        sug = []
        for campo, paths in (CAMPOS_LLAVE.items() if caso != "D" else ()):
            p = next((p for p in paths if refs.get(p)), None)
            if not filled(r.get(campo)) and p and key_of(refs[p]):
                row[campo] = key_of(refs[p])
                sug.append(f"{campo}={srcs[p]}")
        row["llaves_sugeridas"] = ", ".join(sug) if sug else (r.get("llaves_sugeridas") or "")
        sch = json_schema(a.artifacts, hit["json"]) if hit and hit["json"] else []
        nombres = [c["name"].upper() for c in sch] or [c.upper() for c in src1.get("cols", [])]
        ddl_t = ddl.get(row["tabla_fuente"].upper()) or ddl.get(base.upper()) or {}
        origen = {c: ("dl2" if sch else "dl1") for c in nombres}
        if ddl_t:                                   # todas las columnas de la fuente, en su orden; las nuevas marcadas
            nuevas = [c for c in ddl_t["orden"] if c not in origen]
            origen.update({c: NUEVA for c in nuevas})
            nombres = [c for c in ddl_t["orden"]] + [c for c in nombres if c not in ddl_t["cols"]]
        vs = []
        if not filled(r.get("llave_unica")) and ddl_t.get("pk"):
            row["llave_unica"] = "|".join(ddl_t["pk"])
            vs.append("llave_unica=DDL")
        row["estrategia_sugerida"] = sugerir_estrategia(ddl_t, row["llave_unica"])   # nunca toca `estrategia` (manual)
        for k, lk in (("ontology", "ontology"), ("owner", "owner_project")):
            if not filled(r.get(k)) and drepo.get(lk):
                row[k] = drepo[lk]
                vs.append(f"{k}=repo")
        if not filled(r.get("active_since")):       # se fija en la primera generación y se conserva (.estado.json)
            row["active_since"] = date.today().strftime("%Y%m%d")
            vs.append("active_since=fecha de generación")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", row.get("active_since") or ""):
            row["active_since"] = row["active_since"].replace("-", "")            # label: AAAAMMDD
        row["valores_sugeridos"] = ", ".join(vs) if vs else (r.get("valores_sugeridos") or "")
        if ddl_t:
            prev_k = prev_cols[f"{sistema}_{tabla}"]
            calc = lambda c: bool((prev_k.get(c) or {}).get("expresion") or src1.get("exprs", {}).get(c))
            sobran = [c for c in nombres if c not in ddl_t["cols"] and not calc(c)]
            calculadas = [c for c in nombres if c not in ddl_t["cols"] and calc(c)]
            row["cobertura_ddl"] = " · ".join(x for x in (
                f"fuente {len(ddl_t['orden'])} cols, DL 1.0 extraía {len(nombres) - len(nuevas)}",
                f"nuevas (marcadas {NUEVA}): {', '.join(nuevas)}" if nuevas else "",
                f"calculadas en DL 1.0 (expresión): {', '.join(calculadas)}" if calculadas else "",
                f"no existen en la fuente: {', '.join(sobran)}" if sobran else "") if x)
        else:
            row["cobertura_ddl"] = "sin DDL" if ddl else ""
        rows.append(row)
        tablas_cols.append((row, sistema, f"{sistema}_{tabla}", src1, sch, ddl_t, origen, nombres))

    # alias de tabla (APS_, APE_, SFI_…): prefijo de 2-4 letras en >= 50 % de las columnas de alguna tabla;
    # se quita en toda la maestra antes de clasificar
    alias = set()
    for *_, nombres in tablas_cols:
        cnt = Counter(n.split("_")[0] for n in nombres if "_" in n and 2 <= len(n.split("_")[0]) <= 4)
        alias |= {k for k, v in cnt.items() if v >= max(2, len(nombres) * 0.5)}
    sin_alias = lambda c: c.split("_", 1)[1] if "_" in c and c.split("_")[0] in alias else c
    surrogates = {sin_alias(c) for *_, ddl_t, _o, nombres in tablas_cols for c in nombres
                  if SURROGATE.search(norm((ddl_t.get("cols", {}).get(c) or {}).get("comments", "")).replace("_", " "))}
    for row, sistema, key, src1, sch, ddl_t, origen, nombres in tablas_cols:
        dcols = ddl_t.get("cols", {})
        repetidos = Counter((d.get("comments") or "").strip().upper() for d in dcols.values() if (d.get("comments") or "").strip())
        tipos = dict(zip([c.upper() for c in src1.get("cols", [])], src1.get("tipos", [])))
        cifr = {c.upper() for c in src1.get("cifradas", [])}
        prevj = {c["name"].upper(): c for c in sch}
        exprs = src1.get("exprs", {})
        for i, c in enumerate(nombres, 1):
            old = prev_cols[key].get(c, {})
            sin_pref = c.split("_", 1)[1] if "_" in c and c.split("_")[0] in alias else c
            sugt, regla, obs0 = clasificar(sin_pref, cat)
            dd = dcols.get(c, {})
            coment = (dd.get("comments") or "").strip()
            if coment:
                sugt, regla, obs0 = con_ddl(sin_pref, coment, sugt, regla, obs0, cat, surrogates)
            control = bool(CONTROL.search(old.get("expresion") or exprs.get(c, "") or "")) and not dd
            if control:                                  # fecha de carga calculada en la extracción: sin dato del cliente
                sugt, regla, obs0 = OTHERS, "columna de control", ""
            # estado_gobierno previo: aprobado y en revisión los fija Gobierno (no se recalculan ni se bajan)
            crudo = (old.get("estado_gobierno") or "").strip()
            est_prev = estado_gob(crudo) or ("en revisión" if crudo else "")   # no reconocido: se conserva y bloquea
            aprob = est_prev == "aprobado"
            pt_old, sug_prev = (old.get("policy_tag") or "").strip(), (old.get("tag_sugerido") or "").strip()
            manual = bool(pt_old) and pt_old.upper() != PEND and pt_old != sug_prev and not aprob   # tag editado a mano
            actual = pt_old if aprob else (old.get("policy_tag") or str(prevj.get(c, {}).get("policy_tag", "")))
            if not aprob and (actual.strip().upper() == PEND or (sug_prev and actual == sug_prev)):
                # sugerencia previa del script (igual al catálogo): se recalcula; un tag cambiado a mano se conserva
                actual = str(prevj.get(c, {}).get("policy_tag", ""))  # sugerencia previa del script: se recalcula
            obs = [obs0] if obs0 and obs0 != "sin coincidencia en catálogo" else []
            tag, nota = normalizar_tag(actual, cat) if actual else ("", "")
            if nota:
                obs.append(nota)
            fijo = aprob and tag in cat["tags"]          # aprobado con tag válido: no se recalcula
            if fijo:
                obs = [o for o in obs if "escalar" not in o]
                est["resuelto"] += 1
            elif aprob:                                  # aprobado sin tag válido: no se toca, queda bloqueado
                tag = tag or PEND
                obs.append("aprobado sin policy tag válido en el catálogo")
            elif not tag:
                tag = sugt if sugt and "escalar" not in obs0 and "ambiguo" not in obs0 else PEND
                est["catálogo" if sugt else "sin tag"] += 1
            elif a.adoptar and sugt and "ambiguo" not in obs0 and "verificar" not in obs0 and \
                    RANGO.get(sugt[3:5], 3) < RANGO.get(tag[3:5], 3):
                obs.append(f"adoptado del catálogo (antes {tag})")
                tag = sugt
                est["adoptado"] += 1
            if not sugt:
                obs.append("sin coincidencia en catálogo")
            if old.get("_definido_invalido"):
                obs.append(f"tag_definido {old['_definido_invalido']} no existe en catálogo")
            if tag not in cat["tags"] and tag != PEND:
                obs.append(f"{tag} no existe en catálogo")
            if sugt and sugt != tag:
                obs.append(f"catálogo sugiere {sugt}")
            if tags_env and tag != PEND and tag not in tags_env:
                obs.append("sin llave en variables.yaml")
            incluir = (old.get("incluir") or "sí").strip().lower()
            if origen.get(c) == NUEVA and incluir != "no" and not aprob:
                obs.append("columna nueva (DL 1.0 no la extraía): confirmar inclusión y policy tag: escalar")
                if not manual:
                    tag = PEND                                   # el catálogo queda como sugerencia
            if c in cifr and tag == OTHERS:
                obs.append(f"cifrada en V1 con {OTHERS}: escalar (DL 2.0 protege solo con policy tag)")
            if regla.startswith("exclusión"):
                est["exclusión"] += 1
            d_prev = (old.get("descripcion") or "").strip()
            if coment and repetidos[coment.upper()] > 1 and not (filled(d_prev) and d_prev != ddl_desc(coment)):
                # (con descripción propia ya escrita y distinta al comentario del DDL no hay nada que verificar)
                obs.append(f"comentario DDL repetido en {repetidos[coment.upper()]} columnas: verificar descripción")
            if ddl_t and not dd and not (old.get("expresion") or exprs.get(c)):
                obs.append("no existe en el DDL de la fuente")   # columna calculada (expresión) no aplica
            heur, t_ddl, t_old = std_type(c, tipos.get(c)), (ddl_tipo(dd) if dd else ""), old.get("tipo_std")
            tipo = t_old if t_old and t_old not in (heur, t_ddl) else (t_ddl or t_old or heur)   # manual > DDL > heurística
            if control and not (t_old and t_old not in (heur, t_ddl, "DATETIME")):
                tipo = "DATETIME"                        # SYSDATE trae fecha y hora: no truncar a DATE
            d_old = old.get("descripcion")
            desc = d_old if filled(d_old) else ((prevj.get(c, {}).get("description") or "").strip()
                                                or (ddl_desc(coment) if coment else "")
                                                or ("Fecha y hora de carga en la extracción (control)" if control else "")
                                                or PEND)
            revisar = "; ".join(dict.fromkeys(obs))
            # estado_gobierno: pendiente donde antes se escalaba; aprobado / en revisión no se bajan; un pendiente con
            # responsable, ticket u observaciones (trabajo humano) se conserva
            escala = (tag == PEND or "ambiguo" in revisar or "no existe" in revisar or "escalar" in revisar) \
                and not aprob and incluir != "no"
            tocado = any(old.get(k) for k in ("responsable", "ticket", "observaciones"))
            del_usuario = est_prev == "pendiente" and pend_script is not None and f"{key}|{c}" not in pend_script
            if est_prev in ("aprobado", "en revisión") or (est_prev == "pendiente" and (tocado or del_usuario)):
                eg = est_prev
            else:
                eg = "pendiente" if escala else "sugerido"
                if escala:
                    pend_nuevo.add(f"{key}|{c}")
            # notas de una sola vez que se conservan mientras la fila no cambie (el scan corre dos veces por corrida)
            previas = [x.strip() for x in (old.get("motivo") or "").split(";")]
            nota_no_rec = f"estado_gobierno '{crudo}' no reconocido: usar {' | '.join(ESTADOS_GOB)}" if crudo and not estado_gob(crudo) \
                else next((x for x in previas if "no reconocido" in x and x.startswith("estado_gobierno '")), "") if eg == "en revisión" else ""
            notas_ret = [x for x in previas if (m_ := re.fullmatch(r"\S+ retirado -> (\S+)", x)) and m_.group(1) == tag and x not in revisar]
            motivo = "; ".join(x for x in (
                revisar, *notas_ret, f"regla: {regla}" if revisar and regla else "", "cifrada en DL 1.0" if c in cifr else "",
                nota_no_rec) if x)
            cols_out.append({
                "tabla": key, "orden": i, "columna": c, "origen": origen.get(c, "dl1"),
                "incluir": "no" if incluir == "no" else "sí", "expresion": old.get("expresion") or exprs.get(c, ""),
                "tipo_std": tipo, "descripcion": desc, "policy_tag": tag, "tag_sugerido": sugt, "estado_gobierno": eg,
                "motivo": motivo, **{k: old.get(k, "") for k in GOB_MANUAL}})
    # filas de tablas que ya no están en la lista pero llevan decisiones de Gobierno: se conservan
    procesadas = {k for _, _, k, *_ in tablas_cols}
    for t, d in prev_cols.items():
        if t in procesadas:
            continue
        for old in d.values():
            if con_gobierno(old):
                cols_out.append({k: old.get(k, "") for k in COLUMNAS} | {"estado_gobierno": estado_gob(old.get("estado_gobierno")) or "sugerido"})
    escribir_tablas(tpath, rows)
    write_csv(cpath, cols_out, COLUMNAS)
    ppath.write_text(json.dumps(sorted(pend_nuevo | ((pend_script or set()) & {f"{x['tabla']}|{x['columna']}" for x in cols_out
                                                                                if x.get("estado_gobierno") == "pendiente"})), indent=0),
                     encoding="utf-8")
    if migrados:                       # pt_por_confirmar ya no se escribe: queda importado y se renombra
        dst = epath.with_name(epath.stem + ".migrado.csv")
        n = 1
        while dst.exists():
            n += 1
            dst = epath.with_name(f"{epath.stem}.migrado{n}.csv")
        epath.rename(dst)
        print(f"  migración: {migrados} filas de {epath.name} importadas a {cpath.name} (archivo renombrado a {dst.name})")
    print(f"{len(rows)} tablas · casos {dict(Counter(r['caso'] for r in rows))} · "
          f"{len(cols_out)} columnas · tags nuevos: {est['catálogo']} por catálogo, {est['exclusión']} por exclusión, "
          f"{est['sin tag']} sin tag, {est['resuelto']} aprobados por Gobierno, {est['adoptado']} adoptados · "
          f"distintos al catálogo: {sum(1 for c in cols_out if c['tag_sugerido'] and c['tag_sugerido'] != c['policy_tag'])} · "
          f"pendientes de Gobierno: {sum(1 for c in cols_out if c['estado_gobierno'] == 'pendiente')} ({cpath.name})")
    for r in rows:
        nc = [c for c in cols_out if c["tabla"] == f"{r['sistema']}_{r['tabla']}"]
        pend = sum(1 for c in nc if c["descripcion"] == PEND)
        rev = sum(1 for c in nc if c["motivo"])
        falt = [k for k, q, _ in TABLAS if q == "manual" and r.get(k) == PEND]
        print(f"  [{r['caso']}] {r['tabla_origen']} -> {r['sistema']}/{r['tabla']} · {len(nc)} cols "
              f"({pend} sin descripción, {rev} a revisar) · lee_ods={r['dl2_lee_ods']} · dl2_gen={r['dl2_gen']} · "
              f"llaves sugeridas: {len([x for x in r['llaves_sugeridas'].split(',') if x.strip()])} · por llenar: {len(falt)}")


# ----------------------------------------------------------------------------- plantillas
def strip_comment(line):
    q = None
    for i, ch in enumerate(line):
        if ch in "'\"" and (q is None or q == ch):
            q = None if q else ch
        elif ch == "#" and q is None and (i == 0 or line[i - 1] in " \t"):
            return line[:i].rstrip()
    return line.rstrip()

def yaml_from_template(path, variant):
    """Activa la variante elegida de ingesta.yaml (descomenta sus líneas) y elimina la otra."""
    out, active, bind, started = [], None, 0, False
    uncomment = lambda r: strip_comment(re.sub(r"^(\s*)#\s?", r"\1", r, count=1))
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        s = raw.lstrip()
        ind = len(raw) - len(s)
        if not s:
            active = None
            continue
        if not s.startswith("#"):
            started, active = True, None
            inline = next((v for v in VARIANTS if f"[{v}]" in s), None)   # valor  # [JDBC]
            if inline in (None, variant):
                out.append(strip_comment(raw))
            continue
        if not started:
            continue
        tag = next((v for v in VARIANTS if f"[{v}]" in s), None)
        body = s[1:].strip()
        if body.startswith("---"):
            active, bind = tag, ind
            continue
        if tag:
            active, bind = tag, ind
            line = re.sub(r"\[(JDBC|CIFRADO)\]\s*", "", uncomment(raw))
            if tag == variant and re.match(r"\s*(-\s+)?[\w.\-]+\s*:", line):   # el marcador puede traer llave
                out.append(line)
            continue
        if active and ind >= bind and not body.startswith("#") and active == variant:
            out.append(uncomment(raw))
    return load_refyaml("\n".join(l for l in out if l.strip()))


# ----------------------------------------------------------------------------- render
def marca_txt(t, prefijo):
    return f"{prefijo} {MARCA}: {GEN} | {t['ticket']} | {date.today().isoformat()} | {renombrar(t['tabla_origen'])}\n"

BU, IND = CLI["bu"], CLI["ind"]     # labels bu (unidad de negocio) e ind; aplican a modelo .sql y JSON

LABELS_TABLA = {"domain", "subdomain", "source_system", "update_type", "update_frequency", "available_time", "pii",
                "permanence", "load_strategy", MARCA}


def labels_repo(models):
    """Expresión más usada por label en los modelos del repo. Si es dinámica (var(), target., env_var()) el label
    cambia por ambiente y el generador la usa en lugar del literal. Se calcula una vez por corrida."""
    if "labels" in TAXO:
        return TAXO["labels"]
    cnt = defaultdict(Counter)
    for f in (Path(models) / f"{DBT}/models").rglob("*.sql"):
        txt = f.read_text(encoding="utf-8", errors="replace")
        if MARCA in txt[:4000]:
            continue                                     # solo modelos escritos por el equipo
        m = re.search(r"labels\s*=\s*\{(.*?)\}\s*\)", txt, re.S)
        for k, v in re.findall(r"['\"]([\w-]+)['\"]\s*:\s*([^,\n]+?)\s*(?:,|\n|$)", m.group(1) if m else ""):
            cnt[k][v.strip()] += 1
    dyn = {}
    for k, c in cnt.items():
        if k in LABELS_TABLA:
            continue
        tot = sum(c.values())
        expr, n = next(((e, n) for e, n in c.most_common() if DINAMICO.search(e)), ("", 0))
        if expr and n >= max(2, tot * 0.3):              # patrón dinámico con uso real en el repo
            dyn[k] = expr
    TAXO["labels"] = dyn
    return dyn


LABELS_FIJOS = {"env", "bu", "ind", "country", "layer"}     # decididos: no se toman de variables.yaml


def labels_dinamicos(a, envs):
    """Labels que cambian por ambiente: patrón dinámico de los modelos del repo; si no hay, y la llave existe en
    artifacts co/variables.yaml (p. ej. scope -> SCOPE), se referencia con var('SCOPE'). Si la var no existe en
    dbt_project.yml, vars_dbt_faltantes la toma de variables.yaml y la agrega marcada."""
    if "labels_all" in TAXO:
        return TAXO["labels_all"]
    dyn = dict(labels_repo(a.models))
    for k in ("scope", "comp-id", "workload-type", "engine", "team_to_report_dev"):
        v = k.upper().replace("-", "_")
        if k not in dyn and any(v in b for b in envs.values()):
            dyn[k] = f"var('{v}')"
    TAXO["labels_all"] = dyn
    return dyn


def _sql_partes(t, cols, pii, dyn=None, est="snapshot"):
    """(líneas SELECT con casts, bloque de labels) comunes a todos los modelos del std. est = estrategia efectiva
    que se renderiza (snapshot | incremental | cdc_hash): label load_strategy; update_type del modelo es
    incremental en incremental y cdc_hash (el modelo agrega por fecha), y el de la tabla en snapshot."""
    cast = {"DATE": "SAFE_CAST(LEFT({c}, 10) AS DATE)", "NUMERIC": "SAFE_CAST({c} AS NUMERIC)",
            "INT64": "SAFE_CAST({c} AS INT64)", "DATETIME": "SAFE_CAST({c} AS DATETIME)",
            "BIGNUMERIC": "SAFE_CAST({c} AS BIGNUMERIC)", "TIMESTAMP": "SAFE_CAST({c} AS TIMESTAMP)"}   # v6.8.3
    sel = ",\n".join(f"    {cast.get(c['tipo_std'], '{c}').format(c=c['columna'])} AS {c['columna'].lower()}" for c in cols)
    labels = {"env": None, "country": "co", "layer": "std", "bu": BU, "ind": IND, "comp-id": CLI["comp_id"],
              "scope": "datalake", "workload-type": "ingest", "engine": "batch", "domain": t.get("dominio_label") or t["dominio"],
              "subdomain": t["subdominio"], "source_system": nombre_sis(t["sistema"]), "update_type": "incremental" if est in ("incremental", "cdc_hash") else "completa",
              "load_strategy": est, "update_frequency": t["periodicidad"], "available_time": t["available_time"], "pii": pii,
              "permanence": t["permanence"], "team_to_report_dev": FIJOS["team_to_report_dev"], MARCA: GEN}
    for k, expr in (dyn or {}).items():                 # labels por ambiente según el repo (scope, env…)
        if k in labels and k not in LABELS_TABLA:
            labels[k] = ("expr", expr)
    fmt = lambda v: "target.name" if v is None else (v[1] if isinstance(v, tuple) else f"'{label_safe(v)}'")
    return sel, ",\n      ".join(f"'{k}': " + fmt(v) for k, v in labels.items())


def model_sql(t, cols, pii, dyn=None):
    sel, lab = _sql_partes(t, cols, pii, dyn)
    m = t["dbt_model"].split("|")[0]
    return (marca_txt(t, "--") +
        '{% set partitions_to_replace = [var("information_date")] %}\n\n'
        "{{\n  config(\n    materialized = 'incremental',\n    incremental_strategy = 'insert_overwrite',\n"
        "    partition_by = {'field': 'information_date', 'data_type': 'date'},\n"
        "    partitions = partitions_to_replace,\n"
        f"    labels = {{\n      {lab}\n    }}\n  )\n}}}}\n\n"
        f"WITH {m} AS (\n  SELECT\n{sel},\n"
        '    DATE({{ var("information_date") }}) AS information_date,\n'
        '    DATE({{ var("extraction_date") }}) AS extraction_date,\n'
        "    CURRENT_DATETIME('America/Bogota') AS execution_dttm,\n"
        '    {{ var("source") }} AS source\n'
        f"  FROM {{{{ source('raw_{nombre_sis(t['sistema'])}', '{t['raw_table']}') }}}}\n)\n\n"
        f"SELECT * FROM {m}\n{{% if is_incremental() %}}\n"
        "WHERE information_date IN ({{ partitions_to_replace | join(',') }})\n{% endif %}\n")


# Bitácora de cambios (CDC): el modelo _hist guarda solo las filas nuevas, modificadas (y borradas en cdc_hash) de cada
# fecha, con row_hash y op (I/U/D); el modelo _current resuelve el estado vigente. Las marcas @@X@@ se reemplazan.
HIST_CDC = """@@MARCA@@{% set partitions_to_replace = [var("information_date")] %}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'information_date', 'data_type': 'date'},
    partitions = partitions_to_replace,
    cluster_by = [@@CLUSTER@@],
    on_schema_change = 'fail',
    labels = {
      @@LAB@@
    }
  )
}}

WITH fuente AS (
  SELECT
@@SEL@@
  FROM {{ source('raw_@@SIS@@', '@@RAW@@') }}
@@QUALIFY@@),
fuente_h AS (
  SELECT f.*, TO_HEX(SHA256(TO_JSON_STRING(STRUCT(@@HASH@@)))) AS row_hash
  FROM fuente f
),
{% if is_incremental() %}
estado AS (   -- última versión vigente por llave antes de la fecha que se procesa
  SELECT * EXCEPT (rn) FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY @@PK@@ ORDER BY information_date DESC, execution_dttm DESC) AS rn
    FROM {{ this }} t
    WHERE information_date < DATE({{ var("information_date") }})
  )
  WHERE rn = 1 AND op != 'D'
),
{% else %}
estado AS (
  SELECT *, 'I' AS op FROM fuente_h WHERE FALSE
),
{% endif %}
cambios AS (
  SELECT f.*, IF(e.row_hash IS NULL, 'I', 'U') AS op
  FROM fuente_h f
  LEFT JOIN estado e USING (@@PK@@)
  WHERE e.row_hash IS NULL OR e.row_hash != f.row_hash
@@BORRADOS@@)@@CONTROL@@
SELECT
  c.*,
  DATE({{ var("information_date") }}) AS information_date,
  DATE({{ var("extraction_date") }}) AS extraction_date,
  CURRENT_DATETIME('America/Bogota') AS execution_dttm,
  {{ var("source") }} AS source
FROM cambios c
@@GUARDA@@"""

BORRADOS_CDC = """  UNION ALL
  SELECT @@COLS@@, e.row_hash, 'D' AS op
  FROM estado e
  LEFT JOIN fuente_h f USING (@@PK@@)
  WHERE f.row_hash IS NULL
"""

CONTROL_CDC = """,
control AS (
  SELECT (SELECT COUNT(*) FROM fuente_h) AS n_fuente,
         (SELECT COUNT(*) FROM estado) AS n_estado,
         (SELECT COUNT(*) FROM cambios WHERE op = 'D') AS n_borrados
)
"""

GUARDA_CDC = """CROSS JOIN control k
WHERE IF(k.n_estado > 0 AND (k.n_fuente = 0 OR k.n_borrados > k.n_estado * @@UMBRAL@@),
         ERROR('cdc: extracción vacía o borrados sobre el umbral; se aborta para no marcar filas como borradas'), TRUE)
"""


def model_sql_cdc(t, cols, pii, dyn, ef):
    """<sis>_<tabla>_hist para incremental / cdc_hash (BigQuery). Hash sobre las columnas de negocio sin las de control
    (SYSDATE…) ni, en incremental, la columna_control: así una fila solo cambia si cambió su contenido."""
    sel, lab = _sql_partes(t, cols, pii, dyn, ef["est"])
    pk, cdc = ", ".join(ef["pk"]), ef["est"] == "cdc_hash"
    qualify = (f"  WHERE TRUE\n  QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY {ef['control'].lower()} DESC) = 1\n"
               if ef["est"] == "incremental" else "")
    borrados = BORRADOS_CDC.replace("@@COLS@@", ", ".join(f"e.{c['columna'].lower()}" for c in cols)) if cdc else ""
    rep = {"@@MARCA@@": marca_txt(t, "--"), "@@CLUSTER@@": ", ".join(f"'{c}'" for c in ef["pk"][:4]), "@@LAB@@": lab,
           "@@SEL@@": sel, "@@SIS@@": nombre_sis(t["sistema"]), "@@RAW@@": t["raw_table"], "@@QUALIFY@@": qualify,
           "@@HASH@@": ", ".join(ef["hashcols"]), "@@BORRADOS@@": borrados,
           "@@CONTROL@@": CONTROL_CDC if cdc else "\n",
           "@@GUARDA@@": GUARDA_CDC.replace("@@UMBRAL@@", str(UMBRAL_BORRADO)) if cdc else ""}
    txt = HIST_CDC
    for k in ("@@BORRADOS@@", "@@GUARDA@@"):          # primero los bloques que traen marcas propias
        txt = txt.replace(k, rep.pop(k))
    txt = txt.replace("@@PK@@", pk)
    for k, v in rep.items():
        txt = txt.replace(k, v)
    return txt


def model_current_sql(t, cols, pii, dyn, ef):
    """<sis>_<tabla>_current: última versión vigente por llave desde la bitácora (sin las filas borradas)."""
    _, lab = _sql_partes(t, cols, pii, dyn, ef["est"])
    pk = ", ".join(ef["pk"])
    return (marca_txt(t, "--") +
        "{{\n  config(\n    materialized = 'table',\n"
        f"    cluster_by = [{', '.join(repr(c) for c in ef['pk'][:4])}],\n"
        f"    labels = {{\n      {lab}\n    }}\n  )\n}}}}\n\n"
        "SELECT * EXCEPT (rn, op) FROM (\n"
        f"  SELECT h.*, ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY information_date DESC, execution_dttm DESC) AS rn\n"
        f"  FROM {{{{ ref('{ef['hist']}') }}}} h\n)\nWHERE rn = 1 AND op != 'D'\n")


def model_schema(t, cols, ef=None):
    pt = lambda k: ["{{ var('%s') }}" % k]
    cc = [{"name": c["columna"].lower(), "description": c["descripcion"], "policy_tags": pt(c["policy_tag"])} for c in cols]
    meta = [{"name": k, "description": v, "policy_tags": pt(OTHERS)} for k, v in (
        ("information_date", "Fecha de información"), ("extraction_date", "Fecha de extracción"),
        ("execution_dttm", "Fecha y hora de ejecución"), ("source", "Ruta del archivo origen en raw"))]
    llave = [x.strip().lower() for x in t["llave_unica"].split("|") if x.strip()] if filled(t["llave_unica"]) else [PEND]
    if ef and ef["est"] != "snapshot":
        rh = {"name": "row_hash", "policy_tags": pt(OTHERS),
              "description": "Hash SHA-256 de las columnas de negocio para detectar cambios"}
        op = {"name": "op", "policy_tags": pt(OTHERS),
              "description": "Tipo de cambio: I inserción, U actualización, D borrado en la fuente"}
        prueba = lambda cols_: [{"dbt_utils.unique_combination_of_columns": {"combination_of_columns": cols_}}]
        return marca_txt(t, "#") + dump({"version": 2, "models": [
            {"name": ef["hist"], "meta": {MARCA: GEN},
             "description": f"Bitácora de cambios ({ef['est']}) de {nombre_sis(t['sistema'])}.{t['tabla']} (origen {t['tabla_fuente']}): "
                            "una fila por cambio detectado (op = I, U o D) en cada information_date.",
             "data_tests": prueba(llave + ["information_date"]), "columns": cc + [rh, op] + meta},
            {"name": ef["current"], "meta": {MARCA: GEN},
             "description": f"Estado actual de {nombre_sis(t['sistema'])}.{t['tabla']} (última versión vigente por llave, "
                            f"desde la bitácora {ef['hist']}).",
             "data_tests": prueba(llave), "columns": cc + [rh] + meta}]})
    cc += meta
    # v6.8 (G3): sin llave_unica no hay prueba de unicidad (POR_CONFIRMAR como columna rompe dbt)
    pruebas = [{"dbt_utils.unique_combination_of_columns": {
        "combination_of_columns": llave,
        "config": {"where": 'information_date = DATE({{ var("information_date") }})'}}}] if filled(t["llave_unica"]) else []
    return marca_txt(t, "#") + dump({"version": 2, "models": [{
        "name": t["dbt_model"].split("|")[0],
        "description": f"Histórico diario de {nombre_sis(t['sistema'])}.{t['tabla']} (origen {t['tabla_fuente']}).",
        "meta": {MARCA: GEN},
        "data_tests": pruebas,
        "columns": cc}]})


def expr_ventana(tecnologia, n):
    """Expresión de fecha del filtro incremental (ventana de n días hacia atrás desde $FECHA_INFORMACION).
    -> (expresión, aviso o ''). Tecnología desconocida: forma Oracle y aviso."""
    f = {"sqlserver": f"DATEADD(day, -{n}, CAST('{FECHA_INFO}' AS DATE))",
         "db2": f"DATE('{FECHA_INFO}') - {n} DAYS"}.get(tecnologia)
    if f:
        return f, ""
    return (f"TO_DATE('{FECHA_INFO}', 'YYYY-MM-DD') - {n}",
            "" if tecnologia == "oracle" else f"tecnología '{tecnologia or '-'}' sin forma de fecha conocida: se usó la "
                                              "sintaxis Oracle; confirmar sintaxis de fecha")


def estrategia_efectiva(t, cols, maestra):
    """Estrategia de carga que se renderiza y sus pendientes. Vacía/POR_CONFIRMAR o con algún problema -> snapshot
    (el comportamiento de siempre) y la tabla queda BORRADOR. -> {est, pedida, pend, control, ventana, sugerida,
    pk, hashcols}."""
    nombres = {c["columna"].upper(): c for c in cols}
    sug, v = (t.get("estrategia_sugerida") or "").strip(), (t.get("estrategia") or "").strip()
    ef = {"est": "snapshot", "pedida": v, "pend": [], "control": "", "ventana": 3, "sugerida": sug, "pk": [], "hashcols": []}
    est = ESTRATEGIAS.get(v.lower())
    if not v or v == PEND:
        ef["pend"].append(f"confirmar estrategia en {maestra}_tablas.csv (sugerida: {sug or 'sin DDL'})")
        return ef
    if est is None:
        ef["pend"].append(f"estrategia inválida '{v}': snapshot | incremental | cdc_hash")
        return ef
    if est == "snapshot":
        return ef
    pk = [x.strip().lower() for x in t["llave_unica"].split("|") if x.strip()] if filled(t.get("llave_unica")) else []
    if not pk:
        ef["pend"].append(f"estrategia {est} requiere llave_unica")
        return ef
    fuera = [c for c in pk if c.upper() not in nombres]
    if fuera:
        ef["pend"].append(f"estrategia {est}: llave_unica con columnas que no están incluidas: {', '.join(fuera)}")
        return ef
    if est == "incremental":
        cc = (t.get("columna_control") or "").strip()
        if not cc:
            m = re.match(r"incremental:(\S+)", sug)
            cc = m.group(1) if m else ""
        if not cc:
            ef["pend"].append("incremental requiere columna_control")
            return ef
        if cc.upper() not in nombres:
            ef["pend"].append(f"columna_control '{cc}' no es una columna incluida de la tabla")
            return ef
        vd = (t.get("ventana_dias") or "3").strip()
        if not re.fullmatch(r"\d+", vd):
            ef["pend"].append(f"ventana_dias inválida '{vd}': entero >= 0")
            return ef
        ef.update(control=nombres[cc.upper()]["columna"], ventana=int(vd))
    ef["hashcols"] = [c["columna"].lower() for c in cols if not CONTROL.search(c.get("expresion") or "")
                      and not (est == "incremental" and c["columna"].upper() == ef["control"].upper())] or pk
    ef.update(est=est, pk=pk)
    return ef


def usa_fecha_informacion(artifacts):
    """¿Alguna ingesta existente en co/data_source usa $FECHA_INFORMACION? (una vez por corrida)"""
    if "fecha_info" not in TAXO:
        TAXO["fecha_info"] = any(FECHA_INFO in f.read_text(encoding="utf-8", errors="replace")
                                 for f in (Path(artifacts) / "co/data_source").rglob("*") if f.is_file())
    return TAXO["fecha_info"]

# v6.8 (G1): campos secretos de la variante cipher -> sufijo de la llave <SIS>_<sufijo> (siempre referencia {LLAVE}, nunca literal)
SECRETOS_CIPHER = {"secret_host": "SECRET_HOST", "hashicorp_secret_path": "HASHICORP_SECRET_PATH",
                   "external_secret": "HASHICORP_SECRET_USER", "hashicorp_secret_user": "HASHICORP_SECRET_USER"}
OCI_VIEJO = ((r"_OCI_SECRET_PATH\}", "_HASHICORP_SECRET_PATH}"), (r"_OCI_SECRET\}", "_HASHICORP_SECRET_USER}"))


def secretos_cipher(y, sis_up):
    """v6.8 (G1): fija los campos secretos del YAML cipher donde estén (dict o lista key/value); si la plantilla no
    trae alguno, lo agrega bajo extractor. Además reescribe cualquier referencia vieja <S>_OCI_SECRET[_PATH]."""
    visto = set()
    def marcar(k, ponerlo):
        n = k.rsplit(".", 1)[-1]
        if n in SECRETOS_CIPHER:
            visto.add(n)
            ponerlo("{%s_%s}" % (sis_up, SECRETOS_CIPHER[n]))
    def viejo(v):
        for rx, nuevo in OCI_VIEJO if isinstance(v, str) else ():
            if re.search(rx, v):
                v = type(v)(re.sub(rx, nuevo, v))      # conserva Lit / QStr
        return v
    def recorre(x):
        if isinstance(x, dict):
            for k in list(x):
                if isinstance(k, str) and k in SECRETOS_CIPHER and not isinstance(x[k], (dict, list)):
                    marcar(k, lambda v, x=x, k=k: x.__setitem__(k, v))
                elif isinstance(x[k], (dict, list)):
                    recorre(x[k])
                else:
                    x[k] = viejo(x[k])
        elif isinstance(x, list):
            for i in x:
                if isinstance(i, dict) and isinstance(i.get("key"), str) and i["key"].rsplit(".", 1)[-1] in SECRETOS_CIPHER:
                    marcar(i["key"], lambda v, i=i: i.__setitem__("value", v))
                else:
                    recorre(i)
    recorre(y)
    for n in SECRETOS_CIPHER:
        if n not in visto:
            marcar(n, lambda v, n=n: put(y, f"extractor.{n}", v))


def render_table(t, cols, a, out, ctx, rep, usos):
    envs, idx, infos, conv, cat = ctx
    sis, tab = nombre_sis(t["sistema"]), t["tabla"]            # sis = nombre DL 2.0 (todo lo emitido); t["sistema"] = código
    name = f"{sis}_{tab}"
    cipher = t["extractor"] == "cipher"
    tpl = Path(a.artifacts) / "ci/checks/templates"
    y = yaml_from_template(tpl / "ingesta.yaml", "CIFRADO" if cipher else "JDBC")
    _, _, _, hit, _, _ = resolver(t, envs, idx, infos, conv)
    refs, _ = refs_base(t["sistema"], name, t["caso"], hit, infos, conv)
    for p, v in refs.items():                      # referencias de ambiente: existente > hermano > convención
        if p != "@owner" and p not in CONN and exists_ref(y, p):
            put_ref(y, p, v)
    names = [c["columna"] for c in cols]
    sel = [f"{c['expresion']} AS {c['columna']}" if c.get("expresion") else c["columna"] for c in cols]
    query = Lit("SELECT\n    " + ",\n    ".join(sel) + f"\nFROM {ref(t['owner_db'])}.{t['tabla_fuente']}\n")
    ef = t.get("_ef") or estrategia_efectiva(t, cols, t["sistema"])
    extra, pend_t = t.setdefault("_extra", []), t.setdefault("_pend", [])
    m0 = t["dbt_model"].split("|")[0]
    cdc = ef["est"] != "snapshot"
    ef["hist"] = m0
    ef["current"] = re.sub(r"_hist$", "", m0) + "_current"
    if ef["est"] == "incremental":                  # solo la ventana de días hacia atrás desde la fecha de información
        cc = next(c for c in cols if c["columna"].upper() == ef["control"].upper())
        fecha, aviso = expr_ventana(t["tecnologia"], ef["ventana"])
        query = Lit(query + f"WHERE {cc.get('expresion') or cc['columna']} >= {fecha}\n")
        if aviso:
            extra.append(aviso)
        if not usa_fecha_informacion(a.artifacts):
            extra.append(f"ninguna ingesta DL 2.0 usa {FECHA_INFO}: validar en dev que el extractor lo reemplace")
    tipo_carga = "incremental" if ef["est"] == "incremental" else "completa"   # derivado de la estrategia efectiva
    pasos = [{"dbt_model": m} for m in t["dbt_model"].split("|")]
    if cdc:                                         # bitácora: _hist y _current, y los demás modelos existentes después
        pasos = [{"dbt_model": ef["hist"]}, {"dbt_model": ef["current"]}] + [
            x for x in pasos if x["dbt_model"] not in (ef["hist"], ef["current"])]
    S = {
        "source.system": sis, "source.periodicity": t["periodicidad"], "source.domain": t.get("dominio_label") or t["dominio"],
        "source.subdomain": t["subdominio"], "source.data_owner": t["data_owner"],
        "source.data_steward": t["data_steward"], "source.data_custodian": t["data_custodian"],
        "source.update_frequency": t["periodicidad"], "source.update_type": tipo_carga,
        "source.team_to_report": TEAM_REPORT, "source.available_schedule": t["available_time"],
        "source.retention": t["permanence"], "source.owner": t["owner"], "source.business_unit": BU,
        "gcp.composer.extraction.scheduling": t["scheduling"], "gcp.composer.email_on_failure": ref(EMAIL_KEY),
        "gcp.gcs.ln.regex": f".*{name}.*\\.{'avro' if cipher else 'csv'}$",
        "gcp.bigquery.raw.raw_table": t["raw_table"],
        "gcp.bigquery.trf_steps": pasos,
        "gcp.start_extraction_topic_id": ref(TOPICO[cipher][0]),
    }
    if TOPICO[cipher][1]:
        S["gcp.start_extraction_topic_project_id"] = ref(TOPICO[cipher][1])
    if cipher:
        pre = t["conexion"].strip("{}").removesuffix("_HOST")
        S.update({"extractor.type": "cipher", "extractor.host": ref(f"{pre}_HOST"), "extractor.port": ref(f"{pre}_PORT"),
                  "extractor.sid": ref(f"{pre}_SID"), "extractor.query_string": query,
                  "extractor.columns": ",".join(names), "file.type": "avro"})
    else:
        r = FIJOS["resources"]
        S.update({"source.address": ref(t["conexion"]), "source.csv": FIJOS["csv"],
                  "gcp.bigquery.raw.raw_skip_lines": "1",
                  "extractor.resources": {"requests": dict(r), "limits": dict(r)}})
        envv = {"source.query_string": query, "source.batch_size": FIJOS["batch_size"],
                "csv.encoding": FIJOS["encoding"], "process_file": False,
                "source.datasource.username": ref(t["usuario"]), "source.datasource.password": ref(t["password"])}
        if t["tecnologia"] == "oracle":
            envv["source.datasource.oracle_connection_type"] = "jdbc"
        for k, v in envv.items():
            put_ref(y, f"extractor.env#{k}", v)
    for k, v in S.items():
        put(y, k, v)
    if cipher:
        secretos_cipher(y, sis.upper())             # v6.8 (G1): secret_host / hashicorp_* / external_secret como {<S>_...}
    for k in ("project_name",):                      # sin uso ni valor definido en el repo: no se genera
        (y.get("source") or {}).pop(k, None)
    for p, v in FALLBACK.items():
        cur = get_ref(y, p)
        if (p.startswith("extractor.env") and cipher) or (cur and not PH.search(str(cur))):
            continue
        put_ref(y, p, v)

    js = json.loads((tpl / "esquema.json").read_text(encoding="utf-8"))
    pii = "true" if any(PII_TAG.match(c["policy_tag"]) for c in cols) else "false"
    js["labels"].update({
        "bu": BU, "ind": IND, "source_system": sis, "source_detail": sis, "domain": t.get("dominio_label") or t["dominio"], "subdomain": t["subdominio"],
        "ontology": t["ontology"], "update_frequency": t["periodicidad"], "update_type": tipo_carga,
        "available_time": t["available_time"], "permanence": t["permanence"], "pii": pii,
        "data_owner": t["data_owner"], "data_steward": t["data_steward"], "data_custodian": t["data_custodian"],
        "owner_project": t["owner"], "team_to_report_dev": FIJOS["team_to_report_dev"],
        "tech_focal_mail": EQUIPO, "active_since": t["active_since"], MARCA: GEN})
    js["labels"].pop("project_name", None)
    js["labels"] = {k: label_safe(v) for k, v in js["labels"].items()}   # BigQuery rechaza @, . y mayúsculas
    js["schema"] = [{"name": c["columna"], "type": "STRING", "description": c["descripcion"],
                     "policy_tag": f"<{c['policy_tag']}>"} for c in cols]
    t["_yaml"] = y                                   # dict renderizado: base de la prueba de conexión (v6.6)
    ytxt = marca_txt(t, "#") + dump(y)
    jtxt = json.dumps(js, ensure_ascii=False, indent=2) + "\n"
    art = out / "artifacts/co/data_source" / sis / tab
    files = [(art / f"{name}.yaml", ytxt), (art / f"{name}.json", jtxt)]

    existente = next(Path(a.models).glob(f"{DBT}/models/**/{m0}.sql"), None)
    if cdc and existente and "row_hash" not in existente.read_text(encoding="utf-8", errors="replace"):
        rel_m = existente.relative_to(a.models).as_posix()
        en_prd = subprocess.run(["git", "-C", str(a.models), "cat-file", "-e", f"origin/{CLI["env_prd"]}:{rel_m}"],
                                capture_output=True).returncode == 0
        if en_prd or not tiene_marca(existente):   # productivo o escrito a mano: la tabla tiene otra estructura
            pend_t.append("el modelo existente es snapshot" + (" y está en producción" if en_prd else "")
                          + ": pasar a CDC requiere aprobación TL y full refresh o modelo nuevo")
        else:                                      # generado y aún no productivo: se reemplaza por la bitácora
            rep.append(f"  - modelo existente {rel_m} (snapshot, no productivo): se reemplaza por la bitácora CDC; "
                       "si la tabla ya existe en BigQuery de dev, borrarla antes de la primera carga")
    if existente and not a.force and not tiene_marca(existente):
        rep.append(f"  - modelo existente sin marca: {existente.relative_to(a.models).as_posix()} "
                   "(se conserva; --force para normalizarlo)")
    else:
        md = (existente.parent.relative_to(Path(a.models)) if existente
              else Path(f"{DBT}/models") / f"std_{t['dominio']}" / name)
        dyn = labels_dinamicos(a, envs)
        files += [(out / "models" / md / f"{m0}.sql",
                   model_sql_cdc(t, cols, pii, dyn, ef) if cdc else model_sql(t, cols, pii, dyn)),
                  (out / "models" / md / "schema.yml", model_schema(t, cols, ef if cdc else None))]
        if cdc:
            files.append((out / "models" / md / f"{ef['current']}.sql", model_current_sql(t, cols, pii, dyn, ef)))
        if not existente:
            rep.append(f"  - agregar `{t['raw_table']}` a `raw_{sis}` en std_{t['dominio']}/__sources.yml si falta")
    if not getattr(a, "solo_textos", False):
        for p, txt in files:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(txt, encoding="utf-8", newline="\n")
            rep.append(f"  - {p.relative_to(out).as_posix()}")

    for mm in MARK.finditer(ytxt):
        usos[mm.group(1) or mm.group(2)].add("yaml")
    for mm in MARK.finditer(jtxt):
        usos[mm.group(1) or mm.group(2)].add("json")
    for c in cols:
        if filled(c["policy_tag"]):
            usos[c["policy_tag"]].add("policy_tag")
    usos.pop(PEND, None)
    falt = sorted({k for k in usos if any(k not in v for v in envs.values())} &
                  {mm.group(1) or mm.group(2) for mm in MARK.finditer(ytxt + jtxt)})
    if falt:
        rep.append(f"  - llaves a crear/completar en variables.yaml: {len(falt)} (ver variables_{a.maestra}.csv)")
    pend = [c["columna"] for c in cols if c["descripcion"] == PEND]
    rev = [c["columna"] for c in cols if c.get("motivo")]
    if pend:
        rep.append(f"  - columnas sin descripción: {len(pend)}")
    if rev:
        rep.append(f"  - columnas a revisar (policy tag): {len(rev)}")
    otros = sorted(set(PH.findall(ytxt)))
    if PEND in ytxt + jtxt or otros:
        rep.append(f"  - quedan valores {PEND}{' y de plantilla ' + ', '.join(otros) if otros else ''}")
    return files

# ----------------------------------------------------------------------------- audit
def cmd_audit(a):
    repos(a)
    res = Counter()
    for f in (Path(a.artifacts) / "co/data_source").rglob("*"):
        if f.suffix in (".yaml", ".json"):
            res[("artifacts", f.relative_to(Path(a.artifacts) / "co/data_source").parts[0], tiene_marca(f))] += 1
    for f in (Path(a.models) / f"{DBT}/models").rglob("*.sql"):
        res[("models", f.relative_to(Path(a.models) / f"{DBT}/models").parts[0], tiene_marca(f))] += 1
    print(f"{'repo':10} {'carpeta':28} {MARCA:>8} {'sin marca':>10}")
    for repo, carpeta in sorted({(r, c) for r, c, _ in res}):
        print(f"{repo:10} {carpeta:28} {res[(repo, carpeta, True)]:>8} {res[(repo, carpeta, False)]:>10}")


# ============================================================================= orquestación (un solo comando)
EXTRACTORES = {"cipher": "cipher", "cifrado": "cipher", "jdbc": "jdbc"}
REGLA_EXT = "regla TL: corp → cipher, resto → jdbc"


def elegir_extractor(valor_lista, hit):
    """(extractor, fuente). Manda la lista; si está vacía, la variante de la ingesta existente en DL 2.0; si no, jdbc.
    fuente queda en <sis>_tablas.csv y alimenta ESTADO.md (cambio de variante, valor inválido, valor por defecto)."""
    actual = (hit or {}).get("extractor", "")
    v = (valor_lista or "").strip().lower()
    if v and v not in EXTRACTORES:
        return actual or "jdbc", f"inválido '{v}' en la lista: usar cipher o jdbc"
    if v:
        ext = EXTRACTORES[v]
        return ext, (f"lista (cambia {actual}→{ext} en ingesta existente: requiere aprobación TL)"
                     if actual and actual != ext else "lista")
    if actual:
        return actual, "dl2-existente (definir en la lista)"
    return "jdbc", f"por defecto (definir en la lista; {REGLA_EXT})"


CONFIG = IN / "config.yaml"
CAMPOS_CONN = (("conexion", "JDBC_{S}", r"^JDBC_"), ("owner_db", "{S}_OWNER_DB", r"OWNER"),
               ("usuario", "USER_{S}_CREDENTIALS", r"USER"), ("password", "PASS_{S}_CREDENTIALS", r"PASS"))
TOKEN = lambda x: rf"(?<![\w.-]){re.escape(x)}(?![\w.-])"
RAMA_PRD_DL1 = CLI["rama_prd_legacy"]


def git_in(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                          encoding="utf-8", errors="replace").stdout


def cargar_config(a):
    """in/config.yaml: ramas de ingdfw (DL 1.0) por ambiente de DL 2.0. Se autodetecta la primera vez."""
    txt = CONFIG.read_text(encoding="utf-8") if CONFIG.exists() else ""
    refs = (load_yaml(CONFIG).get("dl1_refs") or {}) if txt else {}
    if "autodetect v2" not in txt:                      # config de versiones anteriores: se regenera
        remotas = [r.strip() for r in git_in(a.ingdfw, "branch", "-r").splitlines() if "->" not in r and r.strip()]
        locales = [r.strip("* ").strip() for r in git_in(a.ingdfw, "branch").splitlines() if r.strip()]
        prd = next((r for r in remotas + locales if re.search(r"(^|/)" + RAMA_PRD_DL1 + "$", r)), "")
        refs = {ENV_DEV: "", ENV_PRD: prd}
        CONFIG.write_text(
            "# gen_ingestas autodetect v2: ramas de ingdfw (DL 1.0) de donde se leen las conexiones.\n"
            f"# {ENV_PRD} = rama productiva de DL 1.0 ({RAMA_PRD_DL1}); 'git fetch origin' antes de correr.\n"
            f"# {ENV_DEV} vacío: DL 1.0 no tiene ambiente dev equivalente; las llaves salen con el mismo nombre y valor\n"
            f"# '# PENDIENTE valor {ENV_DEV}'. Si existe una rama válida para dev, escribirla aquí.\n"
            f"dl1_refs:\n  {ENV_DEV}: '{refs[ENV_DEV]}'\n  {ENV_PRD}: '{refs[ENV_PRD]}'\n", encoding="utf-8")
        print(f"  config: {ENV_PRD}='{prd or '-'}' {ENV_DEV}='-' (in/{CONFIG.name})")
    if refs.get(ENV_PRD) and not re.search(r"(^|/)" + RAMA_PRD_DL1 + "$", refs[ENV_PRD]):
        print(f"  AVISO: {ENV_PRD} de in/{CONFIG.name} no es {RAMA_PRD_DL1}: los valores podrían no ser los productivos")
    return refs


def conexiones_dl1(a, refs, nombres):
    """{ambiente: {tabla: conexión}} leyendo cookiecutter.json de cada rama sin checkout."""
    out = {}
    for env, ref in refs.items():
        if not ref:
            continue
        rutas = [p for p in git_in(a.ingdfw, "ls-tree", "-r", "--name-only", ref).splitlines()
                 if p.endswith("/cookiecutter.json")]
        cand = [p for p in rutas if p.rsplit("/", 2)[-2].lower() in nombres]
        faltan = nombres - {p.rsplit("/", 2)[-2].lower() for p in cand}
        for n in faltan:                                     # carpeta con otro nombre: buscar por table_name
            cand += [l.split(":", 1)[1] for l in git_in(a.ingdfw, "grep", "-il", f".{n}\"", ref, "--",
                                                        "*cookiecutter.json").splitlines() if ":" in l]
        res = {}
        for p in dict.fromkeys(cand):
            try:
                d = json.loads(git_in(a.ingdfw, "show", f"{ref}:{p}") or "{}")
            except json.JSONDecodeError:
                continue
            owner, _, tab = str(d.get("table_name", "")).partition(".")
            if tab.lower() not in nombres:
                continue
            tec, blk = next(((k, v) for k, v in d.items() if isinstance(v, dict) and ("sid" in v or "cols" in v)), ("", {}))
            res[tab.lower()] = {
                "tec": tec, "ruta": p.rsplit("/", 1)[0], "host": str(d.get("host") or ""), "port": str(d.get("port") or ""),
                "sid": str(blk.get("sid") or ""), "owner": owner.upper(),
                "usuario": str(blk.get("db_creds_user_secret_name") or blk.get("user") or ""),
                "password": str(blk.get("db_creds_password_secret_name") or blk.get("password_secret_name") or ""),
                "incremental": str(d.get("is_incremental_load", "")), "schedule": str(d.get("schedule", "")),
                "retencion": str(d.get("bq_max_storage_years") or ""), "table_name": str(d.get("table_name") or "")}
        out[env] = res
    return out


def formato_jdbc(envs, tec):
    """Formato de URL JDBC más usado en variables.yaml para la tecnología."""
    vals = [str(v) for b in envs.values() for k, v in b.items() if k.startswith("JDBC_")]
    if tec == "oracle":
        o = [v for v in vals if "oracle" in v.lower()]
        return ("jdbc:oracle:thin:@//{host}:{port}/{sid}" if sum("@//" in v for v in o) >= len(o) / 2
                else "jdbc:oracle:thin:@{host}:{port}:{sid}")
    if tec == "sqlserver":
        return "jdbc:sqlserver://{host}:{port};databaseName={sid}"
    return ""


def valor_dl1(campo, c, fmt):
    """campo: conexion (URL JDBC) | host | port | sid (cipher) | owner_db | usuario | password."""
    if campo == "conexion":
        return fmt.format(host=c["host"], port=c["port"], sid=c["sid"]) if fmt and c["host"] and c["sid"] else ""
    return c["owner"] if campo == "owner_db" else c[campo]


def coincide_dl1(campo, c, v, cipher=False, bloque=None, llave=""):
    """¿El valor v (de variables.yaml) apunta a la misma base de DL 1.0? En cipher, una llave <P>_HOST solo se reutiliza
    si <P>_PORT y <P>_SID del mismo bloque también coinciden (varias bases pueden compartir host)."""
    v = str(v)
    if campo == "conexion" and cipher:
        pre = llave.removesuffix("_HOST")
        return all(coincide_dl1(x, c, (bloque or {}).get(f"{pre}_{x.upper()}", "")) for x in ("host", "port", "sid"))
    if campo == "conexion":
        return bool(c["host"]) and all(re.search(TOKEN(x), v, re.I) for x in (c["host"], c["port"], c["sid"]) if x)
    val = c["owner"] if campo == "owner_db" else c[campo]
    return bool(val) and val.lower() == v.strip().lower()


def campos_conn(cipher):
    """Llaves de conexión por variante. cipher: <S>_HOST (de él se derivan _PORT y _SID) y owner; los secretos de la
    variante cifrada (v6.8: <S>_SECRET_HOST, <S>_HASHICORP_SECRET_PATH, <S>_HASHICORP_SECRET_USER) los fija
    secretos_cipher en render_table y entran por `usos`, no por usuario y password."""
    if cipher:
        return (("conexion", "{S}_HOST", r"_HOST$"), CAMPOS_CONN[1])
    return CAMPOS_CONN


def rutas(sis):
    base = OUT / sis
    r = {"base": base, "in": base / "in", "out": base / "out"}
    r.update({k: r["in"] / f"{sis}_{k}.csv" for k in ("tablas", "columnas", "ddl")})
    r.update({"estado": r["out"] / "ESTADO.md", "traza": r["out"] / "trazabilidad_dl1.csv"})
    return r


def paso_conexiones(a, maestra, envs):
    """Resuelve conexión origen por tabla: reutiliza llaves de DL 2.0 que ya apuntan a la misma base, o propone
    llaves nuevas con el valor de DL 1.0 por ambiente. Actualiza el CSV de tablas (sin pisar lo manual)."""
    tpath = rutas(maestra)["tablas"]
    rows = [{k: (r.get(k) or d) for k, _, d in FILA} for r in leer_tablas(tpath)]
    nombre = lambda r: r["tabla_origen"].rpartition(".")[2].lower()            # v6.8.2: la tabla tal cual (stg_sql_x)
    alterno = lambda r: (STG.match(nombre(r)) or [None, None, nombre(r)])[2].lower()   # respaldo: sin prefijo stg_
    refs = cargar_config(a)
    conn = conexiones_dl1(a, refs, {x for r in rows if r.get("caso") != "D" for x in (nombre(r), alterno(r))})
    propuestas, estado, traza = defaultdict(dict), {}, []
    for r in rows:
        n, sis = nombre(r), r["sistema"]
        if r.get("caso") == "D" or not sis:
            continue
        auto = {x.split("=")[0].strip() for x in r.get("llaves_sugeridas", "").split(",") if "=" in x}
        notas, fuentes, info = [], [], []
        por_env = {env: conn.get(env, {}).get(n) or conn.get(env, {}).get(alterno(r)) for env in envs}
        ref_c = next((c for c in (por_env.get(ENV_PRD), *por_env.values()) if c), None)
        det = []
        if ref_c:
            r["tecnologia"] = r["tecnologia"] or ref_c["tec"]
            if ref_c["schedule"]:
                det.append(f"schedule DL1 '{ref_c['schedule']}' (Composer usa UTC: confirmar zona)")
                if r.get("caso") == "C" and r["scheduling"] in ("", "35 06 * * *"):
                    r["scheduling"] = ref_c["schedule"]
            if ref_c["retencion"].isdigit():
                det.append(f"retención DL1 {ref_c['retencion']} años")
                if r["permanence"] in ("", "3y"):
                    r["permanence"] = f"{ref_c['retencion']}y"
            det.append(f"incremental DL1 {ref_c['incremental'] or '-'}")
            if ref_c["incremental"].lower() == "true" and not filled(r.get("estrategia")):
                det.append("DL 1.0 cargaba incremental")      # sugerencia: update_type sale de `estrategia`, no se cambia solo
        r["dl1_detalle"] = " · ".join(det) if det else "no encontrada en DL 1.0"
        cipher = r["extractor"] == "cipher"
        for campo, patron, rx in campos_conn(cipher):
            ok = lambda k: bool(re.search(rx, k or ""))       # la llave debe corresponder a la variante
            manual = filled(r[campo]) and campo not in auto and ok(r[campo])
            if filled(r[campo]) and not ok(r[campo]):         # llave de la otra variante: se reemplaza y se informa
                info.append(f"{campo} {r[campo]} reemplazada: no corresponde a la variante {r['extractor']}")
            existente = next((k for env in sorted(envs, reverse=True) for k, v in envs[env].items()
                              if ok(k) and por_env.get(env) and coincide_dl1(campo, por_env[env], v, cipher, envs[env], k)), "")
            nombre_llave = r[campo] if manual else (existente or (r[campo] if filled(r[campo]) and ok(r[campo])
                                                                  else patron.format(S=nombre_sis(sis).upper())))
            if not manual:
                r[campo] = nombre_llave
                fuentes.append(f"{campo}={'dl2-existente' if existente else ('dl1-nueva' if ref_c else 'convención')}")
            llaves = [(nombre_llave, campo)]
            if cipher and campo == "conexion":                # cipher: <P>_HOST + <P>_PORT + <P>_SID
                pre = nombre_llave.removesuffix("_HOST")
                llaves = [(f"{pre}_HOST", "host"), (f"{pre}_PORT", "port"), (f"{pre}_SID", "sid")]
            for (nombre_llave, sub), (env, bloque) in ((x, e) for x in llaves for e in envs.items()):
                c = por_env.get(env)
                val = valor_dl1(sub, c, formato_jdbc(envs, c["tec"])) if c else ""
                if nombre_llave in bloque:
                    if c and not coincide_dl1(sub, c, bloque[nombre_llave]):
                        notas.append(f"{nombre_llave} existe en {env} con otro valor: revisar")
                    continue
                coment = (f"PENDIENTE valor {env}" if not val else
                          "secreto de DL 1.0 (se reutiliza)" if sub in ("usuario", "password")
                          else f"desde DL 1.0 ({refs.get(env) or '-'})")
                prev = propuestas[env].get(nombre_llave)
                if prev and prev[0] and val and prev[0] != val:
                    notas.append(f"{nombre_llave}: valores distintos entre tablas en {env}")
                if not prev or (val and not prev[0]):
                    propuestas[env][nombre_llave] = (val, coment)
        r["llaves_sugeridas"] = ", ".join(fuentes) if fuentes else r.get("llaves_sugeridas", "")
        for env, c in por_env.items():
            if c:
                traza.append({"maestra": maestra, "tabla": n, "ambiente_dl2": env, "rama_dl1": refs.get(env, ""),
                              "carpeta_dl1": c["ruta"], "tecnologia": c["tec"], "host": c["host"], "puerto": c["port"],
                              "sid_servicio": c["sid"], "owner": c["owner"], "secreto_usuario": c["usuario"],
                              "secreto_clave": c["password"], "incremental": c["incremental"], "schedule_dl1": c["schedule"],
                              "bq_max_storage_years": c["retencion"], "table_name_dl1": c["table_name"],
                              "llave_jdbc_dl2": r["conexion"], "llave_owner_dl2": r["owner_db"],
                              "llave_usuario_dl2": r["usuario"], "llave_password_dl2": r["password"]})
        estado[f"{sis}_{r['tabla']}"] = {"dl1": ", ".join(e for e, c in por_env.items() if c) or "no encontrada",
                                         "notas": list(dict.fromkeys(notas)), "info": info}
    escribir_tablas(tpath, rows)
    if traza:
        rutas(maestra)["out"].mkdir(parents=True, exist_ok=True)
        write_csv(rutas(maestra)["traza"], traza, list(traza[0]))
    return propuestas, estado


def paso_render(a, maestra, propuestas, estado_conn):
    R = rutas(maestra)
    ctx = contexto(a)
    envs = ctx[0]
    mvars = model_vars(a.models)
    a.maestra, a.solo_textos = maestra, True
    a.ren, nom_sis = info_renombre(), nombre_sis(maestra)           # nom_sis: nombre DL 2.0 de lo emitido
    tablas = leer_tablas(R["tablas"])
    por_tabla = defaultdict(list)
    for c in read_csv(R["columnas"]):
        por_tabla[c["tabla"]].append(c)
    out_root = R["out"]
    editados, previo_vars, previo_dbt = leer_ediciones(out_root)   # antes de borrar out/
    gen_prev = llaves_generadas(out_root)               # v6.8 (G2): llaves que propuso el generador en la corrida anterior
    for sub in ("artifacts", "models", "listo", "borrador", "prueba_conexion"):
        shutil.rmtree(out_root / sub, ignore_errors=True)
    manifiesto, conservados = {}, []
    cipher_sis = False                                # v6.8: ¿alguna tabla del sistema es cipher?
    usos_tot, resumen, fuentes = defaultdict(set), [], defaultdict(list)
    candidatas_prueba = []                            # [(tabla, dict YAML)] de las tablas con YAML, en el orden de la lista (v6.7)
    for t in tablas:
        t = {k: (t.get(k) or d) for k, _, d in FILA}
        t["update_type"] = "completa"                                   # derivado de la estrategia efectiva (render_table)
        t["dominio"], t["subdominio"] = operacional(t["dominio"]), operacional(t["subdominio"])
        if t["available_time"].isdigit() and len(t["available_time"]) < 4:
            t["available_time"] = t["available_time"].zfill(4)          # HHMM: Excel quita el cero inicial
        pend_dom = resolver_dominio(t, a.models)                 # antes de render: fija carpeta y labels
        key = f"{t['sistema']}_{t['tabla']}"
        if t["caso"] == "D":
            resumen.append({"tabla": key, "nom": renombrar(key), "estado": "SIN FUENTE", "pend": ["no existe en DL 1.0 ni DL 2.0: escalar"],
                            "archivos": []})
            continue
        cols = [c for c in sorted(por_tabla.get(key, []), key=lambda x: int(x.get("orden") or 0))
                if (c.get("incluir") or "sí").lower() != "no"]
        rep_t, usos_t = [], defaultdict(set)
        t["_ef"], t["_extra"], t["_pend"] = estrategia_efectiva(t, cols, maestra), [], []
        ev = t.get("evidencia_dl2") or ""
        if "nombres existentes distintos" in ev:                       # v6.8.1: aviso, no bloquea
            t["_extra"].append("AVISO ingesta DL 2.0 existente " + ev)
        cipher_sis = cipher_sis or t["extractor"] == "cipher"
        files = render_table(t, cols, a, out_root, ctx, rep_t, usos_t) if cols else []
        if cols:
            candidatas_prueba.append((t, t["_yaml"]))
        files, cons = aplicar_ediciones(files, out_root, editados, manifiesto)
        conservados += cons
        pend = []
        if not cols:
            pend.append("sin columnas")
        no_aplica = {"usuario", "password"} if t["extractor"] == "cipher" else set()
        if t["_ef"]["est"] == "snapshot" and not t["_ef"]["pend"] and not filled(t.get("llave_unica")):
            no_aplica = no_aplica | {"llave_unica"}     # v6.8 (G3): snapshot confirmado sin llave_unica: no es requerida
            t["_extra"].append("snapshot sin llave_unica: schema.yml sin prueba de unicidad")
        campos = [k for k, q, _ in TABLAS if q in ("manual", "excel") and k not in no_aplica | NUEVAS_CARGA
                  and not filled(t.get(k))]
        if campos:
            pend.append(f"{maestra}_tablas.csv: completar {', '.join(campos)}")
        pend += t["_ef"]["pend"] + t["_pend"]          # estrategia de carga: validación propia (no es genérica)
        pend += pend_dom
        fx = t.get("extractor_fuente") or ""
        if fx.startswith("inválido"):
            pend.append(f"extractor {fx}")
        cob = t.get("cobertura_ddl") or ""
        sin_ddl = [c["columna"] for c in cols if "no existe en el DDL" in (c.get("motivo") or "")]
        if sin_ddl:                                   # las excluidas (incluir = no) y las calculadas no bloquean
            pend.append(f"columnas que no existen en la fuente: {', '.join(sin_ddl)} (excluir con incluir = no "
                        f"en {maestra}_columnas.csv o corregir el nombre)")
        valido = lambda c: c.get("policy_tag") in ctx[4]["tags"]       # no vacío, no POR_CONFIRMAR y en el catálogo
        nuevas = sum(1 for c in cols if c.get("origen") == NUEVA and not valido(c))
        npt = sum(1 for c in cols if not valido(c))
        if npt:
            pend.append(f"policy tag sin definir o fuera del catálogo en {npt} columnas de {maestra}_columnas.csv (Gobierno)"
                        + (f", {nuevas} son columnas nuevas ({NUEVA})" if nuevas else ""))
        bloquea = ("pendiente", "en revisión") if not APROBACION_TOTAL else ("pendiente", "en revisión", "sugerido")
        ngob = sum(1 for c in cols if (estado_gob(c.get("estado_gobierno")) or "pendiente") in bloquea)
        if ngob:
            pend.append(f"{ngob} columnas pendientes de Gobierno en {maestra}_columnas.csv (filtrar estado_gobierno = "
                        + ("distinto de aprobado)" if APROBACION_TOTAL else "pendiente / en revisión)"))
        ndesc = sum(1 for c in cols if not filled(c.get("descripcion")))
        if ndesc:
            pend.append(f"descripción de {ndesc} columnas en {maestra}_columnas.csv")
        pend += estado_conn.get(key, {}).get("notas", [])
        ph = sorted({f"{m} en {k} ({fp.suffix[1:]})" for fp, txt in files
                     for k, m in re.findall(r"^\s*-?\s*['\"]?([\w.\-]+)['\"]?\s*:[^\n]*?(<[a-z][^<>{}\n]*>)", txt, re.M)})
        if ph:
            pend.append(f"valores de plantilla sin resolver: {', '.join(ph)} (no hay tabla hermana ni convención en el repo)")
        if any(PEND in txt for _, txt in files) and not pend:
            pend.append("quedan valores POR_CONFIRMAR en los archivos")
        nuevo_modelo = bool(cols)          # v6.8.3: toda tabla con columnas declara su raw_table en __sources.yml (solo si falta)
        if nuevo_modelo:
            fuentes[(t["dominio"], nombre_sis(t["sistema"]))].append(t["raw_table"])
        for fp, txt in files:
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(txt, encoding="utf-8", newline="\n")
        for k, v in usos_t.items():
            usos_tot[k] |= v
        resumen.append({"tabla": key, "nom": renombrar(key), "estado": "BORRADOR" if pend else "LISTO", "pend": pend,
                        "dl1": estado_conn.get(key, {}).get("dl1", "-"),
                        "archivos": [fp.relative_to(out_root).as_posix() for fp, _ in files],
                        "usos": sorted(set(usos_t) - {PEND}),
                        "fuente": (t["dominio"], nombre_sis(t["sistema"]), t["raw_table"]) if nuevo_modelo else None,
                        "carga": (t["_ef"], t.get("estrategia_sugerida", ""), t.get("ventana_dias") or "3"),
                        "extra": [f"extractor: {t['extractor']} · {fx}"] + estado_conn.get(key, {}).get("info", [])
                                 + t["_extra"]
                                 + [x.strip(" -") for x in rep_t if "modelo existente" in x]
                                 + ([f"info: {cob}"] if NUEVA in cob else [])})
    usos_tot.pop(PEND, None)
    sis_up = nom_sis.upper()
    # v6.8 (G2): valores manuales de out/.../variables.yaml solo de llaves que usa este sistema (o que el generador propuso)
    usadas = set(usos_tot) | {m.group(1) or m.group(2) for txt in editados.values() for m in MARK.finditer(txt)}
    ignoradas = set()
    for env, bloque in previo_vars.items():
        for k, v in bloque.items():
            if v in (None, "") or envs.get(env, {}).get(k) or propuestas[env].get(k, ("", ""))[0]:
                continue
            if k in usadas or k in gen_prev:
                propuestas[env][k] = (str(v), "valor manual (out/variables.yaml)")
            else:
                ignoradas.add(k)
    if cipher_sis:                                    # v6.8 (G8): usuario y clave cipher van vacíos en el grupo del sistema
        for env, bloque in envs.items():
            for k in (f"USER_{sis_up}_CREDENTIALS", f"PASS_{sis_up}_CREDENTIALS"):
                if k not in bloque and k not in propuestas[env]:
                    propuestas[env][k] = ("", f"PENDIENTE valor {env}")
    for env, bloque in envs.items():                              # llaves usadas que faltan (policy tags, etc.)
        for k in sorted(usos_tot):
            if k in bloque or k in propuestas[env]:
                continue
            rec = next((v for suf, v in RECURSOS.items() if k == f"{sis_up}_{suf}"), None)
            if k == EMAIL_KEY:
                propuestas[env][k] = ("", f"PENDIENTE valor {env}: correo del canal o grupo de alertas (dato sensible)")
            elif rec and env in AMBIENTE:
                propuestas[env][k] = (rec.format(s=nom_sis.replace("_", "-"), sis=nom_sis, amb=AMBIENTE[env]),
                                      "sugerido por convención DL 2.0: crear en Terraform")
            else:
                propuestas[env][k] = ("", f"PENDIENTE valor {env}" + (" (policy tag)" if "policy_tag" in usos_tot[k] else ""))
    notas_src = escribir_fuentes(a, out_root, fuentes, a.ren)
    for f in (out_root / "models").rglob("__sources.yml"):   # __sources.yml editado a mano: se conserva
        orig = Path(a.models) / f.relative_to(out_root / "models")
        eol = leer_texto(orig, a.models)[1] if orig.exists() else "\n"           # fin de línea del archivo del repo
        res, cons = aplicar_ediciones([(f, leer_texto(f)[0])], out_root, editados, manifiesto)
        escribir_texto(f, res[0][1], eol)
        conservados += cons
    for r in resumen:                     # sin __sources.yml el modelo no compila: la tabla no puede quedar LISTO
        if r.get("fuente") and any(f"std_{r['fuente'][0]}/" in n and "a mano" in n for n in notas_src):
            r["pend"].append(f"declarar raw_{r['fuente'][1]} en std_{r['fuente'][0]}/__sources.yml")
            r["estado"] = "BORRADOR"
    if ignoradas:
        notas_src.append("AVISO: valores manuales de out/…/variables.yaml ignorados (llaves que este sistema no usa; no se "
                         f"re-proponen): {', '.join(sorted(ignoradas))}")
    notas_src += escribir_variables(a, out_root, propuestas, nom_sis, list(envs))
    pt_dbt = sorted(k for k, v in usos_tot.items() if "policy_tag" in v and mvars and k not in mvars)
    dbt_nuevas = vars_dbt_faltantes(a, out_root, nom_sis, mvars, envs)
    for k, d in dbt_nuevas.items():                   # valores escritos a mano en out/models/dbt_project.yml
        for e in ("dev", "prd"):
            if not d[e] and previo_dbt.get(e, {}).get(k):
                d[e], d["origen"] = str(previo_dbt[e][k]), "valor manual (out/dbt_project.yml)"
    notas_src += escribir_dbt_vars(a, out_root, dbt_nuevas, nom_sis, a.ren)
    con_valor = lambda k, e: envs.get(e, {}).get(k) not in (None, "") or bool(propuestas[e].get(k, ("", ""))[0])
    dbt_vacias = {k for k, d in dbt_nuevas.items() if not (d["dev"] and d["prd"])}
    for r in resumen:                                 # llaves y vars sin valor en los archivos finales de out/
        if r["estado"] == "SIN FUENTE":
            continue
        sin = [k for k in r.get("usos", []) if any(not con_valor(k, e) for e in envs)]
        if sin:
            r["pend"].append(f"{len(sin)} llaves sin valor en co/variables.yaml de out/: {', '.join(sin)} "
                             "(completar en out/…/variables.yaml; ver sección Llaves)")
        refs = {m for rel in r["archivos"] if rel.startswith("models/")
                for m in re.findall(r"var\(\s*['\"]([\w-]+)['\"]", (out_root / rel).read_text(encoding="utf-8"))}
        if refs & dbt_vacias:
            r["pend"].append(f"vars dbt sin valor: {', '.join(sorted(refs & dbt_vacias))} (completar en "
                             "out/models/dbt_project.yml)")
        r["estado"] = "BORRADOR" if r["pend"] else "LISTO"
    est_t = {r["tabla"]: r["estado"] for r in resumen}                 # estado final de cada tabla -> tablas.csv
    filas = leer_tablas(R["tablas"])
    for f in filas:
        f["estado"] = est_t.get(f"{f['sistema']}_{f['tabla']}", f.get("estado", ""))
    escribir_tablas(R["tablas"], filas)
    a.prueba = escribir_prueba(a, maestra, out_root, candidatas_prueba, resumen, editados, manifiesto)   # con el estado final
    if a.prueba.get("editado"):
        conservados.append(a.prueba["ruta"])
    escribir_copia(a, maestra, out_root, resumen, a.prueba)
    escribir_retiro(a, maestra, out_root, resumen)
    (out_root / ".generado.json").write_text(json.dumps(manifiesto, indent=0, sort_keys=True), encoding="utf-8")
    escribir_estado(maestra, R, resumen, propuestas, envs, notas_src, pt_dbt, dbt_nuevas, sorted(set(conservados)), a.ren, a.prueba)
    return resumen


sha = lambda txt: hashlib.sha256(txt.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def leer_ediciones(out_root):
    """Antes de regenerar out/: archivos generados que el usuario editó (hash distinto al del manifiesto), y valores
    escritos a mano en la copia de variables.yaml y dbt_project.yml. Así una corrida nueva no borra cambios."""
    man = out_root / ".generado.json"
    try:
        previo = json.loads(man.read_text(encoding="utf-8")) if man.exists() else {}
    except json.JSONDecodeError:
        previo = {}
    editados = {}
    for rel, h in previo.items():
        f = out_root / rel
        if f.exists():
            txt = f.read_text(encoding="utf-8")
            if sha(txt) != h:                       # corrida anterior con el nombre viejo: ruta y contenido pasan al nombre DL 2.0
                editados[renombrar(rel)] = renombrar(txt) if renombrar(rel) != rel else txt
    var = out_root / "artifacts/co/variables.yaml"
    previo_vars = envs_dl2(None, var) if var.exists() else {}
    dbt = out_root / "models/dbt_project.yml"
    previo_dbt = {}
    if dbt.exists():
        v = load_yaml(dbt).get("vars") or {}
        amb = v.pop("ambientes", {}) or {}
        previo_dbt = {e: {**{renombrar(k): x for k, x in v.items() if x},
                          **{renombrar(k): x for k, x in (amb.get(e) or {}).items() if x}} for e in ("dev", "prd")}
    return editados, previo_vars, previo_dbt


# v6.8 (G2): comentarios con los que el generador marca las llaves que propone en variables.yaml
MARCA_PROPUESTA = re.compile(r"#\s*(PENDIENTE valor|desde DL 1\.0|secreto de DL 1\.0|sugerido por convención)")


def llaves_generadas(out_root):
    """Nombres de las llaves que la corrida anterior propuso en out/.../variables.yaml (línea con comentario del generador)."""
    var = out_root / "artifacts/co/variables.yaml"
    if not var.exists():
        return set()
    return {renombrar(k) for k in (llave_de(l) for l in var.read_text(encoding="utf-8").splitlines()
                                   if MARCA_PROPUESTA.search(l)) if k}


def aplicar_ediciones(files, out_root, editados, manifiesto):
    """Registra el hash de lo generado y, si el usuario había editado ese archivo, conserva su versión."""
    out, cons = [], []
    for fp, txt in files:
        rel = fp.relative_to(out_root).as_posix()
        manifiesto[rel] = sha(txt)
        if rel in editados:
            out.append((fp, editados[rel]))
            cons.append(rel)
        else:
            out.append((fp, txt))
    return out, cons


# ----------------------------------------------------------------------------- fusiones con archivos del repo
def bloque_env(lines, env):
    """(inicio, fin, indent_llave) del bloque `- <env>:` en variables.yaml (texto, sin reformatear)."""
    hdr = re.compile(rf"^(\s*)(-\s+)?['\"]?{re.escape(env)}['\"]?:\s*(#.*)?$")
    for i, l in enumerate(lines):
        m = hdr.match(l)
        if not m:
            continue
        ind = len(m.group(1))
        fin, kin = len(lines), None
        for j in range(i + 1, len(lines)):
            x = lines[j]
            if not x.strip() or x.lstrip().startswith("#"):
                continue
            xi = len(x) - len(x.lstrip())
            if xi <= ind:
                fin = j
                break
            kin = xi if kin is None else kin
        while fin > i + 1 and not lines[fin - 1].strip():
            fin -= 1
        return i, fin, kin if kin is not None else ind + 4
    return None


def _eol(raw):
    """Fin de línea dominante de unos bytes (CRLF o LF); None si no tienen saltos de línea."""
    lf = raw.count(b"\n")
    return None if not lf else ("\r\n" if raw.count(b"\r\n") > lf - raw.count(b"\r\n") else "\n")


def eol_commiteado(repo, path):
    """Fin de línea del archivo commiteado en HEAD (git show HEAD:./<ruta>), o None si no hay repo, el archivo no está
    commiteado o git falla. En Windows con core.autocrlf=true el archivo de trabajo es CRLF aunque el blob sea LF."""
    try:
        rel = Path(path).resolve().relative_to(Path(repo).resolve()).as_posix()
        r = subprocess.run(["git", "-C", str(repo), "show", f"HEAD:./{rel}"], capture_output=True)
    except (OSError, ValueError):
        return None
    return _eol(r.stdout) if r.returncode == 0 else None


def leer_texto(path, repo=None):
    """(texto con saltos \\n, fin de línea). Con repo, el fin de línea es el del blob commiteado en HEAD (fuente de
    verdad: no depende de core.autocrlf); si no se puede, el dominante del archivo de trabajo. Las copias de out/ lo
    conservan."""
    raw = Path(path).read_bytes()
    eol = (eol_commiteado(repo, path) if repo else None) or _eol(raw) or "\n"
    return raw.decode("utf-8").replace("\r\n", "\n"), eol


def escribir_texto(path, txt, eol="\n"):
    Path(path).write_text(txt.replace("\r\n", "\n").replace("\n", eol), encoding="utf-8", newline="")


def duplicadas(lines, region):
    c = Counter(k for k in (llave_de(l) for l in lines[region[0]:region[1]]) if k)
    return {k: n for k, n in c.items() if n > 1}


def verificar_fusion(base, lines, regiones_base, regiones_nuevo, movidas=None):
    """Control de la fusión: las líneas originales siguen en su orden y no se duplicó ninguna llave por bloque.
    v6.8: `movidas` = líneas que la fusión movió a propósito (G7); se comparan sin ellas y sin blancos.
    -> texto del problema o ''."""
    if movidas is not None:
        if not es_subsecuencia([l for l in base if l.strip() and l not in movidas], [l for l in lines if l.strip()]):
            return "se alteraron o reordenaron líneas existentes"
    elif not es_subsecuencia(base, lines):
        return "se alteraron o reordenaron líneas existentes"
    for rb, rn in zip(regiones_base, regiones_nuevo):
        dup = {k: n for k, n in duplicadas(lines, rn).items() if n > duplicadas(base, rb).get(k, 1)}
        if dup:
            return f"llaves duplicadas: {', '.join(sorted(dup))}"
    return ""


def renombrar_variables(texto, envs, info):
    """variables.yaml: renombre en sitio de las llaves con el código anterior (ver renombrar_regiones), por bloque de
    ambiente. Si el control del renombre falla se deja el texto sin renombrar y se avisa. -> (texto, notas)."""
    if not _cambian():
        return texto, []
    base = texto.splitlines()
    regs = [(b[0] + 1, b[1]) for b in (bloque_env(base, e) for e in envs) if b]
    tmp = info_renombre()
    lines = renombrar_regiones(base, regs, tmp)
    mal = verificar_renombre(base, lines)
    if mal:
        return texto, [f"variables.yaml: el renombre en sitio falló ({mal}); se dejó sin renombrar"]
    for k in ("llaves", "recursos", "recursos_otro_valor", "viejas"):
        info[k] += tmp[k]
    info["titulos"] += tmp["titulos"]
    out = "\n".join(lines) + ("\n" if texto.endswith("\n") else "")
    info["residuales"] += residuales(out, "variables.yaml")
    return out, []


# v6.8 (G8/G7): grupo por sistema en la sección de buckets y conexiones; formato del equipo
SECCION_SIS = "Buckets GCP & Connections JDBC | CIPHER"
CAB_SIS = re.compile(r"^#\s*[A-Za-z0-9_]+\s*$")        # encabezado de grupo de sistema: `# <SIS>`


def subgrupos_sistema(S, cipher):
    """Llaves del grupo `# <S>` en orden fijo, por subgrupo: recursos, conexión y secretos cipher."""
    rec = [f"{S}_{x}" for x in ("RAW_DATASET", "LND_BUCKET", "RAW_BUCKET")]
    cred = [f"USER_{S}_CREDENTIALS", f"PASS_{S}_CREDENTIALS"]
    if cipher:
        conn = [f"JDBC_{S}", f"{S}_HOST", f"{S}_PORT", f"{S}_SID", *cred, f"{S}_OWNER_DB"]
        sec = [f"{S}_HASHICORP_SECRET_PATH", f"{S}_SECRET_HOST", f"{S}_HASHICORP_SECRET_USER"]
    else:
        conn, sec = [f"JDBC_{S}", *cred, f"{S}_OWNER_DB"], []
    return rec, conn, sec


def _colapsa(t):
    return re.sub(r"\s+", " ", t.strip().lstrip("#").strip()).lower()


def _seccion_sis(lines, ini, fin):
    """(índice del encabezado de la sección, fin exclusivo) dentro de lines[ini:fin], o None. La sección termina en el
    siguiente comentario que va tras una línea en blanco y no es `# <SIS>` ni una llave comentada."""
    h = next((j for j in range(ini, fin) if lines[j].lstrip().startswith("#")
              and _colapsa(SECCION_SIS) in _colapsa(lines[j])), None)
    if h is None:
        return None
    for j in range(h + 1, fin):
        l = lines[j].strip()
        if l.startswith("#") and not CAB_SIS.match(l) and ":" not in l and not lines[j - 1].strip():
            return h, j
    return h, fin


def _quita(lines, j, fin):
    """Borra la línea j (dentro del bloque que termina en fin) y el blanco sobrante si quedan dos seguidos. -> líneas borradas."""
    del lines[j]
    if j < fin - 1 and not lines[j].strip() and (j == 0 or not lines[j - 1].strip()):
        del lines[j]
        return 2
    return 1


def ubicar_sistema(lines, ini, fin, titulo, K, cipher, linea, kin, movidas):
    """v6.8 (G8/G7): arma el grupo `# <SIS>` del sistema en la sección SECCION_SIS de lines[ini:fin] (bloque de un
    ambiente), en orden alfabético entre los grupos de sistema. Las llaves del sistema (K) sueltas en el bloque se
    MUEVEN al grupo (conservan valor y comentario); las ausentes se generan con linea(k). Un grupo existente se
    reconstruye en su sitio y lo que no es llave conocida del sistema queda al final. Las líneas movidas o reescritas
    van a `movidas` (control de la fusión). -> (ok, nombres de llaves movidas); ok False = no hay sección."""
    S = titulo.upper()
    if _seccion_sis(lines, ini, fin) is None:
        return False, []
    conocidas = {k for sg in subgrupos_sistema(S, True) for k in sg}
    simple = lambda j: not any(x.strip() and not x.lstrip().startswith("#")        # sin valor multilínea debajo
                               and len(x) - len(x.lstrip()) > len(lines[j]) - len(lines[j].lstrip()) for x in lines[j + 1:j + 2])
    h = next((j for j in range(ini, fin) if CAB_SIS.match(lines[j].strip()) and _colapsa(lines[j]).upper() == S), None)
    prop, extras, e = {}, [], h
    if h is not None:                       # rachas del grupo: la primera siempre; las siguientes solo con llaves del sistema
        j = h + 1
        e = j
        while j < fin:
            r0 = j
            while j < fin and lines[j].strip():
                j += 1
            racha = lines[r0:j]
            if r0 > h + 1 and not all(llave_de(x) in conocidas for x in racha):
                break
            for x in racha:
                k = llave_de(x)
                if k in conocidas and k not in prop:
                    prop[k] = x
                else:
                    extras.append(x)
            e = max(e, j)
            while j < fin and not lines[j].strip():
                j += 1
        movidas.update(x for x in lines[h:e] if x.strip())
    dentro = lambda j: h is not None and h <= j < e
    sueltas = []
    for j in range(ini, fin):
        k = llave_de(lines[j])
        if k in K and not dentro(j) and simple(j) and k not in prop:
            sueltas.append((k, j))
            prop[k] = lines[j]
    ocupadas = {sk for sk, _ in sueltas} | set(prop)
    complejas = {llave_de(lines[j]) for j in range(ini, fin) if llave_de(lines[j]) in K and not dentro(j)} - ocupadas
    movidas.update(lines[j] for _, j in sueltas)
    for _, j in sorted(sueltas, key=lambda x: -x[1]):   # de abajo hacia arriba: los índices previos no cambian
        n = _quita(lines, j, fin)
        fin -= n
        if h is not None and j < h:
            h, e = h - n, e - n
    g = []
    for sg in (*subgrupos_sistema(S, cipher), None):
        ls = extras if sg is None else [prop.get(k) or linea(k) for k in sg if (k in K or k in prop) and k not in complejas]
        if ls:
            g += ([""] if g else []) + ls
    g = [f"{' ' * kin}# {S}"] + g
    sh, se = _seccion_sis(lines, ini, fin)
    if h is not None and sh < h < se:                    # ya está en la sección: se reconstruye en su sitio
        lines[h:e] = g
        return True, [k for k, _ in sueltas]
    if h is not None:                                    # estaba fuera de la sección: se saca y se reubica
        del lines[h:e]
        fin -= e - h
        while h < fin and not lines[h].strip():
            del lines[h]
            fin -= 1
    sh, se = _seccion_sis(lines, ini, fin)
    sist = [j for j in range(sh + 1, se) if CAB_SIS.match(lines[j].strip()) and (j == sh + 1 or not lines[j - 1].strip())]
    sig = next((j for j in sist if _colapsa(lines[j]).upper() > S), None)
    if sig is not None:
        lines[sig:sig] = g + [""]
    else:
        ult = max((j for j in range(sh + 1, se) if lines[j].strip()), default=sh)
        lines[ult + 1:ult + 1] = ([""] if ult > sh else []) + g
    return True, [k for k, _ in sueltas]


def fusionar_variables(texto, propuestas, solo_con_valor=False, titulo="", ordenado=True, info=None, envs_ren=None):
    """Agrega a cada bloque de ambiente las llaves que faltan sin reformatear el archivo ni tocar valores existentes.
    - Ubicación (ordenado): por sistema o por tipo según cómo esté organizado el bloque (ubicar_llaves); igual en
      todos los ambientes. Si el control posterior falla, se usa el grupo `# <titulo>` al final y se deja una nota.
    - Paridad: con solo_con_valor (--vars/--apply) una llave entra si tiene valor en algún ambiente; en los demás va
      vacía con `# PENDIENTE valor <env>`.
    - Una llave que ya existe vacía se completa si hay valor (no se pisa ningún valor existente)."""
    q = lambda v: "'" + str(v).replace("'", "''") + "'"
    notas = []
    if info is not None:
        texto, notas = renombrar_variables(texto, list(envs_ren or propuestas), info)
    lines = texto.splitlines()
    con_valor = {k for p in propuestas.values() for k, (v, _) in p.items() if v}
    todas = {k for p in propuestas.values() for k in p}
    elegidas = con_valor if solo_con_valor else todas
    for env in propuestas:                                # completar llaves existentes vacías
        b = bloque_env(lines, env)
        if not b:
            notas.append(f"bloque {env} no encontrado en variables.yaml")
            continue
        ini, fin, kin = b
        props = propuestas[env]
        for j in range(ini + 1, fin):
            k = llave_de(lines[j])
            val = lines[j].split(":", 1)[1].split("#")[0].strip().strip("'\"") if k else "x"
            if k in elegidas and not val and props.get(k, ("", ""))[0]:
                v, c = props[k]
                lines[j] = f"{' ' * kin}{k}: {q(v)}" + (f"   # {c}" if c else "")
    base = list(lines)
    movidas, movidas_k = set(), set()
    K, cipher = set(), False
    if titulo and ordenado:                               # v6.8 (G8): llaves del grupo del sistema, iguales en todos los bloques
        conocidas = {k for sg in subgrupos_sistema(titulo.upper(), True) for k in sg}
        K = {k for k in elegidas if k in conocidas}
        for env in propuestas:
            b = bloque_env(lines, env)
            K |= {llave_de(l) for l in lines[b[0] + 1:b[1]]} & conocidas if b else set()
        S = titulo.upper()
        cipher = bool(K & {f"{S}_{x}" for x in ("HOST", "PORT", "SID", "HASHICORP_SECRET_PATH", "SECRET_HOST",
                                                  "HASHICORP_SECRET_USER")})
        if cipher:
            K |= {f"USER_{S}_CREDENTIALS", f"PASS_{S}_CREDENTIALS"}
    for env in propuestas:
        b = bloque_env(lines, env)
        if not b:
            continue
        ini, fin, kin = b
        props = propuestas[env]
        def linea(k):
            v, c = props.get(k, ("", f"PENDIENTE valor {env}"))
            if not v and not c.startswith("PENDIENTE"):
                c = f"PENDIENTE valor {env}"
            return f"{' ' * kin}{k}: {q(v) if v else chr(39) * 2}" + (f"   # {c}" if c else "")
        nuevas = [(k, linea(k)) for k in sorted(elegidas)]
        if titulo and ordenado:
            ubicar_llaves(lines, ini + 1, fin, titulo, [x for x in nuevas if x[0] not in K], kin)   # el resto, como antes
            ini, fin, kin = bloque_env(lines, env)
            ok, mv = ubicar_sistema(lines, ini + 1, fin, titulo, K, cipher, linea, kin, movidas)
            if not ok:                                    # sin la sección: criterio anterior y aviso
                ubicar_llaves(lines, ini + 1, fin, titulo, [(k, linea(k)) for k in sorted(K)], kin)
                notas.append(f"variables.yaml: no se encontró la sección `# {SECCION_SIS}` en {env}; el grupo `# {titulo.upper()}` "
                             "se ubicó con el criterio anterior")
            movidas_k |= set(mv)
        elif titulo:
            insertar_grupo(lines, ini + 1, fin, titulo, nuevas, kin)
        else:
            ya = {llave_de(l) for l in lines[ini + 1:fin]}
            lines[fin:fin] = [x for k, x in nuevas if k not in ya]
    if ordenado and titulo:                               # control: sin cambios a lo existente ni llaves duplicadas
        def regiones(ls):
            return [(b[0] + 1, b[1]) for b in (bloque_env(ls, e) for e in propuestas) if b]
        mal = verificar_fusion(base, lines, regiones(base), regiones(lines), movidas or None)
        if mal:
            res, n2 = fusionar_variables(texto, propuestas, solo_con_valor, titulo, ordenado=False)   # texto ya renombrado
            return res, n2 + [f"variables.yaml: la ubicación ordenada falló ({mal}); se usó el grupo `# {titulo}` "
                              "al final de cada bloque"]
    if movidas_k:
        notas.append(f"variables.yaml: llaves del sistema movidas al grupo `# {titulo.upper()}` (valor y comentario "
                     f"conservados; solo nombres): {', '.join(sorted(movidas_k))}")
    return "\n".join(lines) + "\n", notas

def escribir_variables(a, out_root, propuestas, titulo="", envs=None):
    src = Path(a.artifacts) / "co/variables.yaml"
    if not src.exists():
        return []
    texto, eol = leer_texto(src, a.artifacts)
    ren = getattr(a, "ren", None) or info_renombre()
    txt, notas = fusionar_variables(texto, propuestas, titulo=titulo, info=ren, envs_ren=envs)
    if not any(propuestas.values()) and txt.rstrip("\n") == texto.rstrip("\n"):
        return notas                                # nada que agregar ni renombrar: no hay copia que escribir
    dst = out_root / "artifacts/co/variables.yaml"
    dst.parent.mkdir(parents=True, exist_ok=True)
    escribir_texto(dst, txt, eol)
    return notas


ITEM_FUENTE = re.compile(r"^(\s*)-\s+name:\s*['\"]?([\w\-]+)['\"]?\s*$")


def bloques_fuentes(ls):
    """[[inicio, fin, nombre]] de los sources de __sources.yml (líneas)."""
    out, top = [], None
    for i, l in enumerate(ls):
        m = ITEM_FUENTE.match(l)
        if m and (top is None or len(m.group(1)) == top):
            top = len(m.group(1))
            out.append([i, len(ls), m.group(2)])
        elif out and l.strip() and not l.lstrip().startswith("#") and len(l) - len(l.lstrip()) <= top \
                and not ITEM_FUENTE.match(l):
            out[-1][1] = min(out[-1][1], i)
            break
    for k in range(len(out) - 1):
        out[k][1] = out[k + 1][0]
    return out


def renombrar_fuentes(texto, info=None):
    """__sources.yml: renombra EN SITIO el source raw_<código anterior> (nombre, var del dataset y tablas del bloque) a
    raw_<nombre DL 2.0>. Si el source nuevo ya existe se deja y el viejo se reporta. -> texto."""
    if not _cambian() or not texto.strip():
        return texto
    lines = texto.splitlines()
    bs = bloques_fuentes(lines)
    nombres = {b[2] for b in bs}
    cam = {f"raw_{c}": f"raw_{n}" for c, n in _cambian().items()}
    for i0, f0, nom in bs:
        if nom.lower() in cam and cam[nom.lower()] not in nombres:
            for j in range(i0, f0):
                lines[j] = renombrar(lines[j])
            if info is not None:
                info["fuentes"].append((nom, cam[nom.lower()]))
        elif nom.lower() in cam and info is not None:
            info["viejas"].append(nom)
    out = "\n".join(lines) + "\n"
    if info is not None:
        info["residuales"] += residuales(out, "__sources.yml")
    return out


def fusionar_fuentes(texto, sis, tablas, plantilla="", info=None):
    """Agrega tablas al source raw_<sis> de __sources.yml (texto; sis = nombre DL 2.0). Si el source anterior
    raw_<código> existe se renombra en sitio. Si el source no existe, clona el primer bloque del archivo (o de
    `plantilla`) cambiando el sistema. Devuelve (texto, nota)."""
    texto = renombrar_fuentes(texto, info)
    lines = texto.splitlines() if texto.strip() else []
    item, bloques = ITEM_FUENTE, bloques_fuentes
    def agregar(ls, ini, fin, nombres):
        ti = next((j for j in range(ini, fin) if re.match(r"^\s*tables:\s*$", ls[j])), None)
        if ti is None:
            return ls, "source sin 'tables:'"
        tind = len(ls[ti]) - len(ls[ti].lstrip())
        hijos = [j for j in range(ti + 1, fin) if item.match(ls[j]) and len(item.match(ls[j]).group(1)) > tind]
        kin = len(item.match(ls[hijos[0]]).group(1)) if hijos else tind + 2
        ya = {item.match(ls[j]).group(2) for j in hijos}
        fin2 = fin
        while fin2 > ti + 1 and not ls[fin2 - 1].strip():
            fin2 -= 1
        ls[fin2:fin2] = [f"{' ' * kin}- name: '{n}'" for n in nombres if n not in ya]
        return ls, ""
    bs = [b for b in bloques(lines) if b[2] == f"raw_{sis}"] if lines else []
    if bs:
        lines, nota = agregar(lines, bs[0][0], bs[0][1], tablas)
        return "\n".join(lines) + "\n", nota
    base = lines or (plantilla.splitlines() if plantilla.strip() else [])
    bp = bloques(base)
    if not bp:
        return texto, f"no hay plantilla de __sources.yml para crear raw_{sis}: declararlo a mano"
    i0, f0, nom = bp[0]
    viejo = nom.removeprefix("raw_")
    blk = [re.sub(re.escape(viejo), sis, re.sub(re.escape(viejo.upper()), sis.upper(), l)) for l in base[i0:f0]]
    blk[0] = re.sub(r"(name:\s*)(['\"]?)(raw_[\w\-]+)\2\s*$", lambda m: f"{m.group(1)}'{m.group(3)}'", blk[0])
    ti = next((j for j, l in enumerate(blk) if re.match(r"^\s*tables:\s*$", l)), None)
    if ti is not None:
        tind = len(blk[ti]) - len(blk[ti].lstrip())
        blk = blk[:ti + 1] + [l for l in blk[ti + 1:] if l.strip() and len(l) - len(l.lstrip()) <= tind]
    blk, _ = agregar(blk, 0, len(blk), tablas)
    if lines:
        fin = bloques(lines)[-1][1]
        while fin > 0 and not lines[fin - 1].strip():
            fin -= 1
        lines[fin:fin] = [""] + blk                    # separado del source anterior
    else:
        lines = base[:i0] + blk
    return "\n".join(lines) + "\n", f"source raw_{sis} creado copiando la estructura de {nom}: revisar dataset y database"


RUNTIME_VARS = {"information_date", "extraction_date", "source"}   # las envía el DAG en la ejecución
DBT_ENV = dict(CLI["dbt_env"])                        # bloque de variables.yaml -> vars.ambientes
MARCA_VAR = "dl2-gen: crear"                                        # marca de var agregada por el generador


def vars_dbt_faltantes(a, out_root, sis, mvars, envs):
    """var() de los modelos, schema.yml y source raw_<sis> generados que no existen en dbt_project.yml.
    -> {var: {"dev": v, "prd": v, "origen": txt}}. Valor: artifacts co/variables.yaml (mismo nombre de llave);
    si no está y es el dataset del source, raw_<sis>; si no, vacío (PENDIENTE)."""
    refs = set()
    for f in (out_root / "models").rglob("*"):
        if f.suffix not in (".sql", ".yml") or f.name == "dbt_project.yml":
            continue
        txt = f.read_text(encoding="utf-8")
        if f.name == "__sources.yml":                    # solo el bloque raw_<sis>
            txt = "\n".join(vars_bloque_fuente(txt, sis))
        refs |= set(re.findall(r"var\(\s*['\"]([\w-]+)['\"]", txt))
    out = {}
    for k in sorted(refs - RUNTIME_VARS - set(mvars or ()) - {PEND}):
        vals = {DBT_ENV[e]: str(envs.get(e, {}).get(k) or "") for e in DBT_ENV}
        if any(vals.values()):
            origen = "desde artifacts co/variables.yaml"
        elif "DATASET" in k.upper():
            vals, origen = {e: f"raw_{sis}" for e in vals}, "dataset raw por convención"
        else:
            origen = "sin valor en artifacts: definir"
        out[k] = {**vals, "origen": origen}
    return out


def vars_bloque_fuente(txt, sis):
    bloque, dentro = [], False
    for l in txt.splitlines():
        m = re.match(r"^(\s*)-\s+name:\s*['\"]?([\w\-]+)", l)
        if m and len(m.group(1)) <= 4:
            dentro = m.group(2) == f"raw_{sis}"
        if dentro:
            bloque.append(l)
    return bloque


def llave_de(l):
    m = re.match(r"^\s*['\"]?([\w.\-]+)['\"]?\s*:", l)
    return m.group(1) if m and not l.lstrip().startswith("#") else None


def insertar_ordenado(lines, g0, g1, nuevas):
    """Inserta (llave, línea) en lines[g0:g1] en orden alfabético sin mover las existentes. Devuelve cuántas."""
    n = 0
    for k, x in sorted(nuevas):
        pos = next((j for j in range(g0, g1 + n) if (llave_de(lines[j]) or "") > k), g1 + n)
        lines.insert(pos, x)
        n += 1
    return n


def insertar_grupo(lines, ini, fin, titulo, nuevas, kin):
    """Inserta (llave, línea) en lines[ini:fin]: dentro del grupo `# titulo` si existe (en orden alfabético, sin
    mover lo existente); si no, al final del bloque con línea en blanco + `# titulo`. Omite llaves ya presentes."""
    ya = {llave_de(l) for l in lines[ini:fin]} - {None}
    nuevas = [(k, x) for k, x in nuevas if k not in ya]
    h = next((j for j in range(ini, fin) if lines[j].strip() == f"# {titulo}"), None)
    if h is not None:
        g = h + 1
        while g < fin and lines[g].strip():
            g += 1
        grupo = lines[h + 1:g]
        if all(llave_de(l) for l in grupo):          # grupo del generador: se deja en orden alfabético
            lines[h + 1:g] = sorted(grupo + [x for _, x in nuevas], key=llave_de)
            return len(nuevas)
        return insertar_ordenado(lines, h + 1, g, nuevas)
    if not nuevas:
        return 0
    while fin > ini and not lines[fin - 1].strip():
        fin -= 1
    lines[fin:fin] = ["", f"{' ' * kin}# {titulo}"] + [x for _, x in sorted(nuevas)]
    return len(nuevas) + 2


# --- ubicación ordenada de llaves nuevas (variables.yaml y dbt_project.yml) ---
SINONIMO_GRUPO = {   # token de la llave -> palabras del encabezado de grupo que lo describen
    "topic": {"topicos", "topico"}, "bucket": {"bucket", "buckets"}, "dataset": {"dataset", "datasets"},
    **{t: {"secretos", "secreto", "credenciales"} for t in ("credentials", "secret", "user", "pass")},
    **{t: {"policy", "tags", "politicas"} for t in ("pt", "policy")},
    **{t: {"conexion", "conexiones", "jdbc"} for t in ("jdbc", "host", "port", "sid")},
    "email": {"correo", "alertas"}}
PUNTAJE_MINIMO = 2


def _hdr_norm(linea):
    """'# Bucket Configuración' -> 'bucket_configuracion' (minúsculas, sin '#', espacios a '_', sin tildes)."""
    t = unicodedata.normalize("NFKD", linea.strip().lstrip("#")).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", "_", t.strip().lower())


def _grupos(lines, ini, fin):
    """Grupos de lines[ini:fin]: rachas de líneas no vacías. Encabezado = comentarios iniciales (nh líneas);
    keys = [(llave, índice)]."""
    out, j = [], ini
    while j < fin:
        if not lines[j].strip():
            j += 1
            continue
        g0 = j
        while j < fin and lines[j].strip():
            j += 1
        nh = 0
        while g0 + nh < j and lines[g0 + nh].lstrip().startswith("#"):
            nh += 1
        out.append({"ini": g0, "fin": j, "nh": nh, "hdr": _hdr_norm(lines[g0]) if nh else "",
                    "keys": [(llave_de(lines[x]), x) for x in range(g0 + nh, j) if llave_de(lines[x])]})
    return out


def _nombra_sistema(g, titulo):
    """El encabezado del grupo es un sistema: igual a `titulo` o al primer token de al menos la mitad de sus llaves."""
    if not g["hdr"]:
        return False
    if g["hdr"] == _hdr_norm(titulo):
        return True
    return bool(g["keys"]) and 2 * sum(k.split("_")[0].lower() == g["hdr"] for k, _ in g["keys"]) >= len(g["keys"])


def _puntaje_grupo(llave, g, sis_tok):
    """Similitud de una llave nueva con un grupo de tipo: sufijo común (2 por token), prefijo común (2 por token) y
    palabras del encabezado (3 por token). El token del propio sistema no cuenta."""
    tk = [t for t in llave.split("_") if t.upper() not in sis_tok]
    suf = pre = 0
    for gk, _ in g["keys"]:
        gt = [t for t in gk.split("_") if t.upper() not in sis_tok]
        n = 0
        while n < min(len(tk), len(gt)) and tk[-1 - n] == gt[-1 - n]:
            n += 1
        suf = max(suf, n)
        n = 0
        while n < min(len(tk), len(gt)) and tk[n] == gt[n]:
            n += 1
        pre = max(pre, n)
    hw = {w for w in re.split(r"[^a-z0-9]+", g["hdr"]) if w}
    enc = sum(1 for t in {x.lower() for x in tk} if (SINONIMO_GRUPO.get(t, set()) | {t}) & hw)
    return 2 * suf + 2 * pre + 3 * enc


def ubicar_llaves(lines, ini, fin, titulo, nuevas, kin):
    """Inserta (llave, línea) en lines[ini:fin] donde corresponde, sin mover, reordenar ni borrar líneas existentes.
    - Sin grupos con encabezado: grupo `# titulo` al final (insertar_grupo).
    - Disposición por sistema (>= 50 % de los grupos con encabezado llevan nombre de sistema): dentro del grupo
      `# titulo` (orden alfabético) o grupo nuevo en el orden alfabético de los grupos de sistema.
    - Disposición por tipo (Tópicos, Buckets, Policy tags…): cada llave al grupo más parecido (_puntaje_grupo,
      mínimo PUNTAJE_MINIMO; empate: el primero); sin grupo aceptable -> grupo `# titulo` al final.
    Omite llaves ya presentes en el bloque. Devuelve cuántas líneas agregó."""
    ya = {llave_de(l) for l in lines[ini:fin]} - {None}
    nuevas = sorted({k: x for k, x in nuevas if k not in ya}.items())
    if not nuevas:
        return 0
    n0, thdr = len(lines), _hdr_norm(titulo)
    gs = _grupos(lines, ini, fin)
    con_hdr = [g for g in gs if g["hdr"]]
    if not con_hdr:
        insertar_grupo(lines, ini, fin, titulo, nuevas, kin)
        return len(lines) - n0
    cab = f"{' ' * kin}# {titulo}"
    sobran = nuevas
    if 2 * sum(_nombra_sistema(g, titulo) for g in con_hdr) >= len(con_hdr):          # disposición por sistema
        gt = next((g for g in con_hdr if g["hdr"] == thdr), None)
        if gt:
            insertar_ordenado(lines, gt["ini"] + gt["nh"], gt["fin"], nuevas)
            return len(lines) - n0
        sist = [g for g in con_hdr if _nombra_sistema(g, titulo)]
        sig = next((g for g in sist if g["hdr"] > thdr), None)
        bloque = [cab] + [x for _, x in nuevas]
        if sig:
            lines[sig["ini"]:sig["ini"]] = bloque + [""]
        else:
            lines[sist[-1]["fin"]:sist[-1]["fin"]] = [""] + bloque
        return len(lines) - n0
    sis_tok = {t for t in titulo.upper().split("_") if t}                                # disposición por tipo
    sobran = []
    for k, x in nuevas:
        gs = _grupos(lines, ini, fin)
        mejor, mp = None, PUNTAJE_MINIMO - 1
        for g in gs:
            if g["hdr"] and g["hdr"] != thdr:
                pt = _puntaje_grupo(k, g, sis_tok)
                if pt > mp:
                    mejor, mp = g, pt
        if mejor is None:
            sobran.append((k, x))
            continue
        ks = [kk for kk, _ in mejor["keys"]]
        if ks == sorted(ks):
            insertar_ordenado(lines, mejor["ini"] + mejor["nh"], mejor["fin"], [(k, x)])
        else:
            lines.insert(mejor["keys"][-1][1] + 1 if mejor["keys"] else mejor["fin"], x)
        fin += 1
    if sobran:
        gt = next((g for g in _grupos(lines, ini, fin) if g["hdr"] == thdr), None)
        if gt:
            insertar_ordenado(lines, gt["ini"] + gt["nh"], gt["fin"], sobran)
        else:
            while fin > ini and not lines[fin - 1].strip():
                fin -= 1
            lines[fin:fin] = ["", cab] + [x for _, x in sobran]
    return len(lines) - n0


def es_subsecuencia(base, nuevo):
    """¿Todas las líneas de `base` están, en el mismo orden, en `nuevo`?"""
    it = iter(nuevo)
    return all(any(l == m for m in it) for l in base)


def fin_bloque(lines, i):
    """Fin (exclusivo) del bloque que abre la línea i: primera línea con contenido e indentación <= la de i."""
    ind = len(lines[i]) - len(lines[i].lstrip())
    for j in range(i + 1, len(lines)):
        if lines[j].strip() and not lines[j].lstrip().startswith("#") and len(lines[j]) - len(lines[j].lstrip()) <= ind:
            return j
    return len(lines)


def regiones_dbt(lines):
    """[(ini, fin)] de las secciones de llaves de `vars:`: comunes, y cada ambientes.dev / ambientes.prd."""
    v = next((i for i, l in enumerate(lines) if re.match(r"^vars:\s*(#.*)?$", l)), None)
    if v is None:
        return []
    fv = fin_bloque(lines, v)
    ia = next((j for j in range(v + 1, fv) if re.match(r"^\s+ambientes:\s*(#.*)?$", lines[j])), None)
    regs = [(v + 1, ia if ia is not None else fv)]
    if ia is not None:
        regs += [(j + 1, fin_bloque(lines, j)) for j in range(ia + 1, fin_bloque(lines, ia))
                 if re.match(r"^\s+(dev|prd):\s*(#.*)?$", lines[j])]
    return regs


def renombrar_dbt(texto, info):
    """dbt_project.yml: renombre en sitio de las vars (comunes y vars.ambientes.*) con el código anterior. Mismo control
    que variables.yaml. -> (texto, notas)."""
    if not _cambian():
        return texto, []
    base = texto.splitlines()
    tmp = info_renombre()
    lines = renombrar_regiones(base, regiones_dbt(base), tmp)
    mal = verificar_renombre(base, lines)
    if mal:
        return texto, [f"dbt_project.yml: el renombre en sitio falló ({mal}); se dejó sin renombrar"]
    for k in ("llaves", "recursos", "recursos_otro_valor", "viejas"):
        info["dbt_" + k] = info.get("dbt_" + k, []) + tmp[k]
    out = "\n".join(lines) + ("\n" if texto.endswith("\n") else "")
    info["residuales"] += residuales(out, "dbt_project.yml")
    return out, []


def fusionar_dbt(texto, faltan, titulo, solo_con_valor=False, ordenado=True):
    """Agrega a dbt_project.yml las vars que faltan, sin reformatear: mismo valor en dev y prd -> vars comunes;
    distinto -> vars.ambientes.dev/prd (paridad: ambas). Cada llave lleva `# dl2-gen: crear · <origen>`.
    La ubicación sigue la organización de cada sección (ubicar_llaves); si el control posterior falla se usa el grupo
    `# <titulo>` al final y se devuelve una nota."""
    q = lambda v: "'" + str(v).replace("'", "''") + "'"
    lines = texto.splitlines()
    base = list(lines)
    ubicar = ubicar_llaves if ordenado else insertar_grupo
    v = next((i for i, l in enumerate(lines) if re.match(r"^vars:\s*(#.*)?$", l)), None)
    if v is None:
        return texto, "dbt_project.yml sin bloque vars"
    comunes, amb = {}, {"dev": {}, "prd": {}}
    for k, d in sorted(faltan.items()):
        if solo_con_valor and not (d["dev"] or d["prd"]):
            continue
        if d["dev"] == d["prd"] and d["dev"]:
            comunes[k] = (d["dev"], d["origen"])
        else:
            for e in amb:
                amb[e][k] = (d[e], d["origen"] if d[e] else f"PENDIENTE valor {e}")
    linea = lambda ind, k, val, org: f"{' ' * ind}{k}: {q(val) if val else chr(39) * 2}   # {MARCA_VAR} · {org}"
    fv = fin_bloque(lines, v)
    ia = next((j for j in range(v + 1, fv) if re.match(r"^\s+ambientes:\s*(#.*)?$", lines[j])), None)
    if ia is not None and any(amb.values()):
        fa = fin_bloque(lines, ia)
        hijos = [j for j in range(ia + 1, fa) if re.match(r"^\s+(dev|prd):\s*(#.*)?$", lines[j])]
        for j in sorted(hijos, reverse=True):            # de abajo hacia arriba: no se corren los índices
            e = lines[j].strip().split(":")[0]
            fj = fin_bloque(lines, j)
            kin = next((len(l) - len(l.lstrip()) for l in lines[j + 1:fj] if l.strip() and not l.lstrip().startswith("#")),
                       len(lines[j]) - len(lines[j].lstrip()) + 2)
            ubicar(lines, j + 1, fj, titulo, [(k, linea(kin, k, *vo)) for k, vo in amb[e].items()], kin)
    if comunes:
        fv = fin_bloque(lines, v)
        ia = next((j for j in range(v + 1, fv) if re.match(r"^\s+ambientes:\s*(#.*)?$", lines[j])), None)
        pos = ia if ia is not None else fv
        kin = next((len(l) - len(l.lstrip()) for l in lines[v + 1:fv] if l.strip() and not l.lstrip().startswith("#")), 2)
        n0 = len(lines)
        ubicar(lines, v + 1, pos, titulo, [(k, linea(kin, k, *vo)) for k, vo in comunes.items()], kin)
        n = len(lines) - n0
        if n and ia is not None and lines[ia + n - 1].strip():
            lines.insert(ia + n, "")                     # línea en blanco antes de `ambientes:`
    if ordenado:
        mal = verificar_fusion(base, lines, regiones_dbt(base), regiones_dbt(lines))
        if mal:
            res, _ = fusionar_dbt(texto, faltan, titulo, solo_con_valor, ordenado=False)
            return res, (f"dbt_project.yml: la ubicación ordenada falló ({mal}); se usó el grupo `# {titulo}` "
                         "al final de cada sección")
    return "\n".join(lines) + "\n", ""


def escribir_dbt_vars(a, out_root, faltan, titulo, info=None):
    """out/models/dbt_project.yml: dbt_project.yml de la rama con las vars renombradas en sitio + vars faltantes
    (pendientes vacías y marcadas). Sin vars faltantes ni renombres no se escribe copia."""
    src = Path(a.models) / "dbt_project.yml"
    if not src.exists():
        return []
    texto, eol = leer_texto(src, a.models)
    texto0 = texto
    notas_r = []
    if info is not None:
        texto, notas_r = renombrar_dbt(texto, info)
    if not faltan and texto.rstrip("\n") == texto0.rstrip("\n"):
        return notas_r
    txt, nota = fusionar_dbt(texto, faltan, titulo, solo_con_valor=False) if faltan else (texto, "")
    nota = nota or (notas_r[0] if notas_r else "")
    dst = out_root / "models/dbt_project.yml"
    dst.parent.mkdir(parents=True, exist_ok=True)
    escribir_texto(dst, txt, eol)
    return [nota] if nota else []


def escribir_fuentes(a, out_root, fuentes, info=None):
    """out/: __sources.yml por carpeta std_<dominio> con las tablas nuevas (el repo solo se lee). Además, todo
    __sources.yml del repo con el source raw_<código anterior> sale renombrado aunque el dominio no tenga tablas nuevas
    (si no, un dominio quedaría apuntando a la var del dataset anterior)."""
    notas, models = [], Path(a.models) / f"{DBT}/models"
    plantilla = next((f.read_text(encoding="utf-8") for f in sorted(models.glob("std_*/__sources.yml"))), "")
    hechos = {dom for (dom, _) in fuentes}
    for f in sorted(models.glob("std_*/__sources.yml")):             # dominios solo con renombre
        dom = f.parent.name.removeprefix("std_")
        if dom in hechos or not _cambian():
            continue
        texto, eol = leer_texto(f, a.models)
        txt = renombrar_fuentes(texto, info)
        if txt.rstrip("\n") != texto.rstrip("\n"):
            dst = out_root / "models" / f.relative_to(a.models)
            dst.parent.mkdir(parents=True, exist_ok=True)
            escribir_texto(dst, txt, eol)
    for (dom, sis), tablas in fuentes.items():
        rel = Path(f"{DBT}/models") / f"std_{dom}" / "__sources.yml"
        src = Path(a.models) / rel
        texto, eol = leer_texto(src, a.models) if src.exists() else ("", "\n")
        txt, nota = fusionar_fuentes(texto, sis, tablas, plantilla, info)
        if nota:
            notas.append(f"std_{dom}/__sources.yml: {nota}")
        if not txt.strip():
            continue
        if src.exists() and txt.rstrip("\n") == texto.rstrip("\n"):
            continue                                    # v6.8.3: ya declaradas en el repo: sin copia que revisar
        dst = out_root / "models" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        escribir_texto(dst, txt, eol)
    return notas


def estado_llave(k, e, envs, propuestas):
    """Estado de una llave en un bloque: existe / vacía / manual / sugerido / DL 1.0 / PENDIENTE."""
    if k in envs.get(e, {}) and envs[e][k] not in (None, ""):
        return "existe"
    v, c = propuestas.get(e, {}).get(k, ("", ""))
    if v:
        return ("manual (out/)" if c.startswith("valor manual") else
                "sugerido (crear)" if c.startswith("sugerido") else "valor DL 1.0")
    return "vacía en el repo" if k in envs.get(e, {}) else "PENDIENTE"


def seccion_renombre(maestra, resumen, ren):
    """Sección ESTADO.md «Renombre de sistema»: solo si el sistema tiene nombre DL 2.0 distinto del código. Solo nombres
    de llaves y rutas, sin valores ni IDs de proyecto."""
    nom = nombre_sis(maestra)
    if nom == maestra:
        return []
    r_ = ren or {}
    ret = r_.get("retiro") or {"rutas": [], "productivo": [], "omitidos": []}
    L = ["", "## Renombre de sistema", "",
         f"Decisión TL (20-nov): el nombre DL 2.0 del sistema es `{nom}` (código de entrada `{maestra}`, que sigue en `in/`, "
         f"`out/{maestra}/` y la lista de tablas). Nada de lo generado en `artifacts/` y `models/` lleva el nombre anterior.", ""]
    creadas = sorted({Path(x).parent.as_posix() for r in resumen for x in r.get("archivos", [])})
    L += ["### Rutas creadas (out/)", ""] + ([f"- `{x}`" for x in creadas] or ["- (ninguna: sin tablas con archivos)"])
    L += ["", f"### Rutas retiradas (`out/{maestra}/out/retirar_anteriores.sh`, se ejecuta en la rama de trabajo)", ""]
    L += [f"- `{x}`" for x in ret["rutas"]] or ["- ninguna (no hay archivos versionados con marca `dl2-gen` y el nombre anterior)"]
    for t_ in ret["productivo"]:
        L.append(f"- productivo: requiere aprobación TL: `{t_}` (no se incluye en el script)")
    for x, motivo in ret["omitidos"]:
        L.append(f"- no se retira: `{x}` ({motivo})")
    if ret.get("huerfanos"):
        L += ["", "AVISO: estos archivos con el nombre anterior NO se retiran y usan llaves que ahora se renombran en "
              "variables.yaml / dbt_project.yml; al copiar quedarían sin llave. Migrarlos a mano o retirarlos con el TL: "
              + ", ".join(f"`{x}`" for x in ret["huerfanos"])]
    llaves = sorted(set(r_.get("llaves", [])) | set(r_.get("dbt_llaves", [])))
    L += ["", "### Llaves renombradas (solo nombres; mismo valor y misma línea en variables.yaml / dbt_project.yml)", ""]
    L += [f"- `{a_}` → `{b_}`" for a_, b_ in llaves] or ["- ninguna"]
    rec = sorted(set(r_.get("recursos", [])) | set(r_.get("dbt_recursos", [])))
    if rec:
        L += ["", "Recursos con el valor pasado a la convención nueva (llevan la nota `renombrado (TL 20-nov): confirmar que "
                  "el recurso existe`; su creación se solicita en Terraform):", ""] + [f"- `{k}`" for k in rec]
    otro = sorted(set(r_.get("recursos_otro_valor", [])) | set(r_.get("dbt_recursos_otro_valor", [])))
    if otro:
        L += ["", "Llaves de recurso renombradas cuyo valor NO seguía la convención vieja (se conserva el valor; revisar): "
              + ", ".join(f"`{k}`" for k in otro)]
    viejas = sorted(set(r_.get("viejas", [])) | set(r_.get("dbt_viejas", [])))
    if viejas:
        L += ["", "Llaves/sources anteriores que se dejan porque el nombre nuevo ya existía (no se duplicó; retirar a mano): "
              + ", ".join(f"`{k}`" for k in viejas)]
    if r_.get("fuentes"):
        L += ["", "Sources renombrados en `__sources.yml`: " + ", ".join(f"`{a_}` → `{b_}`" for a_, b_ in r_["fuentes"])]
    if r_.get("residuales"):
        L += ["", "AVISO: quedan referencias al nombre anterior en la copia de out/ (revisar a mano): "
              + ", ".join(sorted(set(r_["residuales"])))]
    L += ["", "### Objetos que quedan en dev para limpieza del TL (por patrón)", "",
          f"- Dataset raw anterior (`raw_{maestra}` o `raw_<nombre anterior de recurso>`, el que exista): sus tablas.",
          f"- Dataset std: tablas `{maestra}_<tabla>_hist` y `{maestra}_<tabla>_current`.",
          f"- Composer: DAGs `{PREFIJO_DAG}{maestra}_*`.",
          "- La limpieza del deploy de dev retira de los buckets los YAML/JSON y DAG con el nombre anterior cuando se "
          "mergean los retiros; confirmar que corrió.", "",
          "Las tablas `_hist` nuevas arrancan vacías: la primera corrida es carga completa (todo `op = I`). Reprocesar fechas "
          "anteriores obliga a hacerlo en orden."]
    return L


def escribir_estado(maestra, R, resumen, propuestas, envs, notas_src, pt_dbt, dbt_nuevas=None, conservados=(), ren=None, prueba=None):
    listos = [r for r in resumen if r["estado"] == "LISTO"]
    nom_ = nombre_sis(maestra)
    L = [f"# Estado {nom_} · {date.today().isoformat()} · gen_ingestas v{VERSION}", "",
         f"Tablas: {len(resumen)} · listas: {len(listos)} · borrador: {sum(r['estado'] == 'BORRADOR' for r in resumen)} · "
         f"sin fuente: {sum(r['estado'] == 'SIN FUENTE' for r in resumen)}", "",
         "| Tabla | Estado | Conexión DL 1.0 | Pendiente |", "|---|---|---|---|"]
    for r in resumen:
        L.append(f"| {r.get('nom', r['tabla'])} | {r['estado']} | {r.get('dl1', '-')} | {'<br>'.join(r['pend'] + r.get('extra', [])) or '—'} |")
    carga = [r for r in resumen if r.get("carga")]
    if carga:
        L += ["", "## Estrategia de carga", "",
              "| Tabla | Estrategia | Sugerida | Columna control | Ventana (días) |", "|---|---|---|---|---|"]
        for r in carga:
            ef, sug, ven = r["carga"]
            L.append(f"| {r.get('nom', r['tabla'])} | {ef['est']}{' (por confirmar)' if ef['pend'] else ''} | {sug or '—'} | "
                     f"{ef['control'] or '—'} | {ven if ef['est'] == 'incremental' else '—'} |")
        L += ["", "snapshot = copia completa diaria; incremental = extrae la ventana de días hacia atrás de la columna de control "
                  "y no detecta borrados físicos (programar una conciliación periódica con cdc_hash); cdc_hash = extrae "
                  "completo pero guarda solo los cambios (I/U/D) y aborta si la extracción viene vacía o borra más del "
                  f"{UMBRAL_BORRADO:.0%} de lo vigente. Reprocesar una fecha pasada obliga a reprocesar en orden las fechas "
                  "siguientes. Consulta a una fecha D: última versión por llave con information_date <= D y op != 'D' "
                  "(el modelo _current da el estado vigente)."]
        inc = [r for r in carga if r["carga"][0]["est"] == "incremental"]
        if inc:
            L += ["", "### Carga inicial (tablas incremental)", "",
                  "La extracción incremental solo trae la ventana de días; la primera vez hay que traer la tabla completa. "
                  "DAG de extracción de cada tabla:", ""]
            L += [f"- `{PREFIJO_DAG}{r.get('nom', r['tabla'])}-extraction`" for r in inc]
            L += ["", f"1. Una sola vez por tabla: \"Trigger DAG w/ config\" con `{{\"{FECHA_INFO}\": \"1900-01-01\"}}`: la "
                      "extracción trae la tabla completa y `_hist` la carga como I.",
                  "2. Confirmar antes en dev que la config reemplaza los params del DAG.",
                  "3. Tablas grandes: subir temporalmente `extractor.resources` y `gcp.composer.extraction.timeout` "
                  "(volver a los valores normales al terminar).",
                  "4. Desde el día siguiente corre con la ventana normal.", "",
                  "cdc_hash no necesita carga inicial: la primera corrida ya es completa."]
    L += ["", "## Llaves de co/variables.yaml (solo nombres)", "",
          "| Llave | " + " | ".join(envs) + " |", "|---|" + "---|" * len(envs)]
    for k in sorted({k for p in propuestas.values() for k in p}):
        L.append(f"| {k} | " + " | ".join(
            estado_llave(k, e, envs, propuestas) for e in envs) + " |")
    crear = sorted({(e, k, v) for e, p in propuestas.items() for k, (v, c) in p.items()
                    if c.startswith("sugerido") and k not in envs.get(e, {})})
    if crear:
        L += ["", "## Recursos a crear (solicitud Terraform al TL)", "",
              "Nombres sugeridos por convención DL 2.0; revisar antes de solicitar.", "",
              "| Ambiente | Llave | Nombre sugerido |", "|---|---|---|"] + [f"| {e} | {k} | {v} |" for e, k, v in crear]
    if conservados:
        L += ["", "## Archivos editados a mano en out/ (se conservan)", "",
              "La versión generada no los reemplaza. Para regenerar uno, bórralo y vuelve a correr.", ""]
        L += [f"- `{r}`" for r in conservados]
    if dbt_nuevas:
        L += ["", "## Vars a crear en dbt_project.yml (data-models)", "",
              f"Los modelos generados las referencian y no existen. `out/models/dbt_project.yml` las trae marcadas "
              f"`# {MARCA_VAR}`: mismo valor en dev y prd → vars comunes; distinto → `vars.ambientes`. Completar las "
              "pendientes en ese archivo (se conservan entre corridas).", "",
              "| Var | Ubicación | dev | prd | Origen |", "|---|---|---|---|---|"]
        for k, d in sorted(dbt_nuevas.items()):
            ub = "comunes" if d["dev"] and d["dev"] == d["prd"] else "ambientes"
            L.append(f"| {k} | {ub} | {'con valor' if d['dev'] else 'PENDIENTE'} | "
                     f"{'con valor' if d['prd'] else 'PENDIENTE'} | {d['origen']} |")
    if notas_src:
        L += [""] + [f"- {n}" for n in notas_src]
    if any(r.get("extra") and r["extra"][0].startswith("extractor: jdbc") for r in resumen):
        L += ["", "JDBC deja el CSV sin cifrar en el bucket de landing hasta la carga a raw: confirmar IAM, ciclo de "
                  "vida y CMEK del bucket (pendientes TL) antes de prd con PII."]
    L += seccion_renombre(maestra, resumen, ren)
    L += seccion_prueba(maestra, prueba)
    L += ["", "## Siguiente paso",
          f"1. Gobierno trabaja `in/{maestra}_columnas.csv`: filtrar `estado_gobierno` = pendiente / en revisión, definir "
          f"`policy_tag` y pasar la columna a `aprobado` (responsable, ticket y observaciones se conservan); columnas "
          f"`{NUEVA}`: confirmar inclusión (`incluir` = no para excluir) y policy tag.",
          f"2. Completar en `in/{maestra}_tablas.csv` / `_columnas.csv` lo que indique la tabla.",
          "3. Llaves y vars PENDIENTE: completar el valor en `out/…/co/variables.yaml` y `out/models/dbt_project.yml` "
          "(se conservan entre corridas). El script no escribe en los repos.",
          "4. Volver a correr hasta que las tablas pasen a LISTO.",
          f"5. En la rama de trabajo: `bash out/{maestra}/out/copiar_a_repos.sh` (solo tablas LISTO, la prueba de conexión "
          f"si su tabla base está LISTO, variables, vars dbt y __sources.yml) → `git diff` → `run_checks.py` → commit → PR a {ENV_DEV}.", "",
          f"`out/` replica las rutas de los repos: `artifacts/` → {CLI['dir_artifacts']}, `models/` → "
          f"{CLI['dir_models']}. `trazabilidad_dl1.csv` y `artifacts/co/variables.yaml` llevan valores "
          "de conexión: SENSIBLE, no compartir."]
    if nom_ != maestra:
        L.insert(len(L) - 2,
                 f"6. Con el nombre anterior en la rama de trabajo: `bash out/{maestra}/out/retirar_anteriores.sh` (después de "
                 "copiar_a_repos.sh; solo `git rm` de lo versionado con marca `dl2-gen`) → git status → commit.")
    R["estado"].write_text("\n".join(L) + "\n", encoding="utf-8")


# ----------------------------------------------------------------------------- prueba de conexión (v6.6; permanente desde v6.7)
PRUEBA_TAG = "prueba-conexion"                         # marca propia del YAML
PRUEBA_REGEX = "n/a.*\\/.*.csv$"                       # nunca coincide: lnd-to-transformed no se dispara
# tecnología -> (consulta mínima al diccionario del motor, columnas que declara el extractor cipher)
PRUEBA_SQL = {
    "oracle": ("SELECT owner, table_name FROM ALL_TABLES WHERE ROWNUM <= 10", "owner,table_name"),
    "sqlserver": ("SELECT TOP 10 TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", "TABLE_SCHEMA,TABLE_NAME"),
    "db2": ("SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES FETCH FIRST 10 ROWS ONLY", "TABSCHEMA,TABNAME"),
}

def es_prueba_conexion(path):
    """True si la primera línea del archivo lleva la marca dl2-gen con prueba-conexion."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            l = f.readline()
    except OSError:
        return False
    return MARCA in l and PRUEBA_TAG in l

def yaml_prueba(y, t, scheduling=""):
    """Ingesta de prueba de conexión a partir del dict renderizado de una tabla (patrón <sis>/all_tables). Conserva conexión,
    secretos, bucket de landing y tópicos (lo que se prueba) y cambia solo lo necesario para no tocar tablas de negocio ni
    disparar lnd-to-transformed. `scheduling` = cron calculado por cron_prueba. -> (dict, consulta, columnas, aviso)."""
    tec = (t.get("tecnologia") or "").strip().lower()
    consulta, columnas = PRUEBA_SQL.get(tec) or PRUEBA_SQL["oracle"]
    aviso = "" if tec in PRUEBA_SQL else f"tecnología '{tec or '-'}' sin consulta propia: se usó la de Oracle; confirmar"
    p = copy.deepcopy(y)
    if t["extractor"] == "cipher":
        put(p, "extractor.query_string", consulta)
        put(p, "extractor.columns", columnas)
    else:
        put_ref(p, "extractor.env#source.query_string", consulta)
    put(p, "gcp.gcs.ln.regex", PRUEBA_REGEX)
    put(p, "gcp.bigquery.raw.raw_table", "na")
    put(p, "gcp.bigquery.raw.raw_schema", "na")
    put(p, "gcp.bigquery.trf_steps", [{"dbt_model": "na"}])
    put(p, "gcp.composer.extraction.scheduling", scheduling)
    return p, consulta, columnas, aviso

def _sh(x):
    return "'" + str(x).replace("'", "'\\''") + "'"

def _guarda_rama(var):
    """Líneas bash: se niega en ramas protegidas o sin rama."""
    return [f'b=$(git -C "${var}" branch --show-current)',
            'case "$b" in ' + "|".join(PROTEGIDAS) + '|"") echo "ERROR: ' + f'${var}' + ' está en la rama \'$b\' (protegida o sin rama): cambiar a una rama de trabajo"; exit 1;; esac']

def escribir_scripts_prueba(a, out_root, nom):
    """out/<sis>/out/retirar_prueba_conexion.sh (lo ejecuta el usuario; el generador no). La prueba es permanente: el script
    solo se usa si se decide retirarla."""
    rel = Path(os.path.relpath(a.artifacts, out_root)).as_posix()
    hoy = date.today().isoformat()
    dir_rel = f"co/data_source/{nom}/all_tables"
    arc = f"{nom}_all_tables.yaml"
    R_ = ["#!/usr/bin/env bash",
          f"# gen_ingestas v{VERSION} · {hoy} · {nom}: retira del repo de artifacts la ingesta de PRUEBA DE CONEXIÓN ({dir_rel}).",
          "# La prueba es PERMANENTE (validación diaria de la conexión en dev y prd): este script solo se usa si se decide retirarla.",
          f"# Solo si el YAML lleva la marca {PRUEBA_TAG}. Se ejecuta en una rama de trabajo (se niega en {', '.join(PROTEGIDAS)}). No hace commit.",
          "set -euo pipefail", 'cd "$(dirname "$0")"', f'ART="${{ART:-{rel}}}"']
    R_ += _guarda_rama("ART")
    R_ += [f'D={_sh(dir_rel)}', f'Y="$D/{arc}"',
           'if [ ! -f "$ART/$Y" ]; then echo "Nada que retirar: no existe $Y en la rama \'$b\'"; exit 0; fi',
           f"if ! head -n 1 \"$ART/$Y\" | grep -Eq '^# {MARCA}: .*\\| {PRUEBA_TAG} \\|'; then "
           f'echo "ERROR: $Y no lleva la marca {PRUEBA_TAG}: no es una prueba generada, no se retira"; exit 1; fi',
           'otros=$(git -C "$ART" ls-files -- "$D" | grep -vxF "$Y" || true)',
           'if [ -n "$otros" ]; then echo "ERROR: $D tiene otros archivos versionados además de la prueba: no se retira"; exit 1; fi',
           'if git -C "$ART" ls-files --error-unmatch -- "$Y" >/dev/null 2>&1; then',
           '  git -C "$ART" rm -r -q -- "$D"',
           '  echo "Retirado $D (git rm). Revisar git status, commitear y PR."',
           "else",
           '  rm -f "$ART/$Y" && rmdir "$ART/$D" 2>/dev/null || true',
           '  echo "Retirado $D del disco (no estaba versionado)."',
           "fi"]
    (out_root / "retirar_prueba_conexion.sh").write_text("\n".join(R_) + "\n", encoding="utf-8", newline="\n")

def _hm(cron):
    """(hora, minuto) de un cron de 5 campos con minuto y hora enteros simples y `* * *` en el resto; None si no cumple."""
    m = re.fullmatch(r"([0-9]{1,2}) ([0-9]{1,2}) \* \* \*", " ".join(str(cron or "").split()))
    if not m:
        return None
    mi, h = int(m.group(1)), int(m.group(2))
    return (h, mi) if mi <= 59 and h <= 23 else None

def primera_extraccion(schedules):
    """El cron simple más temprano (hora*60 + minuto) de los schedules, normalizado en espacios; None si ninguno es simple."""
    ok = [(h * 60 + m, " ".join(str(s).split())) for s in schedules for hm in [_hm(s)] if hm for h, m in [hm]]
    return min(ok)[1] if ok else None

def cron_prueba(schedules, minutos_antes=None):
    """Schedule de la prueba de conexión: `minutos_antes` (por defecto PRUEBA_MINUTOS_ANTES) minutos antes de la extracción más
    temprana del sistema, módulo 24 h, sin ceros a la izquierda: `M H * * *`. Con PRUEBA_SCHEDULING no vacío se devuelve ese cron.
    Sin ningún schedule simple (minuto y hora enteros, `* * *`) -> '' (el aviso lo da ESTADO.md)."""
    if (PRUEBA_SCHEDULING or "").strip():
        return PRUEBA_SCHEDULING.strip()
    primera = primera_extraccion(schedules)
    if primera is None:
        return ""
    h, m = _hm(primera)
    t = (h * 60 + m - int(PRUEBA_MINUTOS_ANTES if minutos_antes is None else minutos_antes)) % 1440
    return f"{t % 60} {t // 60} * * *"

def _prueba_en_copia(info):
    """La prueba va en copiar_a_repos.sh solo si su tabla base quedó LISTO (sus llaves son un subconjunto de las de la base)."""
    return bool(info) and info.get("estado") == "generada" and info.get("est_tabla") == "LISTO"

def escribir_prueba(a, maestra, out_root, candidatas, resumen, editados=None, manifiesto=None):
    """Genera out/<sis>/out/artifacts/co/data_source/<nom>/all_tables/<nom>_all_tables.yaml (el árbol principal: sale con
    copiar_a_repos.sh) y retirar_prueba_conexion.sh. candidatas = [(fila de tabla, dict YAML)] de las tablas con YAML, en el orden
    de la lista; la base es la primera LISTO y, si ninguna lo está, la primera. El hash va al manifiesto (una edición manual en
    out/ se conserva). -> info para ESTADO.md y copiar_a_repos.sh."""
    nom = nombre_sis(maestra)
    (out_root / "copiar_prueba_conexion.sh").unlink(missing_ok=True)      # v6.6: la prueba ya no se copia aparte
    if getattr(a, "sin_prueba", False):
        return {"estado": "omitida"}
    if not candidatas:
        return {"estado": "sin_yaml"}
    estado = {r["tabla"]: r["estado"] for r in resumen}
    t, y = next((c for c in candidatas if estado.get(f"{c[0]['sistema']}_{c[0]['tabla']}") == "LISTO"), candidatas[0])
    scheds = [c[0].get("scheduling") for c in candidatas]
    sched, primera = cron_prueba(scheds), primera_extraccion(scheds)
    forzado = bool((PRUEBA_SCHEDULING or "").strip())
    p, consulta, columnas, aviso = yaml_prueba(y, t, sched)
    cipher = t["extractor"] == "cipher"
    dest = out_root / "artifacts/co/data_source" / nom / "all_tables" / f"{nom}_all_tables.yaml"
    marca = f"# {MARCA}: {GEN} | {PRUEBA_TAG} | {date.today().isoformat()} | {nom.upper()}.all_tables\n"
    files, cons = aplicar_ediciones([(dest, marca + dump(p))], out_root, editados or {}, manifiesto if manifiesto is not None else {})
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(files[0][1], encoding="utf-8", newline="\n")
    escribir_scripts_prueba(a, out_root, nom)
    key = f"{t['sistema']}_{t['tabla']}"
    info = {"estado": "generada", "nom": nom, "ruta": dest.relative_to(out_root).as_posix(),
            "destino": f"co/data_source/{nom}/all_tables/{nom}_all_tables.yaml",
            "tabla": f"{nom}_{t['tabla']}", "est_tabla": estado.get(key, ""), "variante": "cipher" if cipher else "jdbc",
            "tecnologia": (t.get("tecnologia") or "").strip().lower() or "-", "consulta": consulta,
            "columnas": columnas if cipher else "", "dag": f"{PREFIJO_DAG}{nom}_all_tables-extraction", "aviso": aviso,
            "scheduling": sched, "forzado": forzado, "primera": primera or "", "minutos_antes": PRUEBA_MINUTOS_ANTES,
            "editado": bool(cons)}
    info["en_copia"] = _prueba_en_copia(info)
    return info

def seccion_prueba(maestra, info):
    """Sección ESTADO.md «Prueba de conexión» (ingesta permanente en dev y prd desde v6.7)."""
    L = ["", "## Prueba de conexión", ""]
    est = (info or {}).get("estado")
    if est == "omitida":
        return L + ["Omitida (`--sin-prueba`): no se generó la ingesta de prueba de conexión."]
    if est != "generada":
        return L + ["No se generó: ninguna tabla del sistema tiene YAML (sin columnas o sin fuente). Se genera con la primera tabla "
                    "LISTO que tenga YAML o, si ninguna lo está, con la primera que lo tenga (aunque esté en BORRADOR)."]
    nom = info["nom"]
    ext = "avro" if info["variante"] == "cipher" else "CSV"
    if info.get("forzado"):
        sch = f"`{info['scheduling']}` (fijado por configuración: `prueba_conexion.scheduling` / `PRUEBA_SCHEDULING`)"
    elif info.get("scheduling"):
        sch = (f"`{info['scheduling']}` ({info.get('minutos_antes')} min antes de la primera extracción del sistema, "
               f"{info.get('primera')} UTC)")
    else:
        sch = ("sin definir: ninguna tabla del sistema tiene un schedule simple (minuto y hora enteros, `* * *`). "
               "AVISO: definir el horario (`prueba_conexion.scheduling` en `in/cliente.yaml` o `PRUEBA_SCHEDULING`); "
               "mientras tanto el DAG solo corre manual")
    if info.get("en_copia"):
        copia = "sí (la tabla base está LISTO; el script no pisa un `all_tables` del repo que no lleve la marca `prueba-conexion`)"
    else:
        copia = (f"no (la tabla base `{info['tabla']}` está en {info['est_tabla'] or '-'}, no LISTO: sus llaves aún no están "
                 "completas; queda solo en `out/` hasta que la base pase a LISTO)")
    L += ["Ingesta permanente en dev y prd: valida cada día la conexión, el secreto, la cuenta de servicio, el bucket de landing y los "
          "tópicos. Consulta el diccionario del motor, no toca tablas de negocio y no dispara lnd-to-transformed (regex que nunca "
          "coincide, raw y dbt en `na`).", "",
          f"- Archivo generado: `out/{maestra}/out/{info['ruta']}`" + (" (editado a mano: se conserva)" if info.get("editado") else ""),
          f"- Destino en el repo de artifacts: `{info['destino']}`",
          f"- Tabla base: `{info['tabla']}` ({info['est_tabla'] or '-'}) · variante {info['variante']} · tecnología {info['tecnologia']}",
          f"- Consulta: `{info['consulta']}`" + (f" · columnas: `{info['columnas']}`" if info["columnas"] else ""),
          f"- Scheduling: {sch}",
          f"- DAG: `{info['dag']}`",
          f"- En `copiar_a_repos.sh`: {copia}"]
    if info["aviso"]:
        L.append(f"- AVISO: {info['aviso']}")
    L += ["", "Pruebas de calidad (artifacts): `structure` exige que `raw_schema` apunte a un JSON y la prueba lleva `na`. Mientras "
          "la prueba no tenga excepción propia, registrar en `ci/checks/known_debt.txt` la línea "
          f"`file:{info['destino']}` (con ticket), en el mismo fin de línea del archivo."]
    L += ["", "Validación:", "",
          f"- Verificar cada día que llegue a landing el {ext} de `{nom}_all_tables`. Si llega, conexión, secreto, cuenta de servicio y "
          "bucket están OK y un problema en una tabla está en su consulta o en los permisos sobre ella.",
          "- Si no llega: revisar conectividad, lista blanca, secreto, cuenta de servicio o bucket.",
          "- Nota: el DAG de extracción queda en verde aunque no llegue el archivo (solo publica el mensaje): la alerta por archivo "
          "faltante está pendiente.", "",
          f"Es permanente: `bash out/{maestra}/out/retirar_prueba_conexion.sh` (en una rama de trabajo; solo retira si el YAML lleva la "
          f"marca `{PRUEBA_TAG}`) se usa únicamente si se decide retirarla."]
    return L


def escribir_copia(a, maestra, out_root, resumen, prueba=None):
    """out/<sis>/out/copiar_a_repos.sh: copia a las ramas de trabajo las tablas LISTO, la prueba de conexión (si su tabla base
    está LISTO; no pisa un all_tables del repo sin su marca) y los archivos compartidos
    (variables.yaml, dbt_project.yml, __sources.yml). El generador no lo ejecuta: lo corre el usuario cuando decide.
    Se niega a copiar si artifacts o data-models están en una rama protegida o sin rama."""
    rel = lambda r: Path(os.path.relpath(r, out_root)).as_posix()
    listos = [r for r in resumen if r["estado"] == "LISTO"]
    arch = [x for r in listos for x in r["archivos"]]
    prueba_ok = bool(listos) and _prueba_en_copia(prueba)
    compartidos = [x for x in ("artifacts/co/variables.yaml", "models/dbt_project.yml") if (out_root / x).exists()]
    compartidos += sorted(f.relative_to(out_root).as_posix()               # todo __sources.yml generado (nuevas tablas o renombre)
                          for f in (out_root / "models").rglob("__sources.yml"))
    L = ["#!/usr/bin/env bash",
         f"# gen_ingestas v{VERSION} · {date.today().isoformat()} · {nombre_sis(maestra)}: copia a las ramas de trabajo SOLO las tablas LISTO",
         f"# ({len(listos)} de {len(resumen)}){' + la prueba de conexión (tabla base LISTO)' if prueba_ok else ''}. "
         "Revisar ESTADO.md antes. Después: git status, git diff, run_checks.py.",
         "set -euo pipefail", 'cd "$(dirname "$0")"',
         f'ART="${{ART:-{rel(a.artifacts)}}}"', f'MOD="${{MOD:-{rel(a.models)}}}"',
         'for r in "$ART" "$MOD"; do',
         '  b=$(git -C "$r" branch --show-current)',
         '  case "$b" in ' + "|".join(PROTEGIDAS) + '|"") echo "ERROR: $r está en la rama \'$b\': cambiar a la rama de trabajo"; exit 1;; esac',
         '  echo "$r → $b"', "done"]
    if not listos:
        L += ['echo "Ninguna tabla LISTO: no se copia nada (ver ESTADO.md)"']
    def cp(x):
        sub, _, resto = x.partition("/")
        dst = f'"${{{"ART" if sub == "artifacts" else "MOD"}}}/{resto}"'
        return [f'mkdir -p "$(dirname {dst})" && cp "{x}" {dst}']
    for x in arch:
        L += cp(x)
    if prueba_ok:                                  # la prueba de conexión no pisa un all_tables del repo que no sea una prueba generada
        ruta_pc = prueba["ruta"]
        L += [f'pc="${{ART}}/{ruta_pc.partition("/")[2]}"', 'pc_ok=1',
              'if [ -f "$pc" ]; then',
              f'  case "$(head -n 1 "$pc")" in *{MARCA}*{PRUEBA_TAG}*) ;; *) pc_ok=0;; esac',
              'fi',
              'if [ "$pc_ok" = 1 ]; then',
              f'  mkdir -p "$(dirname "$pc")" && cp "{ruta_pc}" "$pc"',
              'else',
              f'  echo "AVISO: $pc ya existe en el repo y su primera línea no lleva {MARCA} + {PRUEBA_TAG}: no se pisa"',
              'fi']
    for x in (compartidos if listos else []):
        L += cp(x)
    if listos:
        L += [f'echo "Copiados {len(arch)} archivos de {len(listos)} tablas LISTO'
              + (' + 1 de prueba de conexión (si no hay una propia en el repo)' if prueba_ok else '')
              + f' + {len(compartidos)} compartidos. '
              'Revisar git diff (variables.yaml, dbt_project.yml y __sources.yml traen todo lo generado del sistema)."']
    f = out_root / "copiar_a_repos.sh"
    f.write_text("\n".join(L) + "\n", encoding="utf-8", newline="\n")


def _git_lista(repo, *args):
    r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True)
    return [x for x in r.stdout.decode("utf-8", "replace").split("\0") if x] if r.returncode == 0 else []


def _tabla_vieja(rel, sis):
    """Tabla a la que pertenece una ruta con el nombre anterior (data_source/<sis>/<tabla>/… o <sis>_<tabla>[_hist|_current])."""
    partes = rel.split("/")
    if "data_source" in partes and len(partes) > partes.index("data_source") + 2:
        return partes[partes.index("data_source") + 2]
    for p_ in reversed(partes):
        m = re.match(rf"(?i){re.escape(sis)}_(.+?)(?:_hist|_current)?(?:\.\w+)?$", p_)
        if m:
            return m.group(1).lower()
    return rel


def candidatos_retiro(a, sis):
    """Archivos con el nombre anterior en la rama de trabajo: (repo, ruta relativa, tabla, retirable, motivo).
    Retirable = existe, está versionado y lleva la marca dl2-gen. Una tabla con algún modelo en origin/<rama productiva> no se retira."""
    rx = re.compile(rf"(?<![A-Za-z0-9]){re.escape(sis)}(?![A-Za-z0-9])", re.I)
    cand = []
    for clave, repo, base in (("ART", a.artifacts, "co/data_source"), ("MOD", a.models, f"{DBT_DIR}/models")):
        for rel in _git_lista(repo, "ls-files", "-z", "--", base):
            if any(rx.search(p_) for p_ in rel.split("/")):
                f = Path(repo) / rel
                if "all_tables" in rel.split("/") and es_prueba_conexion(f):
                    continue                       # la prueba de conexión la retira retirar_prueba_conexion.sh
                ok, why = f.is_file() and tiene_marca(f), ""
                if not f.is_file():
                    why = "no existe en la rama"
                elif not ok:
                    why = "sin marca dl2-gen"
                cand.append([clave, rel, _tabla_vieja(rel, sis), ok, why])
    prod = set()
    for clave, rel, tab, ok, _ in cand:
        if clave == "MOD" and subprocess.run(["git", "-C", str(a.models), "cat-file", "-e", f"origin/{RAMA_PRD_DL2}:{rel}"],
                                              capture_output=True).returncode == 0:
            prod.add(tab)
    for c in cand:
        if c[2] in prod and c[3]:
            c[3], c[4] = False, "productivo"
    return cand, sorted(prod)


def escribir_retiro(a, maestra, out_root, resumen):
    """out/<sis>/out/retirar_anteriores.sh: `git rm` de lo generado con el nombre anterior (solo rutas versionadas con marca
    dl2-gen; nunca sin marca ni no versionadas; una tabla con modelo en origin/<rama productiva> queda fuera: requiere aprobación TL).
    Lo ejecuta el usuario en la rama de trabajo (mismas protecciones de rama que copiar_a_repos.sh). Solo si el sistema
    tiene nombre DL 2.0 distinto del código."""
    if nombre_sis(maestra) == maestra:
        return
    cand, prod = candidatos_retiro(a, maestra)
    ok = {(c[0], c[1]) for c in cand if c[3]}
    todos = {c[0]: [] for c in cand}
    for c in cand:
        todos[c[0]].append(c[1])
    untracked = {k: set(_git_lista(r_, "ls-files", "-z", "-o", "--exclude-standard")) for k, r_ in (("ART", a.artifacts), ("MOD", a.models))}
    rx = re.compile(rf"(?<![A-Za-z0-9]){re.escape(maestra)}(?![A-Za-z0-9])", re.I)
    cmds, rutas_, vistos = [], [], set()
    def rm(clave, d):
        """git rm -r del directorio d si TODO lo versionado debajo es retirable y no hay archivos sin versionar; si no, baja un nivel."""
        bajo = [x for x in todos.get(clave, []) if x.startswith(d + "/")]
        if bajo and all((clave, x) in ok for x in bajo) and not any(u.startswith(d + "/") for u in untracked[clave]):
            cmds.append((clave, "rm -r", d))
            return
        hijos = sorted({x[len(d) + 1:].split("/")[0] for x in bajo})
        for h in hijos:
            ruta = f"{d}/{h}"
            if ruta in todos.get(clave, []):
                if (clave, ruta) in ok:
                    cmds.append((clave, "rm", ruta))
            else:
                rm(clave, ruta)
    for clave, repo, base in (("ART", a.artifacts, "co/data_source"), ("MOD", a.models, f"{DBT_DIR}/models")):
        tops = sorted({"/".join(x.split("/")[:next(i for i, p_ in enumerate(x.split("/")) if rx.search(p_)) + 1])
                       for x in todos.get(clave, [])})
        for t_ in tops:
            if t_ in todos.get(clave, []):                       # el propio archivo lleva el nombre anterior
                if (clave, t_) in ok:
                    cmds.append((clave, "rm", t_))
            else:
                rm(clave, t_)
    omit = sorted({(c[1], c[4]) for c in cand if not c[3] and c[4] != "productivo"})
    nombres_prod = sorted({c[2] for c in cand if c[4] == "productivo"})
    llaves_ren = {k for k, _ in a.ren["llaves"] + a.ren.get("dbt_llaves", [])}
    huerfanos = []                    # quedan con el nombre anterior y usan llaves que ahora se renombran: se rompen al copiar variables.yaml
    for clave, rel_, _ in ((c[0], c[1], c[4]) for c in cand if not c[3] and c[4] != "productivo"):
        f = Path(a.artifacts if clave == "ART" else a.models) / rel_
        try:
            txt = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if any(re.search(TOKEN(k), txt) for k in llaves_ren):
            huerfanos.append(rel_)
    a.ren["retiro"] = {"rutas": [f"{'artifacts' if k == 'ART' else 'models'}/{d}" for k, _, d in cmds],
                       "productivo": nombres_prod, "omitidos": omit, "huerfanos": sorted(huerfanos)}
    rel = lambda r: Path(os.path.relpath(r, out_root)).as_posix()
    q = lambda x: "'" + x.replace("'", "'\\''") + "'"
    L = ["#!/usr/bin/env bash",
         f"# gen_ingestas v{VERSION} · {date.today().isoformat()} · {nombre_sis(maestra)}: retira de la rama de trabajo lo generado con el",
         f"# nombre anterior ({maestra}). Solo rutas versionadas con marca {MARCA}; ejecutar DESPUÉS de copiar_a_repos.sh.",
         "# Revisar con git status / git diff --cached antes de commitear. No hace commit.",
         "set -euo pipefail", 'cd "$(dirname "$0")"',
         f'ART="${{ART:-{rel(a.artifacts)}}}"', f'MOD="${{MOD:-{rel(a.models)}}}"',
         'for r in "$ART" "$MOD"; do',
         '  b=$(git -C "$r" branch --show-current)',
         '  case "$b" in ' + "|".join(PROTEGIDAS) + '|"") echo "ERROR: $r está en la rama \'$b\': cambiar a la rama de trabajo"; exit 1;; esac',
         '  echo "$r → $b"', "done"]
    for t_ in nombres_prod:
        L.append(f'echo "AVISO: {t_}: productivo: requiere aprobación TL (no se retira)"')
    if not cmds:
        L.append('echo "Nada que retirar (ninguna ruta versionada con marca y el nombre anterior)"')
    for clave, modo, d in cmds:
        L.append(f'git -C "${{{clave}}}" {modo} -q -- {q(d)}')
    if cmds:
        L.append(f'echo "Retiradas {len(cmds)} rutas del nombre anterior. Revisar git status y commitear."')
    (out_root / "retirar_anteriores.sh").write_text("\n".join(L) + "\n", encoding="utf-8", newline="\n")


# ----------------------------------------------------------------------------- lista de trabajo
BASE_LISTA = ["tabla_origen", "sistema_origen", "dominio", "subdominio"]
COLS_LISTA = BASE_LISTA + ["extractor"]   # extractor: cipher | jdbc (se lee en memoria, no se copia)
# v6.8 (H1): encabezado de Excel (Tabla de origen ; Sistema de origen ; ... ; Extractor (JDBC | CIPHER)) -> nombres canónicos
ALIAS_LISTA = {"tabla_de_origen": "tabla_origen", "sistema_de_origen": "sistema_origen", "extractor_jdbc_cipher": "extractor"}


def leer_lista(path):
    """Filas de in/new_migration_tables_to_dl2.csv con las columnas canónicas; acepta el encabezado de Excel sin
    reescribir el archivo. Si vienen ambos nombres, gana el canónico."""
    out = []
    for r in read_csv(path):
        canon = {ALIAS_LISTA[k]: v for k, v in r.items() if k in ALIAS_LISTA}
        out.append({**canon, **{k: v for k, v in r.items() if k not in ALIAS_LISTA}})
    return out


def preparar_sistema(sis, filas_lista):
    """Sincroniza out/<sis>/in/<sis>_tablas.csv con la lista: agrega tablas nuevas, actualiza los 4 campos base y
    retira las que salieron de la lista (lo manual de las que siguen se conserva)."""
    R = rutas(sis)
    R["in"].mkdir(parents=True, exist_ok=True)
    R["out"].mkdir(parents=True, exist_ok=True)
    prev = {r["tabla_origen"].upper(): r for r in leer_tablas(R["tablas"])}   # lee el formato anterior sin perder lo manual
    filas = []
    for b in filas_lista:
        r = dict(prev.get(b["tabla_origen"].upper(), {}))
        r.update({k: b.get(k, "") for k in BASE_LISTA})
        filas.append(r)
    escribir_tablas(R["tablas"], filas)


def procesar(a, maestra):
    print(f"\n== {maestra} ==")
    R = rutas(maestra)
    a.tablas_pos, a.tablas, a.maestra = str(R["tablas"]), None, maestra
    cmd_scan(a)                                                   # 1. tablas, columnas, policy tags, estado_gobierno
    envs = envs_dl2(a.artifacts)
    propuestas, estado_conn = paso_conexiones(a, maestra, envs)   # 2. conexión DL 1.0 -> llaves DL 2.0
    with contextlib.redirect_stdout(io.StringIO()):
        cmd_scan(a)                                               # 3. re-scan con conexión resuelta (idempotente)
        resumen = paso_render(a, maestra, propuestas, estado_conn)  # 4. out/ + variables + estado
    print(f"  resultado: {sum(r['estado'] == 'LISTO' for r in resumen)} listas · "
          f"{sum(r['estado'] == 'BORRADOR' for r in resumen)} borrador · "
          f"{sum(r['estado'] == 'SIN FUENTE' for r in resumen)} sin fuente → out/{maestra}/out/ESTADO.md")
    print(f"  copia a la rama de trabajo (cuando decidas): bash out/{maestra}/out/copiar_a_repos.sh")
    if (getattr(a, "prueba", None) or {}).get("en_copia"):
        print("  prueba de conexión permanente (dev y prd): incluida en copiar_a_repos.sh (tabla base LISTO)")


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Generador DL 2.0: procesa in/new_migration_tables_to_dl2.csv por sistema origen y deja en "
                    "out/<sistema>/in (CSV de trabajo) y out/<sistema>/out (ESTADO.md y archivos con rutas de los repos).")
    p.add_argument("sistemas", nargs="*", help="sistemas origen a procesar (por defecto todos los de la lista)")
    p.add_argument("--apply", action="store_true", help=argparse.SUPPRESS)   # retiradas: el script no escribe repos
    p.add_argument("--vars", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--adoptar", action="store_true", help="reemplaza tags existentes por el del catálogo si es más restrictivo")
    p.add_argument("--force", action="store_true", help="normaliza también modelos existentes sin marca")
    p.add_argument("--audit", action="store_true", help="solo cuenta archivos con y sin marca en los repos")
    p.add_argument("--sin-prueba", action="store_true", dest="sin_prueba",
                   help="no genera la ingesta de prueba de conexión (por defecto se genera una por sistema)")
    p.add_argument("--catalogo")
    p.add_argument("--repos", help="carpeta que contiene los 3 repos")
    for k in ("artifacts", "models", "ingdfw"):
        p.add_argument(f"--{k}")
    a = p.parse_args()
    if not CLIENTE_YAML.exists():
        sys.exit("Falta in/cliente.yaml (valores del cliente). Copiar cliente.ejemplo.yaml a in/cliente.yaml, "
                 "completarlo localmente y volver a correr. Ese archivo no se versiona.")
    if a.apply or a.vars:
        sys.exit("--apply y --vars se retiraron: el script no escribe en los repos. Todo queda en out/<sistema>/out/; "
                 "para copiar las tablas LISTO a la rama de trabajo: bash out/<sistema>/out/copiar_a_repos.sh")
    a.tablas_pos = a.tablas = None
    IN.mkdir(exist_ok=True)
    OUT.mkdir(exist_ok=True)
    if a.audit:
        repos(a)
        cmd_audit(a)
        sys.exit(0)
    if not LISTA.exists():
        write_csv(LISTA, [], COLS_LISTA)
        sys.exit(f"in/{LISTA.name} creado: pegar {';'.join(COLS_LISTA)} y volver a correr")
    grupos = defaultdict(list)
    lista = [r for r in leer_lista(LISTA) if r.get("tabla_origen")]
    a.extractores = {r["tabla_origen"].upper(): r.get("extractor", "") for r in lista}
    if lista and "extractor" not in lista[0]:
        print(f"  aviso: in/{LISTA.name} sin columna extractor (cipher | jdbc): se usa la variante existente o jdbc")
    for r in lista:
        grupos[code(r.get("sistema_origen", "")) or "sin_sistema"].append(r)
    pedidos = [codigo_sis(norm(x)) for x in a.sistemas] or sorted(grupos)     # acepta el código (admiprod) o el nombre (admisiones)
    faltan = [s for s in pedidos if s not in grupos]
    if faltan:
        print(f"  sin tablas en in/{LISTA.name}: {', '.join(faltan)}")
    for sis in (s for s in pedidos if s in grupos):
        preparar_sistema(sis, grupos[sis])
        procesar(a, sis)
