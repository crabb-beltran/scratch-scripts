#!/usr/bin/env python3
"""Lleva a dbt_project.yml solo las vars indicadas, con la ubicación y los valores de otra versión (p. ej. co-dev).

Uso: python llevar_vars_dbt.py <objetivo.yml> <referencia.yml> <salida.yml> LLAVE [LLAVE ...]

- objetivo:   dbt_project.yml que se conserva (p. ej. la rama de la PR).
- referencia: versión de donde se toman las llaves (`git show origin/co-dev:dbt_project.yml`).
- salida:     archivo nuevo; el script no modifica el objetivo ni el repo.

Para cada LLAVE: se quita de vars comunes y de vars.ambientes.<env> del objetivo y se escribe donde está en la
referencia (comunes y/o cada ambiente), copiando la línea de la referencia. Se ubica después de la llave anterior
de la referencia que exista en esa sección del objetivo (si no hay, al final de la sección). El resto del archivo
no cambia. Valida que las demás vars queden iguales y que las llevadas tengan el valor efectivo de la referencia.
Respeta el fin de línea del objetivo. Nunca imprime valores.
"""
import re, sys
import yaml

KEY = re.compile(r"^(\s*)['\"]?([\w.\-]+)['\"]?\s*:")


def leer(p):
    raw = open(p, "rb").read()
    nl = "\r\n" if raw.count(b"\r\n") * 2 > raw.count(b"\n") else "\n"
    return raw.decode("utf-8").replace("\r\n", "\n").split("\n"), nl


def fin_bloque(L, i):
    ind = len(L[i]) - len(L[i].lstrip())
    for j in range(i + 1, len(L)):
        if L[j].strip() and not L[j].lstrip().startswith("#") and len(L[j]) - len(L[j].lstrip()) <= ind:
            return j
    return len(L)


def secciones(L):
    """{'comun': (ini, fin), 'dev': (...), 'prd': (...)} con el rango de líneas de cada sección de vars."""
    v = next((i for i, l in enumerate(L) if re.match(r"^vars:\s*(#.*)?$", l)), None)
    if v is None:
        sys.exit("ERROR: sin bloque vars:")
    fv = fin_bloque(L, v)
    ia = next((j for j in range(v + 1, fv) if re.match(r"^\s+ambientes:\s*(#.*)?$", L[j])), None)
    out = {"comun": (v + 1, ia if ia is not None else fv)}
    if ia is not None:
        for j in range(ia + 1, fin_bloque(L, ia)):
            m = re.match(r"^\s+([\w-]+):\s*(#.*)?$", L[j])
            if m and len(L[j]) - len(L[j].lstrip()) > len(L[ia]) - len(L[ia].lstrip()):
                f = fin_bloque(L, j)
                while f > j + 1 and not L[f - 1].strip():
                    f -= 1
                out[m.group(1)] = (j + 1, f)
    return out


def llaves(L, ini, fin, nivel=None):
    """[(llave, índice)] de primer nivel de la sección."""
    out = []
    for i in range(ini, fin):
        m = KEY.match(L[i])
        if m and not L[i].lstrip().startswith("#"):
            ind = len(m.group(1))
            nivel = ind if nivel is None else nivel
            if ind == nivel and m.group(2) != "ambientes":
                out.append((m.group(2), i))
    return out


def vars_efectivas(L):
    d = yaml.safe_load("\n".join(L)) or {}
    v = dict(d.get("vars") or {})
    amb = v.pop("ambientes", {}) or {}
    return v, amb


def main(obj, ref, salida, claves):
    L, nl = leer(obj)
    R, _ = leer(ref)
    claves = set(claves)
    sr = secciones(R)
    # 1) quitar las llaves del objetivo (de abajo hacia arriba)
    so = secciones(L)
    quitar = sorted((i for s, (a, b) in so.items() for k, i in llaves(L, a, b) if k in claves), reverse=True)
    for i in quitar:
        del L[i]
    # 2) insertar donde están en la referencia
    puestas = []
    for sec in ("comun", "dev", "prd"):
        if sec not in sr:
            continue
        lr = llaves(R, *sr[sec])
        for pos, (k, ir) in enumerate(lr):
            if k not in claves:
                continue
            so = secciones(L)
            if sec not in so:
                sys.exit(f"ERROR: el objetivo no tiene la sección {sec}")
            a, b = so[sec]
            lo = dict(llaves(L, a, b))
            prev = next((lo[p] for p, _ in reversed(lr[:pos]) if p in lo), None)
            donde = prev + 1 if prev is not None else b
            L.insert(donde, R[ir])
            puestas.append(f"{k}→{sec}")
    # 3) validación
    c0, a0 = vars_efectivas(leer(obj)[0])
    c1, a1 = vars_efectivas(L)
    cr, ar = vars_efectivas(R)
    envs = sorted(set(a0) | set(a1))
    ef = lambda c, a, e, k: a.get(e, {}).get(k, c.get(k, "<FALTA>"))
    todas = (set(c0) | set(c1) | {k for b in (*a0.values(), *a1.values()) for k in b})
    problemas = []
    for k in sorted(todas):
        for e in envs:
            if k in claves:
                if ef(c1, a1, e, k) != ef(cr, ar, e, k):
                    problemas.append(f"{k} ({e}): valor efectivo distinto al de la referencia")
            elif ef(c0, a0, e, k) != ef(c1, a1, e, k):
                problemas.append(f"{k} ({e}): cambió y no estaba en la lista")
    for k in claves:
        if k in c1 and any(k in b for b in a1.values()):
            problemas.append(f"{k}: queda en comunes y en ambientes a la vez")
    open(salida, "w", encoding="utf-8", newline="").write(nl.join(L))
    print(f"quitadas del objetivo: {len(quitar)} líneas · puestas: {', '.join(puestas) or '-'}")
    print(f"fin de línea: {'CRLF' if nl == chr(13) + chr(10) else 'LF'}")
    for p in problemas:
        print("PROBLEMA: " + p)
    print("OK: solo cambian las llaves indicadas" if not problemas else "REVISAR")
    return 1 if problemas else 0


if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]))
