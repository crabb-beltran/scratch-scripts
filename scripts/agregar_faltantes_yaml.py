#!/usr/bin/env python3
"""Reinserta en un YAML de trabajo los bloques que existen en <ref> y faltan (p. ej. tras resolver un merge con
--theirs), sin mover ni cambiar líneas existentes. Pensado para dbt_project.yml (vars y models:).

Uso (desde la raíz del repo): python agregar_faltantes_yaml.py <archivo> <ref> [--dry-run]
- Toma de `git show <ref>:<archivo>` cada llave faltante cuyo padre sí existe, con todo su bloque (hijos y
  comentarios inmediatamente encima), y la ubica después de su hermana anterior que exista en el archivo de trabajo.
- Omite una llave si el mismo nombre ya existe en otra parte de la misma sección raíz (llave movida a propósito,
  p. ej. de vars comunes a vars.ambientes) y lo informa.
- Conserva CRLF/LF del archivo de trabajo. NUNCA imprime valores. Salida: 0 OK, 2 abortado (no escribe).
"""
import re, subprocess, sys

KEY = re.compile(r"^(\s*)(['\"]?)([^'\"#:\s][^:#]*?)\2\s*:(\s|$)")


def nodos(lines):
    """{ruta (tupla): (inicio, fin)} de llaves de mapeo; fin = última línea no vacía del bloque + 1."""
    out, pila = {}, []          # pila de (indent, ruta, inicio)
    def cerrar(hasta_ind, i):
        while pila and pila[-1][0] >= hasta_ind:
            ind, ruta, ini = pila.pop()
            fin = i
            while fin > ini + 1 and (not lines[fin - 1].strip() or lines[fin - 1].lstrip().startswith("#")):
                fin -= 1
            out[ruta] = (ini, fin)
    for i, l in enumerate(lines):
        s = l.strip()
        if not s or s.startswith("#") or s.startswith("- "):
            continue
        m = KEY.match(l.rstrip("\r\n"))
        if not m:
            continue
        ind = len(m.group(1))
        cerrar(ind, i)
        ruta = (pila[-1][1] if pila else ()) + (m.group(3).strip(),)
        if ruta in out:
            raise ValueError("llave duplicada: " + "/".join(ruta))
        pila.append((ind, ruta, i))
    cerrar(-1, len(lines))
    return out


def main(argv):
    if len(argv) < 2:
        print(__doc__); return 2
    archivo, ref, dry = argv[0], argv[1], "--dry-run" in argv
    r = subprocess.run(["git", "show", f"{ref}:{archivo}"], capture_output=True)
    if r.returncode:
        print(f"ERROR: no existe {archivo} en {ref}"); return 2
    src = r.stdout.decode("utf-8").splitlines(keepends=True)
    raw = open(archivo, "rb").read().decode("utf-8")
    eol = "\r\n" if raw.count("\r\n") * 2 > raw.count("\n") else "\n"
    tgt = raw.splitlines(keepends=True)
    try:
        ns, nt = nodos(src), nodos(tgt)
    except ValueError as e:
        print(f"RESULTADO: ABORTADO - {e}"); return 2
    nombres_raiz = {}
    for ruta in nt:
        nombres_raiz.setdefault(ruta[0], set()).add(ruta[-1])
    ins, agregadas, omitidas = {}, [], []
    for ruta in sorted(ns, key=lambda p: ns[p][0]):
        if ruta in nt or len(ruta) < 2 or ruta[:-1] not in nt:
            continue
        if any(ruta[:k] in [a for a, _ in agregadas] for k in range(1, len(ruta))):
            continue
        if not ruta[-1].startswith("+") and ruta[-1] in nombres_raiz.get(ruta[0], set()):
            omitidas.append("/".join(ruta)); continue
        ini, fin = ns[ruta]
        while ini > 0 and src[ini - 1].lstrip().startswith("#"):
            ini -= 1                                     # comentarios pegados encima
        hermanas = [p for p in ns if p[:-1] == ruta[:-1] and ns[p][0] < ns[ruta][0] and p in nt]
        if hermanas:
            prev = max(hermanas, key=lambda p: ns[p][0])
            pos, ancla = nt[prev][1], prev[-1]
        else:
            pos, ancla = nt[ruta[:-1]][0] + 1, "(inicio de " + ruta[-2] + ")"
        ins.setdefault(pos, []).extend(l.rstrip("\r\n") + eol for l in src[ini:fin])
        agregadas.append((ruta, ancla))
    nuevo = []
    for i in range(len(tgt) + 1):
        if i in ins:
            if nuevo and not nuevo[-1].endswith("\n"):
                nuevo[-1] += eol
            nuevo += ins[i]
        if i < len(tgt):
            nuevo.append(tgt[i])
    it = iter(nuevo)
    if not all(any(o == n for n in it) for o in tgt):
        print("RESULTADO: ABORTADO - una línea existente cambió"); return 2
    try:
        nn = nodos(nuevo)
    except ValueError as e:
        print(f"RESULTADO: ABORTADO - {e}"); return 2
    faltan = [p for p in ns if p not in nn and not any("/".join(p).startswith(o) for o in omitidas)]
    for ruta, ancla in agregadas:
        print(f"  + {'/'.join(ruta)}  (después de {ancla})")
    for o in omitidas:
        print(f"  = {o}  (omitida: ya existe en otra parte de {o.split('/')[0]})")
    if faltan:
        print("AVISO: siguen faltando: " + ", ".join("/".join(p) for p in faltan))
    if agregadas and not dry:
        open(archivo, "wb").write("".join(nuevo).encode("utf-8"))
    print(("(dry-run) " if dry else "") + f"RESULTADO: OK ({len(agregadas)} bloques agregados)" if agregadas
          else "Sin bloques faltantes")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
