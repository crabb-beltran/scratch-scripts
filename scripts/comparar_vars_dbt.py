#!/usr/bin/env python3
"""Compara vars de dbt_project.yml (archivo de trabajo) contra `git show <ref>:<archivo>`.

Verifica si cada clave movida/cambiada entre `vars` comunes y `vars.ambientes.<env>`
conserva el mismo valor efectivo por ambiente. NUNCA imprime valores, solo nombres de claves.

Uso (desde la raiz del repo): python comparar_vars_dbt.py [archivo=dbt_project.yml] [ref=HEAD]
Valor efectivo(K, E) = ambientes[E][K] si existe; si no, vars[K] (comun); si no, FALTA.
Salida: 0 = OK, 1 = REVISAR (hay CAMBIA o QUITADA), 2 = error (archivo ausente / YAML invalido).
"""
import subprocess, sys
import yaml

FALTA = object()


def salir(msg):
    print(f"ERROR: {msg}")
    sys.exit(2)


def cargar(texto, origen):
    try:
        d = yaml.safe_load(texto) or {}
    except yaml.YAMLError as e:
        m = getattr(e, "problem_mark", None)  # no se imprime e: podria incluir valores
        salir(f"YAML invalido en {origen}" + (f" (linea {m.line + 1})" if m else ""))
    v = d.get("vars") if isinstance(d, dict) else None
    v = v if isinstance(v, dict) else {}
    amb = v["ambientes"] if isinstance(v.get("ambientes"), dict) else {}
    amb = {e: (b if isinstance(b, dict) else {}) for e, b in amb.items()}
    return {k: x for k, x in v.items() if k != "ambientes"}, amb


def efectivo(com, amb, env, k):
    return amb.get(env, {}).get(k, com.get(k, FALTA))


def ubic(com, amb, k):
    c, a = k in com, any(k in b for b in amb.values())
    return {(1, 0): "comun", (0, 1): "ambientes", (1, 1): "comun+ambientes"}.get((c, a), "-")


def estado(a, b):
    if a is FALTA or b is FALTA:
        return None if a is b else "NUEVA" if a is FALTA else "QUITADA"
    return "IGUAL" if type(a) is type(b) and a == b else "CAMBIA"


def main():
    archivo = sys.argv[1] if len(sys.argv) > 1 else "dbt_project.yml"
    ref = sys.argv[2] if len(sys.argv) > 2 else "HEAD"
    r = subprocess.run(["git", "show", f"{ref}:{archivo}"], capture_output=True)  # bytes: evita cp1252 en Windows
    if r.returncode:
        salir(f"no se pudo leer '{archivo}' en la referencia '{ref}' (git show fallo)")
    try:
        actual = open(archivo, encoding="utf-8", newline="").read()
    except OSError:
        salir(f"no se pudo leer el archivo de trabajo '{archivo}'")
    c0, a0 = cargar(r.stdout.decode("utf-8", "replace"), f"{ref}:{archivo}")
    c1, a1 = cargar(actual, archivo)
    envs = sorted(set(a0) | set(a1))
    claves = sorted(set(c0) | set(c1) | {k for b in (*a0.values(), *a1.values()) for k in b})
    cuenta = dict.fromkeys(("IGUAL", "CAMBIA", "NUEVA", "QUITADA"), 0)
    for k in claves:
        u0, u1 = ubic(c0, a0, k), ubic(c1, a1, k)
        est = {e: estado(efectivo(c0, a0, e, k), efectivo(c1, a1, e, k)) for e in envs}
        if u0 == u1 and all(s in (None, "IGUAL") for s in est.values()):
            continue
        for s in est.values():
            if s:
                cuenta[s] += 1
        print(f"{k}  {u0} -> {u1}  " + " ".join(f"{e}={s or '-'}" for e, s in est.items()))
    print("Resumen: " + ", ".join(f"{n}={v}" for n, v in cuenta.items()))
    dup = [k for k in claves if k in c1 and any(k in b for b in a1.values())]
    if dup:
        print(f"AVISO: claves en comun y en bloque de ambiente a la vez: {', '.join(dup)}")
    desb = [k for k in claves if len({k in b for b in a1.values()}) > 1]
    if desb:
        print(f"AVISO: bloques de ambiente con claves distintas (paridad): {', '.join(desb)}")
    malos = cuenta["CAMBIA"] + cuenta["QUITADA"]
    print(f"RESULTADO: REVISAR ({malos} cambios de valor efectivo)" if malos
          else "RESULTADO: OK (sin cambios de valor efectivo)")
    sys.exit(1 if malos else 0)


if __name__ == "__main__":
    main()
