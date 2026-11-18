#!/usr/bin/env python3
"""Verifica que reordenar un YAML no quitó ni cambió valores existentes.

Uso (desde la raíz del repo):
    python verificar_reorden.py <ruta_archivo> [ref]      # ref por defecto: HEAD

Compara el archivo de trabajo contra `git show <ref>:<ruta>`, aplanando ambos a
{ruta: valor}. Las listas cuyos items son mapeos de una sola llave
(`- co-dev: {...}`) se indexan por esa llave, no por posición. También detecta
llaves duplicadas en el mapeo (PyYAML las colapsa en silencio).
NUNCA imprime valores, solo rutas y conteos.
Códigos de salida: 0 = OK, 1 = REVISAR, 2 = archivo no existe en el ref.
"""
import os
import re
import subprocess
import sys

import yaml

PLACEHOLDER = re.compile(r"(?<![{$])\{([A-Z][A-Z0-9_\-]*)\}(?!\})")


class DupLoader(yaml.SafeLoader):
    """SafeLoader que registra llaves duplicadas dentro del mismo mapeo."""
    duplicadas = []

    def construct_mapping(self, node, deep=False):
        vistas = set()
        for k_node, _ in node.value:
            try:
                k = self.construct_object(k_node, deep=True)
                hash(k)
            except Exception:
                continue
            if k in vistas:
                self.duplicadas.append("%s (línea %d)" % (k, k_node.start_mark.line + 1))
            vistas.add(k)
        return super().construct_mapping(node, deep)


def cargar(texto, loader=yaml.SafeLoader):
    return yaml.load(PLACEHOLDER.sub(r"${\1}", texto), Loader=loader)


def aplanar(nodo, ruta=()):
    """Devuelve {ruta: valor} solo para hojas (contenedores vacíos cuentan como hoja)."""
    if isinstance(nodo, dict) and nodo:
        items = [((str(k),), v) for k, v in nodo.items()]
    elif isinstance(nodo, list) and nodo:
        items = []
        for i, it in enumerate(nodo):
            if isinstance(it, dict) and len(it) == 1:
                (k, v), = it.items()
                items.append(((str(k),), v))
            else:
                items.append((("[%d]" % i,), it))
    else:
        return {"/".join(ruta): nodo}
    out = {}
    for sub, v in items:
        out.update(aplanar(v, ruta + sub))
    return out


def listar(titulo, rutas, tope):
    print("%s: %d" % (titulo, len(rutas)))
    for r in rutas[:tope]:
        print("- " + r)
    if len(rutas) > tope:
        print("  ... y %d más" % (len(rutas) - tope))


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    ruta, ref = argv[1], (argv[2] if len(argv) > 2 else "HEAD")
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    p = subprocess.run(["git", "show", "%s:%s" % (ref, ruta.replace("\\", "/"))],
                       capture_output=True, env=env)
    if p.returncode != 0:
        print("El archivo '%s' no existe en %s (o git falló)." % (ruta, ref))
        return 2
    with open(ruta, encoding="utf-8") as f:
        nuevo_txt = f.read()
    viejo = aplanar(cargar(p.stdout.decode("utf-8")))
    DupLoader.duplicadas = []
    nuevo = aplanar(cargar(nuevo_txt, DupLoader))
    quitadas = sorted(set(viejo) - set(nuevo))
    cambiadas = sorted(k for k in set(viejo) & set(nuevo) if viejo[k] != nuevo[k])
    nuevas = sorted(set(nuevo) - set(viejo))
    dups = DupLoader.duplicadas
    listar("quitadas", quitadas, 50)
    listar("cambiadas", cambiadas, 50)
    listar("nuevas", nuevas, 80)
    listar("duplicadas", dups, 50)
    ok = not (quitadas or cambiadas or dups)
    print("RESULTADO: OK (solo reorden y llaves nuevas)" if ok else
          "RESULTADO: REVISAR (hay llaves quitadas, cambiadas o duplicadas)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
