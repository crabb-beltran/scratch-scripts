# repo: datope-fif-bfaco-data-models · rama feature/DL2-FIFDF-89514-ingesta-bup
# Desde la raíz de data-models: deja `data_tests: []` en todo bloque data_tests de schema.yml que contenga POR_CONFIRMAR
# (también listas al mismo nivel que la llave). Conserva CRLF/LF por archivo. Después: contar archivos con POR_CONFIRMAR
# y verificar que el diff no tenga cambios solo de EOL.
python - <<'EOF'
import pathlib, re
n = 0
for p in pathlib.Path("datope_bfaco/models").rglob("schema.yml"):
    raw = p.read_bytes(); eol = b"\r\n" if b"\r\n" in raw else b"\n"
    L = raw.decode("utf-8").replace("\r\n", "\n").split("\n")
    out, i, hit = [], 0, False
    while i < len(L):
        m = re.match(r"^(\s*)data_tests:\s*$", L[i])
        if m:
            ind, j = len(m.group(1)), i + 1
            while j < len(L):
                s = L[j]; k = len(s) - len(s.lstrip())
                if s.strip() and (k < ind or (k == ind and not s.lstrip().startswith("- "))):
                    break
                j += 1
            while j > i + 1 and not L[j - 1].strip():   # no tragar líneas en blanco al final del bloque
                j -= 1
            if any("POR_CONFIRMAR" in x for x in L[i:j]):
                out.append(" " * ind + "data_tests: []"); i, hit = j, True; continue
        out.append(L[i]); i += 1
    if hit:
        p.write_bytes("\n".join(out).encode("utf-8").replace(b"\n", eol)); n += 1
print("schema.yml ajustados:", n)
EOF
grep -rl POR_CONFIRMAR datope_bfaco/models | wc -l
diff <(git diff --numstat) <(git diff --numstat --ignore-cr-at-eol)

# ---------------------------------------------------------------------------------------------------------------------
# repo: datope-fif-bfaco-data-models · rama feature/DL2-FIFDF-89514-ingesta-bup
# Completa las descripciones POR_CONFIRMAR de las columnas de auditoría en el schema.yml que quede con POR_CONFIRMAR
# y en out/bup/in/bup_columnas.csv (para que la siguiente corrida del generador las conserve). Conserva EOL del schema.yml.
F=$(grep -rl POR_CONFIRMAR datope_bfaco/models); echo "$F"
python - "$F" <<'EOF'
import sys, re, pathlib, csv
D = {"last_mod_datetime": "Fecha y hora de la última modificación del registro en la fuente",
     "last_mod_user": "Usuario que realizó la última modificación del registro en la fuente",
     "mod_no": "Número de versión de modificación del registro en la fuente"}
p = pathlib.Path(sys.argv[1]); raw = p.read_bytes(); eol = b"\r\n" if b"\r\n" in raw else b"\n"
L = raw.decode().replace("\r\n", "\n").split("\n"); col = None; n = 0
for i, l in enumerate(L):
    m = re.match(r"^\s*- name: '([^']+)'", l)
    if m: col = m.group(1)
    if "description: 'POR_CONFIRMAR'" in l and col in D:
        L[i] = l.replace("'POR_CONFIRMAR'", f"'{D[col]}'"); n += 1
p.write_bytes("\n".join(L).encode().replace(b"\n", eol)); print("modelo:", n)
c = pathlib.Path("../../scratch/scripts/out/bup/in/bup_columnas.csv")
rows = list(csv.DictReader(open(c, encoding="utf-8-sig"), delimiter=";")); k = 0
for r in rows:
    if r["columna"].lower() in D and r["descripcion"] in ("", "POR_CONFIRMAR"):
        r["descripcion"] = D[r["columna"].lower()]; k += 1
w = csv.DictWriter(open(c, "w", newline="", encoding="utf-8-sig"), fieldnames=rows[0].keys(), delimiter=";"); w.writeheader(); w.writerows(rows)
print("columnas.csv:", k)
EOF
grep -rl POR_CONFIRMAR datope_bfaco/models | wc -l
