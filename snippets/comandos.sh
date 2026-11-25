# repo: <repo-artifacts> (lectura de la rama del compañero)
# Reemplazar antes de correr: <rama-companero> · <ruta-fuente> (carpeta data_source del sistema) · <prefijos-llaves> (regex de prefijos en variables.yaml) · <patron> · <repo-legacy> · <rama-prd-legacy>
B="origin/<rama-companero>"
for f in $(git ls-tree -r --name-only $B -- "<ruta-fuente>/"); do
  echo "== $f"
  git show "$B:$f" | grep -oiE '"(domain|sub_?domain|source_system|update_type|table_name)"\s*:\s*"[^"]*"|\b(type|extractor_type|variant|strategy|load_type)\s*:\s*\S+|from\s+[A-Za-z0-9_.\[\]]+' | sort -u | head -8
done
git show $B:co/variables.yaml | grep -nE '^\s*(<prefijos-llaves>)_[A-Z_]*:' | sed -E 's/^([0-9]+):\s*([A-Z_]+):.*/\1 \2/'
git -C "../<repo-legacy>" fetch -q origin
git -C "../<repo-legacy>" grep -il "<patron>" "origin/<rama-prd-legacy>" | head
