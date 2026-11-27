# repos: <repo-artifacts> y <repo-data-models>
# Reemplazar antes de correr: <ruta-repos> · <repo-artifacts> · <repo-data-models> · <rama-companero> · <sis> (carpeta del sistema en data_source) · <PREFIJO> (prefijo de llaves en variables.yaml) · <patron-ticket> (id del ticket en el nombre de rama)
cd "<ruta-repos>/<repo-artifacts>"
git fetch -q --prune origin
B="origin/<rama-companero>"
for r in origin/co-prd origin/co-dev $B; do
  echo "== $r · $(git ls-tree -r --name-only $r -- co/data_source/<sis> | wc -l) archivos"
  git ls-tree -r --name-only $r -- co/data_source/<sis> | sed 's#co/data_source/<sis>/##'
done
echo "-- commits de la rama fuera de co-dev:"; git log --oneline origin/co-dev..$B
git show origin/co-prd:co/variables.yaml | grep -cE '^\s*<PREFIJO>_'
cd "../<repo-data-models>" && git fetch -q --prune origin
git branch -r | grep -i "<patron-ticket>"
for r in origin/co-prd origin/co-dev; do echo "== models $r"; git ls-tree -r --name-only $r | grep -i "<sis>"; done
