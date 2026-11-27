# repos: artifacts y data-models · rama <rama-trabajo>
# Reemplazar antes de correr: <ruta-scratch> · <repos-raiz> · <repo-artifacts> · <repo-data-models> · <sis> · <dbt-proyecto> · <ticket>
cd "<ruta-scratch>/scripts"
O=out/<sis>/out; R="../../<repos-raiz>"
A="$R/<repo-artifacts>"; M="$R/<repo-data-models>"
mkdir -p $A/co/data_source/<sis> && cp -r $O/artifacts/co/data_source/<sis>/. $A/co/data_source/<sis>/
cp -r $O/models/<dbt-proyecto>/. $M/<dbt-proyecto>/
[ -f $O/models/dbt_project.yml ] && echo "AVISO: hay vars dbt nuevas: revisar out/<sis>/out/models/dbt_project.yml"
for X in $A $M; do echo "== $X"; git -C $X status -s; done
cd $A && grep -q '<sis>/all_tables' ci/checks/known_debt.txt || echo "file:co/data_source/<sis>/all_tables/<sis>_all_tables.yaml  # <ticket> prueba de conexion" >> ci/checks/known_debt.txt
python ci/checks/run_checks.py --base origin/co-prd 2>&1 | grep -E '^\[|^\s+x' | cut -c1-120
cd $M && python ci/checks/run_checks.py --base origin/co-prd 2>&1 | grep -E '^\[|^\s+x' | cut -c1-120

# Continuación (mismas variables $A y $M del bloque anterior; <sis> = sistema, <n> y <descripcion> = texto del commit)
cd $A && git add -A co/data_source/<sis> co/variables.yaml ci/checks/known_debt.txt && git commit -qm "feat(ingesta): agrega ingesta <sis> generada (<n> tablas <descripcion>)" && git push -qu origin HEAD && git log --oneline -1
cd $M && git add -A <dbt-proyecto> && git commit -qm "feat(modelos): agrega modelos std de <sis> generados" && git push -qu origin HEAD && git log --oneline -1
