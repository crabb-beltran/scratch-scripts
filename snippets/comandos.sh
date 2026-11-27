# repo: <repo-artifacts> · rama auxiliar <rama-trabajo>-dev
# Reemplazar antes de correrlo: <ruta-repos> · <repo-artifacts> · <rama-trabajo> · <sis> · <carpeta-1..3> (carpetas de la versión anterior que se retiran en data_source/<sis>)
cd "<ruta-repos>/<repo-artifacts>"
git fetch -q origin
git switch -q -c <rama-trabajo>-dev origin/co-dev --no-track
git merge --no-ff --no-commit <rama-trabajo> 2>&1 | grep -iE 'conflict|automatic' 
git rm -rq co/data_source/<sis>/<carpeta-1> co/data_source/<sis>/<carpeta-2> co/data_source/<sis>/<carpeta-3>
git diff --name-only --diff-filter=U
grep -nE '^(<<<<<<<|=======|>>>>>>>)|^\s*# <PREFIJO>' co/variables.yaml


grep -cE '^(<<<<<<<|=======|>>>>>>>)' co/variables.yaml ci/checks/known_debt.txt
python ci/checks/run_checks.py --base origin/co-dev 2>&1 | grep -E '^\[|^\s+x' | cut -c1-120
git add -A && git commit -qm "chore(ingesta): integra <sis> en co-dev y retira la version anterior" && git push -qu origin HEAD
