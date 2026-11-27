# repo: <repo-data-models>
# Reemplazar antes de correrlo: <ruta-repos> · <repo-data-models> · <rama-trabajo> (la auxiliar es <rama-trabajo>-dev) · <dbt-proyecto> · <dominio> · <modelo-1..3> (carpetas de la versión anterior que se retiran)
cd "<ruta-repos>/<repo-data-models>"
git fetch -q origin
git diff origin/co-prd...origin/<rama-trabajo> -- dbt_project.yml | grep -E '^[+-]\s' | sed -E 's/:.*/: …/'
git switch -q -c <rama-trabajo>-dev origin/co-dev --no-track
git merge --no-ff --no-commit origin/<rama-trabajo> 2>&1 | grep -iE 'conflict|automatic'
git rm -rq <dbt-proyecto>/models/std_<dominio>/<modelo-1> <dbt-proyecto>/models/std_<dominio>/<modelo-2> <dbt-proyecto>/models/std_<dominio>/<modelo-3>
git diff --name-only --diff-filter=U
grep -nE '^(<<<<<<<|=======|>>>>>>>)' dbt_project.yml <dbt-proyecto>/models/*/__sources.yml 2>/dev/null

grep -rcE '^(<<<<<<<|=======|>>>>>>>)' dbt_project.yml <dbt-proyecto>/models | grep -v ':0'
python ci/checks/run_checks.py --base origin/co-dev 2>&1 | grep -E '^\[|^\s+x' | cut -c1-120
git add -A && git commit -qm "chore(modelos): integra <sis> en co-dev y retira la version anterior" && git push -qu origin HEAD
