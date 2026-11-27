# repo: <repo-data-models> · rama <rama-trabajo>-dev
# Reemplazar antes de correrlo: <repo-data-models> · <rama-trabajo> · <sis>
sed -n '28,48p' dbt_project.yml | sed -E "s/(:\s*).*/\1…/"

grep -cE '^(<<<<<<<|=======|>>>>>>>)' dbt_project.yml
python -c "import yaml;yaml.safe_load(open('dbt_project.yml',encoding='utf-8'));print('yaml OK')"
python ci/checks/run_checks.py --base origin/co-dev 2>&1 | grep -E '^\[|^\s+x' | cut -c1-120
git add -A && git commit -qm "chore(modelos): integra <sis> en co-dev y retira la version anterior" && git push -qu origin HEAD
