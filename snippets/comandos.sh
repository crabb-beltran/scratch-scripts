# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
git add -A co/data_source/bup/dim_cod_contraparte co/data_source/bup/all_columns
git commit -q -m "chore(ingesta): retira plantillas de peru sin uso en bup (dim_cod_contraparte, all_columns)"
git add co/data_source/bup co/variables.yaml ci/checks/known_debt.txt
git commit -q -m "feat(ingesta): incorpora bup por cipher con 15 tablas snapshot y prueba de conexion"
git status --short | wc -l
git log --oneline -3
python ci/checks/run_checks.py --base origin/co-dev --base-ref co-dev 2>&1 | grep -E '^\[|^\s+x|^Resultado'
