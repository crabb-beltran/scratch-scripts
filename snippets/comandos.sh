# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup-dev (MERGING)
if grep -qE '^(<<<<<<<|=======|>>>>>>>)' co/variables.yaml ci/checks/known_debt.txt; then echo "QUEDAN MARCADORES: no seguir"; else
  git add co/variables.yaml ci/checks/known_debt.txt
  git commit -q -m "chore(ingesta): integra bup con co-dev y resuelve conflictos en variables y deuda conocida"
  git log --oneline -1
  python ci/checks/run_checks.py --base origin/co-dev --base-ref co-dev 2>&1 | grep -E '^\[|^\s+x|^Resultado'
fi
