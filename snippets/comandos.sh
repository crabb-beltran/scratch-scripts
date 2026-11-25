# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
git diff --stat | tail -1
git diff -U0 | grep -E '^[+-]\s' | sed -E 's/^([+-])\s*([A-Z_]+):\s*(.*)$/\1 \2 \3/' \
 | awk '{v=$3; gsub(/["\047]/,"",v); print $1, $2, (v=="" ? "(vacío)" : length(v) " car.")}'
python ci/checks/run_checks.py --base origin/co-prd --checks keys,parity,dup,syntax,files,hardcoded 2>&1 | grep -E '^\[|^\s+x' | cut -c1-120
git commit -qam "fix(ingesta): agrega usuario y path del secreto hashicorp de bup" && git log --oneline -1
git merge-tree --write-tree --name-only origin/co-dev HEAD | sed -n '2,10p'
