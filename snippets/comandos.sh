# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
cd ../datope-fif-bfaco-artifacts
git switch -q feature/DL2-FIFDF-89514-ingesta-bup && git pull -q --ff-only && git status -sb | head -1
grep -nE '^\s*(co-dev|co-prd):|^\s*BUP_(HASHICORP_SECRET_(PATH|USER)|SECRET_HOST):' co/variables.yaml \
 | awk -F': ' '{v=$2; gsub(/["\047 ]/,"",v); print $1": "(v=="" ? "(vacío)" : "(valor, " length(v) " car.)")}'
