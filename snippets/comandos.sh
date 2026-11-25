# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
grep -nE '^\S[^:]*:\s*$' co/variables.yaml | head -5
grep -nE '^\s*BUP_(HASHICORP_SECRET_(PATH|USER)|SECRET_HOST):' co/variables.yaml \
 | sed -E 's/^([0-9]+):\s*([A-Z_]+):\s*(.*)$/\1 \2 \3/' \
 | awk '{v=$3; gsub(/["\047]/,"",v); print $1, $2, (v=="" ? "(vacío)" : "(valor, " length(v) " car.)")}'
