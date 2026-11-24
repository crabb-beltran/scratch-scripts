# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
cd /c/dataops/bfa-co/falabella-regulado-datafactory/datope-fif-bfaco-artifacts
git fetch -q origin
git log --oneline HEAD..origin/co-prd | head -5
git merge-tree --write-tree --name-only origin/co-prd HEAD | tail -n +2
git merge-tree --write-tree --name-only origin/co-dev HEAD | tail -n +2
git ls-remote --heads origin feature/DL2-FIFDF-89514-ingesta-bup | wc -l

# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
Y=co/data_source/bup/sttm_customer/bup_sttm_customer.yaml
grep -nE "<[a-z_]+>|_OCI_|secret|hashicorp|external" $Y | sed -E "s/'secret_manager_hashicorp'/<literal-gestor>/"
grep -nE '^\s*(BUP_[A-Z_]+|USER_BUP_CREDENTIALS|PASS_BUP_CREDENTIALS)\s*:' co/variables.yaml | awk -F: '{print $1":"$2}'
