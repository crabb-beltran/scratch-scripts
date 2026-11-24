# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
git rebase origin/co-prd
git status --short | grep -vE '^(M |D |A |R )' 
git show origin/co-prd:co/variables.yaml | grep -nE '^\s*(- *)?(co-dev|co-prd)\s*:|^\s*#\s*BUP|^\s*(BUP_[A-Z_]+|USER_BUP_CREDENTIALS|PASS_BUP_CREDENTIALS)\s*:' | awk -F: '{print $1":"$2}'
