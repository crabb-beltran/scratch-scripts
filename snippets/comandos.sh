# repo: datope-fif-bfaco-artifacts · rebase en curso (REBASE 2/3)
grep -cE '^(<<<<<<<|=======|>>>>>>>)' co/variables.yaml
grep -nE '^\s*(- *)?(co-dev|co-prd)\s*:|^\s*#\s*BUP|^\s*(BUP_[A-Z_]+|USER_BUP_CREDENTIALS|PASS_BUP_CREDENTIALS)\s*:' co/variables.yaml | awk -F: '{print $1":"$2}'
