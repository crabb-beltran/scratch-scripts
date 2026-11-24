grep -cE '^(<<<<<<<|=======|>>>>>>>)' co/variables.yaml ci/checks/known_debt.txt
grep -c 'all_tables' ci/checks/known_debt.txt
grep -nE '^\s*(- *)?(co-dev|co-prd)\s*:|^\s*#\s*BUP|^\s*(BUP_[A-Z_]+|USER_BUP_CREDENTIALS|PASS_BUP_CREDENTIALS)\s*:' co/variables.yaml | awk -F: '{print $1":"$2}'

# repo: datope-fif-bfaco-artifacts
git switch -c feature/DL2-FIFDF-89514-ingesta-bup-dev origin/co-dev --no-track
git merge --no-ff --no-commit feature/DL2-FIFDF-89514-ingesta-bup
git status --short | grep -E '^(UU|AA|DU|UD)'
