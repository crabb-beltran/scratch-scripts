grep -cE '^(<<<<<<<|=======|>>>>>>>)' co/variables.yaml ci/checks/known_debt.txt
grep -c 'all_tables' ci/checks/known_debt.txt
grep -nE '^\s*(- *)?(co-dev|co-prd)\s*:|^\s*#\s*BUP|^\s*(BUP_[A-Z_]+|USER_BUP_CREDENTIALS|PASS_BUP_CREDENTIALS)\s*:' co/variables.yaml | awk -F: '{print $1":"$2}'
