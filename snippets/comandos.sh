# repo: datope-fif-bfaco-data-models · rama feature/DL2-FIFDF-89514-ingesta-bup
git diff --stat --ignore-cr-at-eol | tail -1
git ls-files --eol -- dbt_project.yml 'datope_bfaco/models/std_customer/bup_sttm_customer/*' | awk '{print $1,$2,$NF}'
git diff --stat origin/co-dev -- dbt_project.yml | tail -1
git diff --stat --ignore-cr-at-eol origin/co-dev -- datope_bfaco/models/std_customer/ | tail -1
git log --oneline -2 origin/co-prd -- datope_bfaco/models/std_customer/bup_sttm_customer
