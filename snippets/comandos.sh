# repo: datope-fif-bfaco-data-models · rama feature/DL2-FIFDF-89514-ingesta-bup
git diff -U0 origin/co-dev -- dbt_project.yml | grep -E '^[+-] ' | sed -E 's/:.*/: …/'
git diff --stat --ignore-cr-at-eol origin/co-prd origin/co-dev -- 'datope_bfaco/models/std_customer/bup_*' | tail -1
git diff --stat --ignore-cr-at-eol origin/co-dev -- 'datope_bfaco/models/std_customer/bup_*' | tail -1
git status -s --untracked-files=all | grep -v '^ M' | head
