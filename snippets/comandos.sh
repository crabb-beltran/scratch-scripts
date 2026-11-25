# repo: datope-fif-bfaco-data-models · rama feature/DL2-FIFDF-89514-ingesta-bup
git diff dbt_project.yml > ../scratch/dbt_project_wip_25nov.patch && wc -l ../scratch/dbt_project_wip_25nov.patch
git checkout -- dbt_project.yml
git add datope_bfaco/models/std_customer/
git diff --cached --stat | tail -1
git status -s | grep -v '^M ' | head
git commit -q -m "feat(modelos): actualiza los 15 modelos de bup generados como snapshot" && git log --oneline -1
