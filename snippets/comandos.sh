# repo: datope-fif-bfaco-artifacts · rebase en curso (REBASE 2/3)
git add co/variables.yaml
GIT_EDITOR=true git rebase --continue
git log --oneline -4
git status --short | wc -l
python ci/checks/run_checks.py --base origin/co-dev --base-ref co-dev 2>&1 | grep -E '^\[|^\s+x|^Resultado' | grep -vE "campo 'owner'"
