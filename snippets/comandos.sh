# repo: datope-fif-bfaco-artifacts · rama co-dev
git pull -q --ff-only && git status -sb | head -1
git push origin --delete docs/DL2-FIFDF-89481-documentacion feature/DL2-FIFDF-89514-ingesta-bup-dev
git fetch -q --prune origin && git branch -r | grep -E 'FIFDF-(89481|89514|89796|90002)'
