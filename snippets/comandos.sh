# repo: datope-fif-bfaco-artifacts · en tu rama
git fetch -q origin
git diff --stat HEAD origin/<rama-origen> -- <ruta1> <ruta2>
git restore --source=origin/<rama-origen> --staged --worktree -- <ruta1> <ruta2>
git status -s
