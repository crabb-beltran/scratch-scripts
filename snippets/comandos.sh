# repos: <repo-artifacts> y <repo-data-models> (desde artifacts)
# Reemplazar antes de correr: <repo-data-models> = carpeta hermana del repo de modelos · <patron-ramas> = regex de las ramas a revisar
for R in . "../<repo-data-models>"; do
  echo "===== $R"
  git -C "$R" fetch -q origin
  for B in $(git -C "$R" branch -r | grep -iE '<patron-ramas>' | grep -v HEAD); do
    echo "--- $B · commits: $(git -C "$R" rev-list --count origin/co-dev..$B) · último: $(git -C "$R" log -1 --format='%cs %s' $B)"
    git -C "$R" diff --name-status origin/co-dev...$B | head -40
  done
done
