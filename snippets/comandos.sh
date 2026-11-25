# repo: datope-fif-bfaco-data-models
cd ../datope-fif-bfaco-data-models
git fetch -q --prune origin
git switch -q co-dev && git status -sb | head -1
for b in $(git for-each-ref --format='%(refname:short)' refs/heads); do
  printf '%-55s %-8s dev:%-6s prd:%s\n' "$b" \
    "$(git for-each-ref --format='%(upstream:track)' refs/heads/$b | grep -q gone && echo GONE || echo -)" \
    "$(git merge-base --is-ancestor $b origin/co-dev && echo merged || echo -)" \
    "$(git merge-base --is-ancestor $b origin/co-prd && echo merged || echo -)"
done
git branch -r | grep -vE 'HEAD|co-dev$|co-prd$'
