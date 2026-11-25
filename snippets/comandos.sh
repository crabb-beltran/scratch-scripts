# repo: datope-fif-bfaco-artifacts
git fetch -q --prune origin
git switch -q co-dev 2>/dev/null || git switch -q -c co-dev origin/co-dev
for b in $(git for-each-ref --format='%(refname:short)' refs/heads); do
  printf '%-55s %-8s dev:%-6s prd:%s\n' "$b" \
    "$(git for-each-ref --format='%(upstream:track)' refs/heads/$b | grep -q gone && echo GONE || echo -)" \
    "$(git merge-base --is-ancestor $b origin/co-dev && echo merged || echo -)" \
    "$(git merge-base --is-ancestor $b origin/co-prd && echo merged || echo -)"
done
