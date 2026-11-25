# repo: datope-fif-bfaco-artifacts
git fetch -q origin
git switch -c fix/DL2-FIFDF-89796-jdbc-admision-dev origin/co-dev --no-track
sed -i -E "s#^(\s*JDBC_ADMISION:\s*'jdbc:oracle:thin:@[^:/']+:[0-9]+):([A-Za-z0-9_]+)'#\1/\2'#" co/variables.yaml
git diff -U0 | grep -E '^[-+]\s' | sed -E "s#@[^:/']+#@<host>#; s#(:[0-9]+)[:/][A-Za-z0-9_]+#\1<sep><nombre>#"
git diff --stat | tail -1
