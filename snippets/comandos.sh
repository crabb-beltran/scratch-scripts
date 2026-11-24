# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
python - <<'EOF'
import pathlib
R = [("'<referencia_secreto_host>'", "'{BUP_SECRET_HOST}'"),
     ("{BUP_OCI_SECRET_PATH}", "{BUP_HASHICORP_SECRET_PATH}"),
     ("{BUP_OCI_SECRET}", "{BUP_HASHICORP_SECRET_USER}")]
n = 0
for p in pathlib.Path("co/data_source/bup").rglob("*.yaml"):
    b = p.read_bytes(); t = b
    for a, c in R: t = t.replace(a.encode(), c.encode())
    if t != b: p.write_bytes(t); n += 1
print("yaml ajustados:", n)
EOF
grep -rlE "_OCI_SECRET|<referencia" co/data_source/bup | wc -l
git diff --stat | tail -1
git commit -qam "fix(ingesta): usa llaves de secreto cipher de bup con el patron hashicorp" && git log --oneline -1
