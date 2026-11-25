# repo: datope-fif-bfaco-artifacts · cualquier rama al día con co-dev
git show origin/co-dev:co/variables.yaml > /tmp/v.yaml
python - <<'EOF'
import re, yaml
d = yaml.safe_load(open("/tmp/v.yaml", encoding="utf-8"))
root = d.get("enviroments") or d.get("environments") or d
blk = next(b["co-dev"] for b in (root if isinstance(root, list) else [root]) if isinstance(b, dict) and "co-dev" in b)
for k, v in sorted(blk.items()):
    v = str(v or "")
    if re.match(r"^(USER|PASS)_.+_CREDENTIALS$", k):
        amb = "qa" if "_qa_" in v else "dev" if "_dev_" in v else "prd" if "_prd_" in v else "otro/vacío"
        print(f"{k:40} secreto ambiente={amb}")
    elif k.startswith("JDBC_"):
        f = "//host/servicio" if "@//" in v else "host/servicio" if re.search(r"@[^/]+:\d+/", v) else "host:SID" if re.search(r"@[^/]+:\d+:", v) else "otro/vacío"
        print(f"{k:40} url={f}")
EOF
rm -f /tmp/v.yaml
