# repo: <repo-legacy> (solo lectura, rama productiva DL 1.0)
# Reemplazar antes de correrlo: <ruta-repos> · <repo-legacy> · <rama-prd-legacy> · <ruta-cookiecutter> (regex terminado en cookiecutter.json$, con '/' al inicio) · <carpeta-tabla> (carpeta de la tabla, para el listado de respaldo)
cd "<ruta-repos>/<repo-legacy>"
F=$(git ls-tree -r --name-only origin/<rama-prd-legacy> | grep -i '<ruta-cookiecutter>')
[ -n "$F" ] && echo "$F" || { echo "no encontrado"; git ls-tree -r --name-only origin/<rama-prd-legacy> | grep -i '<carpeta-tabla>/'; }
[ -n "$F" ] && git show "origin/<rama-prd-legacy>:$F" | python -c "
import json,sys,re
d=json.load(sys.stdin)
print('raíz:', [k for k in d if not isinstance(d[k],dict)])
for k,v in d.items():
    if isinstance(v,dict):
        print('bloque', k, '->', list(v))
        cols=str(v.get('cols',''))
        print('  cols con AS:', [c.strip()[:90] for c in cols.split(',') if ' AS ' in c.upper()][:5])
        q=str(v.get('query',''))
        print('  query:', len(q), 'car. · AS:', len(re.findall(r'\sAS\s',q,re.I)), '· WHERE:', re.sub(r'\s+',' ',q[q.upper().find('WHERE'):][:160]) if 'WHERE' in q.upper() else 'no')
print('is_incremental_load:', d.get('is_incremental_load'), '· schedule:', d.get('schedule'))
"
