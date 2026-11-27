# repos: <repo-artifacts> y <repo-data-models> · rama nueva <rama-trabajo>
# Reemplazar antes de correr: <ruta-repos> · <ruta-scratch> · <repo-artifacts> · <repo-data-models> · <rama-trabajo> · <sis> (argumento del generador) · <prefijo-tabla> (prefijo de las filas de la tabla en ESTADO.md)
cd "<ruta-repos>"
for R in <repo-artifacts> <repo-data-models>; do
  git -C $R status --porcelain | head -3
  git -C $R switch -q -c <rama-trabajo> origin/co-prd --no-track && git -C $R status -sb | head -1
done
cd "<ruta-scratch>/scripts"
python gen_ingestas.py <sis> 2>&1 | tail -4
grep -E '^\| <prefijo-tabla>' out/<sis>/out/ESTADO.md | cut -c1-300
