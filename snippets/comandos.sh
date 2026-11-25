# repo: datope-fif-bfaco-artifacts · rama fix/DL2-FIFDF-89796-jdbc-admision-dev
git checkout -- co/variables.yaml
sed -i -E "s#^(\s*JDBC_ADMISION:\s*'jdbc:oracle:thin:@[^:/']+:[0-9]+):([A-Za-z0-9_]+)'#\1/\2'#" co/variables.yaml
git diff --stat | tail -1
python ci/checks/run_checks.py --checks keys,parity,dup,syntax,files,hardcoded 2>&1 | grep -E '^\[|^\s+x'
git commit -qam "fix(ingesta): usa nombre de servicio en la url jdbc de admision" && git log --oneline -1
git push -u origin fix/DL2-FIFDF-89796-jdbc-admision-dev

# ---------------------------------------------------------------------------------------------------------------------
# PR (título y descripción)
#
# FIFDF-89796 fix(ingesta): url jdbc de admision por nombre de servicio
#
# ## Cambio
# `JDBC_ADMISION` (co-dev y co-prd): de `host:puerto:SID` a `host:puerto/servicio`, el formato del resto de sistemas JDBC que extraen en dev.
#
# ## Contexto
# Firewall confirma que el tráfico llega y que el servidor cierra la conexión (reset desde el servidor), compatible con un rechazo del listener por SID (ORA-12505).
#
# ## Validación
# - [ ] Checks y deploy co-dev en verde
# - [ ] `co_admision_all_tables-extraction` deja CSV en landing
# - [ ] Antes del PR a co-prd: confirmar con el administrador de la base que en prd el nombre de servicio coincide
