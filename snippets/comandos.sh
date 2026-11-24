# repo: datope-fif-bfaco-artifacts · rama feature/DL2-FIFDF-89514-ingesta-bup
cd /c/dataops/bfa-co/falabella-regulado-datafactory/datope-fif-bfaco-artifacts
K=ci/checks/known_debt.txt
if grep -q $'\r' $K; then e=$'\r\n'; else e=$'\n'; fi
grep -q 'bup/all_tables' $K || printf '%s%s' 'file:co/data_source/bup/all_tables/bup_all_tables.yaml   # FIFDF-89514 prueba de conexion: raw_schema na por diseno; retirar con la excepcion en structure' "$e" >> $K
git status --short | cut -c1-2 | sort | uniq -c
git status --short | grep -vE 'bup/[a-z_]+/bup_[a-z_]+\.(yaml|json)$'
