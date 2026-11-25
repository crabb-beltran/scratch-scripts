# carpeta: <ruta-scratch>/scripts
# Reemplazar antes de correr: <ruta-scratch> · <repos-raiz> · <repo-artifacts> · <repo-data-models> · <rama-companero> · <patron> · <TABLA_1..3> · <SISTEMA> · <dominio> · <subdominio> · <sis>
cd "<ruta-scratch>/scripts"
R="../../<repos-raiz>"
git -C "$R/<repo-artifacts>" switch -q --detach "origin/<rama-companero>"
git -C "$R/<repo-data-models>" switch -q --detach origin/co-dev
F=in/new_migration_tables_to_dl2.csv
grep -ci '<patron>' $F
[ -n "$(tail -c1 $F)" ] && echo >> $F
printf '%s\n' \
 '<TABLA_1>;<SISTEMA>;<dominio>;<subdominio>;jdbc' \
 '<TABLA_2>;<SISTEMA>;<dominio>;<subdominio>;jdbc' \
 '<TABLA_3>;<SISTEMA>;<dominio>;<subdominio>;jdbc' >> $F
tail -4 $F
python gen_ingestas.py <sis> 2>&1 | tail -30
