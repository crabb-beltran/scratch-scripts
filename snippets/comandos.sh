# PR (título y descripción) · repo: datope-fif-bfaco-data-models
#
# FIFDF-89514 feat(modelos): modelos std de bup como snapshot
#
# ## Cambio
# Actualiza los 15 modelos `std_customer/bup_*` (sql + schema.yml), que estaban en su versión inicial (`first push`), con la versión generada para la ingesta BUP por cipher:
# - Estrategia `snapshot` (sin llave única confirmada para CDC)
# - `data_tests: []` donde la llave estaba POR_CONFIRMAR
# - Descripciones de columnas completadas
#
# Sin cambios en `dbt_project.yml`.
#
# ## Contexto
# La ingesta BUP en artifacts ya está en co-dev. co-dev y co-prd tenían la misma versión vieja de estos modelos, así que esta rama aplica limpia a ambos.
#
# ## Validación
# - [ ] Checks en verde
# - [ ] Deploy co-dev y ejecución del DAG BUP: tablas std pobladas
# - [ ] Pendiente: evaluar `cdc_hash` con las PK confirmadas; 10 columnas con Gobierno
