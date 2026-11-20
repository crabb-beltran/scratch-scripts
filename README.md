# scratch-scripts

Utilidades sueltas de soporte para el día a día.

| Script | Uso |
|---|---|
| `scripts/gen_ingestas.py` | Generador DL 2.0 (v6.4): por sistema origen genera YAML/JSON de ingesta, modelos std dbt, `variables.yaml` y vars de `dbt_project.yml` en `out/`. Solo lee los repos. Requiere `scripts/in/cliente.yaml` local (plantilla: `scripts/cliente.ejemplo.yaml`). |
| `scripts/verificar_reorden.py` | Verifica que reordenar un YAML no quitó/cambió valores vs `git show <ref>:<ruta>`. Detecta llaves duplicadas. No imprime valores. Exit: 0 OK, 1 REVISAR, 2 no existe en ref. Requiere `pyyaml`. |
| `scripts/agregar_faltantes.py` | Reinserta en `<file>` las llaves (líneas crudas) que existen en `<ref>` y faltan en el working copy, dentro del bloque `co-*` correcto, sin tocar líneas existentes. Valida antes de escribir (duplicadas, orden, EOL). `--dry-run`. Exit: 0 OK, 1 bloques solo en origen, 2 abortado. |
| `scripts/agregar_faltantes_yaml.py` | Variante genérica de `agregar_faltantes.py` para `dbt_project.yml` (`vars`, `models:`): reinserta bloques completos (hijos + comentarios encima) que están en `<ref>` y faltan (p. ej. tras merge `--theirs`), después de su hermana anterior. Omite llaves movidas a propósito dentro de la misma sección raíz. Conserva CRLF/LF. `--dry-run`. Exit: 0 OK, 2 abortado. |
| `scripts/comparar_vars_dbt.py` | Compara `vars` de `dbt_project.yml` vs `<ref>` por **valor efectivo** por ambiente (`vars.ambientes.<env>[K]` → `vars[K]` → FALTA). Valida mover claves entre `vars` comunes y `ambientes`. Exit: 0 OK, 1 REVISAR (CAMBIA/QUITADA), 2 error. |

```bash
# desde la raíz del repo a verificar
python ../../scratch/scripts/verificar_reorden.py co/variables.yaml        # vs HEAD
python ../../scratch/scripts/verificar_reorden.py dbt_project.yml origin/main
python ../../scratch/scripts/agregar_faltantes.py co/variables.yaml origin/main --dry-run
python ../../scratch/scripts/agregar_faltantes_yaml.py dbt_project.yml origin/main --dry-run
python ../../scratch/scripts/comparar_vars_dbt.py dbt_project.yml origin/main
```

> `scripts/in/` y `scripts/out/` están en `.gitignore`: contienen valores del cliente, catálogos y salidas sensibles. Nunca se versionan.

## Docs

- `docs/estrategias-carga.html`: explicador interactivo de `snapshot` / `incremental` / `cdc_hash` (simulador, comparativa, árbol de decisión, `_hist` vs `_current`, operación). Abrir local en el navegador.
