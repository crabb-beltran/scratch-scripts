# scratch-scripts

Utilidades sueltas de soporte para el día a día.

| Script | Uso |
|---|---|
| `scripts/verificar_reorden.py` | Verifica que reordenar un YAML no quitó/cambió valores vs `git show <ref>:<ruta>`. Detecta llaves duplicadas. No imprime valores. Exit: 0 OK, 1 REVISAR, 2 no existe en ref. Requiere `pyyaml`. |
| `scripts/agregar_faltantes.py` | Reinserta en `<file>` las llaves (líneas crudas) que existen en `<ref>` y faltan en el working copy, dentro del bloque `co-*` correcto, sin tocar líneas existentes. Valida antes de escribir (duplicadas, orden, EOL). `--dry-run`. Exit: 0 OK, 1 bloques solo en origen, 2 abortado. |

```bash
# desde la raíz del repo a verificar
python ../../scratch/scripts/verificar_reorden.py co/variables.yaml        # vs HEAD
python ../../scratch/scripts/verificar_reorden.py dbt_project.yml origin/main
python ../../scratch/scripts/agregar_faltantes.py co/variables.yaml origin/main --dry-run
```
