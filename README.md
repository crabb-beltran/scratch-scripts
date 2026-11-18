# scratch-scripts

Utilidades sueltas de soporte para el día a día.

| Script | Uso |
|---|---|
| `scripts/verificar_reorden.py` | Verifica que reordenar un YAML no quitó/cambió valores vs `git show <ref>:<ruta>`. Detecta llaves duplicadas. No imprime valores. Exit: 0 OK, 1 REVISAR, 2 no existe en ref. Requiere `pyyaml`. |

```bash
# desde la raíz del repo a verificar
python ../../scratch/scripts/verificar_reorden.py co/variables.yaml        # vs HEAD
python ../../scratch/scripts/verificar_reorden.py dbt_project.yml origin/main
```
