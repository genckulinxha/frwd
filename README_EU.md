# EU Pipeline

Run the EU pipeline independently of the existing one.

- Environment: set `DATABASE_URL` as with the existing pipeline.
- Create EU tables and run all phases:

```bash
python eu_main.py
```

- Run a single phase (discovery only) by importing and calling directly if needed:

```python
from eu_pipeline.discovery import discover_eu_laws

discover_eu_laws()
``` 