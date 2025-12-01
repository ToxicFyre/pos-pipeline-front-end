from pathlib import Path
from pos_core import DataPaths
from pos_core.sales import marts

#Configure Paths
paths = DataPaths.from_root(Path("data"), Path("sucursales.json"))

# Sales: Aggregated Sales mart by group
sales_group = marts.fetch_group(paths, "2025-11-24", "2025-11-30")

print(sales_group.head())
