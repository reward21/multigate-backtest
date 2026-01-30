from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    df2 = df.copy()
    if isinstance(df2.index, pd.DatetimeIndex):
        df2 = df2.reset_index().rename(columns={"index": "timestamp"})
    table = pa.Table.from_pandas(df2, preserve_index=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)

def read_parquet(path: Path) -> pd.DataFrame:
    table = pq.read_table(path)
    df = table.to_pandas()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")
    return df
