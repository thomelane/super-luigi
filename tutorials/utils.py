from pathlib import Path
from luigi.parameter import DateParameter, IntParameter
import pandas as pd
import pyarrow.parquet as pq
from typing import List, Union, Optional
from pathlib import Path
import joblib

from superluigi.tasks import Task
from superluigi.targets import LocalFileTarget
from superluigi.tasks.base import AtomicTaskWithMetadata


current_folder = Path(__file__).parent.resolve()


def parquet_to_pandas(parquet_dataset, columns=None):
    dfs = []
    if columns is None:
        columns_ = [k.name for k in parquet_dataset.schema]
    else:
        columns_ = list(columns)
    for column in columns_:
        dfs.append(parquet_dataset.read_pandas(columns=[column]).to_pandas())
    df = pd.concat(dfs, copy=False, axis=1)
    if columns is not None:
        assert set(df.columns) == set(columns)
    return df


def load_parquet_file(filepath: Union[Path, str], columns: List[str] = None) -> pd.DataFrame:
    filepath = Path(filepath)
    assert filepath.is_file(), f"{filepath} is a folder not file. Use `load_parquet_folder` on folders."
    pds = pq.ParquetDataset(filepath)
    df = parquet_to_pandas(pds, columns)
    return df


def load_parquet_folder(
    path: Union[Path, str],
    columns: Optional[List[str]] = None,
    suffix: str = '.parquet',
) -> pd.DataFrame:
    path = Path(path)
    assert path.is_dir(), f"{path} is a file not folder. Use `load_parquet_file` on files."
    files = [f for f in path.glob(f'*{suffix}')]
    assert len(files) > 0, f"Couldn't find any {suffix} files in {path}."
    df_parts = []
    for file in files:
        df_part = load_parquet_file(file, columns)
        df_parts.append(df_part)
    df = pd.concat(df_parts, ignore_index=True)
    return df


def save_parquet_file(df: pd.DataFrame, filepath: Path) -> Path:
    filepath.parent.mkdir(exist_ok=True, parents=True)
    df.to_parquet(filepath)
    return filepath


def load_csv_folder(
    path: Union[Path, str],
    suffix: str = '.csv',
    **kwargs
) -> pd.DataFrame:
    path = Path(path)
    assert path.is_dir()
    files = [f for f in path.glob(f'*{suffix}')]
    assert len(files) > 0, f"Couldn't find any {suffix} files in {path}."
    df_parts = []
    for file in files:
        df_part = pd.read_csv(file, **kwargs)
        df_parts.append(df_part)
    df = pd.concat(df_parts)
    return df


class FeaturesTask(AtomicTaskWithMetadata):
    date = DateParameter()

    def output(self):
        return LocalFileTarget(
            Path(f'./data/date={self.date:%Y%m%d}/features.csv').resolve()
        )

    def run(self, input_path, output_path, metadata):
        df = pd.DataFrame(
            [
                [2, 4, 5],
                [3, 6, 7],
                [4, 2, 9]
            ],
            columns=["feature1", "feature2", "feature3"]
        )
        df.to_csv(output_path, index=None)


class FeaturesTask(AtomicTaskWithMetadata):
    date = DateParameter()
    version = IntParameter()

    def output(self):
        return LocalFileTarget(
            Path(f'./data/date={self.date:%Y%m%d}/version={self.version}/features.csv').resolve()
        )

    def run(self, input_path, output_path, metadata):
        df = pd.DataFrame(
            [
                [2, 4, 5],
                [3, 6, 7],
                [4, 2, 9]
            ],
            columns=["feature1", "feature2", "feature3"]
        )
        df.to_csv(output_path, index=None)


class Model():
    def predict(self, X: pd.DataFrame):
        return X.sum(axis=1)


class ModelTask(AtomicTaskWithMetadata):
    version = IntParameter()

    def output(self):
        return LocalFileTarget(
            Path(f'./models/version={self.version}/model.joblib').resolve()
        )

    def run(self, input_path, output_path, metadata):
        model = Model()
        joblib.dump(model, output_path)
