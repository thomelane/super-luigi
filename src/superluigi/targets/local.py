from contextlib import contextmanager
import tempfile
from typing import Union, Optional, Dict, Any
from pathlib import Path
import shutil
import json
import hashlib
from luigi import Target


def file_md5_hash(filepath: Path) -> str:
    with open(filepath, "rb") as f:
        file_hash = hashlib.md5()
        # md5 has 128-byte digest blocks (8192 is 128×64)
        # x64 is empirically a sweet spot between speed and memory usage.
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)
    return file_hash.hexdigest()


class LocalFileTarget(Target):
    def __init__(
        self,
        path: Union[str, Path],
        metadata_path: Optional[Union[str, Path]] = None
    ) -> None:
        self.path = Path(path)
        self.metadata_path = Path(metadata_path or self.default_metadata_path())

    def exists(self) -> bool:
        assert not self.path.is_dir(), \
            f'A folder was found at {self.path} instead of a file. ' + \
            'Did you mean to use LocalFolderTarget instead?'
        return self.path.exists()

    def default_metadata_path(self) -> Path:
        return self.path.with_suffix(self.path.suffix + '.metadata.json')

    def write_metadata(self, metadata: dict):
        metadata['output'] = {
            'type': self.__class__.__name__,
            'path': str(self.path),
            'md5_hash': file_md5_hash(self.path)
        }
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)

    def read_metadata(self) -> Optional[dict]:
        try:
            with open(self.metadata_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    @contextmanager
    def tmp_read_path(self, **kwargs):
        assert not self.path.is_dir(), f'{self.path} already exists as a folder.'
        with tempfile.NamedTemporaryFile(suffix=self.path.suffix) as tmp_file:
            tmp_path = tmp_file.name
            shutil.copyfile(self.path, tmp_path)
            yield tmp_path

    @contextmanager
    def tmp_write_path(self, **kwargs):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir, f'tmpfile{self.path.suffix}')
            yield tmp_path
            assert tmp_path.exists(), f'{tmp_path} (for {self.path}) does not exist.'
            assert tmp_path.is_file(), f'{tmp_path} (for {self.path}) is not a file.'
            self.path.parent.mkdir(exist_ok=True, parents=True)
            shutil.copyfile(tmp_path, self.path)


class LocalFolderTarget(Target):
    def __init__(
        self,
        path: Union[str, Path],
        metadata_path: Optional[Union[str, Path]] = None,
        metadata_inside_folder: bool = False,
        metadata_contents_limit: Optional[int] = 10
    ) -> None:
        self.path = Path(path)
        self.metadata_path = Path(metadata_path or self.default_metadata_path(metadata_inside_folder))
        self.metadata_contents_limit = metadata_contents_limit

    def exists(self) -> bool:
        assert not self.path.is_file(), \
            f'A file was found at {self.path} instead of a folder. ' + \
            'Did you mean to use LocalFileTarget instead?'
        return self.path.exists()

    def default_metadata_path(self, metadata_inside_folder: bool = False) -> Path:
        """
        When `metadata_path` is not provided, this function is used
        to determine the path of the metadata file. By default, the
        metadata file will be stored *alongside* the folder: i.e.
        `{path}.metadata.json`. This is necessary when the
        contents of the prefix will be consumed by a system that will error
        at the presence of the metadata file: e.g. Spark.

        Using `metadata_inside_folder`, the metadata can be stored
        *inside* the folder: i.e. i.e. `{path}/metadata.json`.

        Args:
            metadata_inside_prefix (bool, optional):
                when True, the metadata file will be located at
                `{path}/metadata.json`. Defaults to False.

        Returns:
            str: object key of the metadata file
        """
        if not metadata_inside_folder:
            return Path(str(self.path) + '.metadata.json')
        else:
            return Path(self.path, 'metadata.json')

    def write_metadata(self, metadata: dict):
        output: Dict[str, Any] = {
            'type': self.__class__.__name__,
            'path': str(self.path)
        }
        files = [f for f in self.path.glob('**/*') if f.is_file()]
        assert len(files) > 0, f"Couldn't find any files at {self.path}."
        if (self.metadata_contents_limit == None) or (len(files) < self.metadata_contents_limit):
            contents = []
            for file in files:
                content = {
                    'path': str(Path(file).relative_to(self.path)),
                    'md5_hash': file_md5_hash(file)
                }
                contents.append(content)
            output['contents'] = contents
        metadata['output'] = output
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.metadata_path, 'w', encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)

    def read_metadata(self) -> Optional[dict]:
        try:
            with open(self.metadata_path, 'r', encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    @contextmanager
    def tmp_read_path(self, **kwargs):
        assert not self.path.is_file(), f'{self.path} already exists as a file.'
        with tempfile.TemporaryDirectory() as tmp_path:
            tmp_path = Path(tmp_path)
            shutil.rmtree(tmp_path)  # delete folder (for shutil.copytree)
            shutil.copytree(self.path, tmp_path)
            yield tmp_path

    @contextmanager
    def tmp_write_path(self, **kwargs):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            yield tmp_path
            assert tmp_path.exists(), f'{tmp_path} (for {self.path}) does not exist.'
            assert tmp_path.is_dir(), f'{tmp_path} (for {self.path}) is not a folder.'
            assert any(tmp_path.iterdir()), f'{tmp_path} (for {self.path}) is an empty folder.'
            self.path.parent.mkdir(exist_ok=True, parents=True)
            if self.path.is_dir():
                shutil.rmtree(self.path)  # delete folder
            shutil.copytree(tmp_path, self.path)
