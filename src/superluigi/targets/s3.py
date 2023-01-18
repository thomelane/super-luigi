import math
import luigi
import boto3
from contextlib import contextmanager
import tempfile
from typing import Union, Generator, Optional, Dict, Any
from pathlib import Path
import botocore
import shutil
import json
from typing import Optional, Union
import os

from superluigi.config import SUPERLUIGI_LOCAL_DATA_DIR


def get_creds(role_arn):
    sts = boto3.client('sts')
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName="placeholder"
    )
    credentials = {
        "aws_access_key_id": response['Credentials']['AccessKeyId'],
        "aws_secret_access_key": response['Credentials']['SecretAccessKey'],
        "aws_session_token": response['Credentials']['SessionToken']
    }
    return credentials


def get_s3_client(
    role_arn: Optional[str] = None,
    **kwargs
) -> boto3.client:
    if role_arn:
        creds = get_creds(role_arn)
        kwargs = {**kwargs, **creds}
    return boto3.client('s3', **kwargs)


def s3_object_exists(
    s3_client: boto3.client,
    bucket: str,
    object_key: str
) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=object_key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
    else:
        return True


def get_metadata_s3_object(
    s3_client: boto3.client,
    bucket: str,
    object_key: str
) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    response_body = response['Body']
    json_str = response_body.read().decode('utf-8')
    data = json.loads(json_str)
    return data


def put_metadata_s3_object(
    s3_client: boto3.client,
    bucket: str,
    object_key: str,
    data: dict
) -> None:
    s3_client.put_object(
        Body=json.dumps(data, indent=4),
        Bucket=bucket,
        Key=object_key
    )


def s3_prefix_exists(
    s3_client: boto3.client,
    bucket: str,
    prefix: str
) -> bool:
    # only list (at most) a single object to save bandwidth
    prefix = prefix if prefix.endswith('/') else prefix + '/'
    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    try:
        return len(response['Contents']) > 0
    except KeyError:
        return False


def head_s3_object(s3_client, bucket, object_key) -> Dict[str, Any]:
    response = s3_client.head_object(
        Bucket=bucket,
        Key=object_key,
    )
    return {
        'key': object_key,
        'size': response['ContentLength'],
        'etag': response['ETag']
    }


def get_bucket_location(s3_client, bucket) -> str:
    response = s3_client.get_bucket_location(Bucket=bucket)
    region = response['LocationConstraint']
    if region is None:
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html#API_GetBucketLocation_ResponseSyntax
        region = 'us-east-1'
    return region


class S3ObjectTarget(luigi.Target):
    def __init__(
        self,
        bucket: str,
        object_key: str,
        metadata_bucket: Optional[str] = None,
        metadata_object_key: Optional[str] = None,
        local_root: Union[Path, str] = SUPERLUIGI_LOCAL_DATA_DIR,
        **kwargs
    ) -> None:
        self.bucket = bucket
        self.object_key = object_key
        self.metadata_bucket = metadata_bucket or self.default_metadata_bucket()
        self.metadata_object_key = metadata_object_key or self.default_metadata_object_key()
        self.local_root = local_root
        self._local_bucket_path = Path(self.local_root, self.bucket)
        self._local_object_path = Path(self.local_root, self.bucket, self.object_key)
        self.local_path = str(self._local_object_path)
        self._s3_client = get_s3_client(**kwargs)

    def exists(self) -> bool:
        assert not s3_prefix_exists(self._s3_client, self.bucket, self.object_key), \
            f'A prefix was found at s3://{self.bucket}/{self.object_key} instead of an object. ' + \
            'Did you mean to use S3PrefixTarget instead?'
        return s3_object_exists(self._s3_client, self.bucket, self.object_key)

    @property
    def path(self) -> str:
        return f"s3://{self.bucket}/{self.object_key}"

    @property
    def metadata_path(self) -> str:
        return f"s3://{self.metadata_bucket}/{self.metadata_object_key}"

    @property
    def region(self) -> str:
        return get_bucket_location(self._s3_client, self.bucket)

    def default_metadata_bucket(self) -> str:
        return self.bucket

    def default_metadata_object_key(self) -> str:
        path = Path(self.object_key)
        metadata_path = path.with_suffix(path.suffix + '.metadata.json')
        return str(metadata_path)

    def write_metadata(self, metadata: dict):
        object_metadata = head_s3_object(
            self._s3_client,
            bucket=self.bucket,
            object_key=self.object_key
        )
        metadata['output'] = {
            'type': self.__class__.__name__,
            'path': self.path,
            'size': object_metadata['size'],
            'etag': object_metadata['etag']
        }
        return put_metadata_s3_object(
            s3_client=self._s3_client,
            bucket=self.metadata_bucket,
            object_key=self.metadata_object_key,
            data=metadata
        )

    def read_metadata(self) -> Optional[dict]:
        try:
            return get_metadata_s3_object(
                s3_client=self._s3_client,
                bucket=self.metadata_bucket,
                object_key=self.metadata_object_key
            )
        except self._s3_client.exceptions.NoSuchKey:
            return None

    def download(self, force_local_download=False) -> Path:
        if force_local_download or not self.local_same_as_remote():
            self.download_object()
        return self._local_object_path

    def download_object(self) -> Path:
        self._local_object_path.parent.mkdir(exist_ok=True, parents=True)
        self._s3_client.download_file(
            self.bucket,
            self.object_key,
            str(self._local_object_path)
        )
        assert self._local_object_path.exists(), f"{self._local_object_path} doesn't exist."
        return self._local_object_path

    def upload_object(self) -> None:
        self._s3_client.upload_file(
            str(self._local_object_path),
            self.bucket,
            self.object_key
        )

    def local_same_as_remote(self) -> bool:
        """
        Currently checks file and object sizes match.
        Can't check timestamps because they are not accurately persisted between file systems.
        """
        if not self._local_object_path.is_file():
            return False
        else:
            response = self._s3_client.head_object(Bucket=self.bucket, Key=self.object_key)
            s3_object_size = response['ContentLength']
            file_size = self._local_object_path.stat().st_size
            sizes_match = s3_object_size == file_size
            return sizes_match

    @contextmanager
    def tmp_read_path(self, force_local_download=False, **kwargs):
        assert not self._local_object_path.is_dir(), f'{self._local_object_path} already exists as a folder.'
        if force_local_download or not self.local_same_as_remote():
            self.download_object()
        with tempfile.NamedTemporaryFile() as tmp_file:
            tmp_path = tmp_file.name
            shutil.copyfile(self.local_path, tmp_path)
            yield tmp_path

    @contextmanager
    def tmp_write_path(self, **kwargs):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir, 'tmpfile')
            yield tmp_path
            assert tmp_path.exists(), f'{tmp_path} (for {self.path}) does not exist.'
            assert tmp_path.is_file(), f'{tmp_path} (for {self.path}) is not a file.'
            self._local_object_path.parent.mkdir(exist_ok=True, parents=True)
            shutil.copyfile(tmp_path, self._local_object_path)
        self.upload_object()


class S3PrefixTarget(luigi.Target):
    def __init__(
        self,
        bucket: str,
        prefix: str,
        metadata_bucket: Optional[str] = None,
        metadata_object_key: Optional[str] = None,
        metadata_inside_prefix: bool = False,
        metadata_contents_limit: Optional[int] = 10,
        local_root: Union[Path, str] = SUPERLUIGI_LOCAL_DATA_DIR,
        **kwargs
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix if prefix.endswith('/') else prefix + '/'
        self.metadata_bucket = metadata_bucket or \
            self.default_metadata_bucket()
        self.metadata_object_key = metadata_object_key or \
            self.default_metadata_object_key(metadata_inside_prefix)
        self.metadata_contents_limit = metadata_contents_limit
        self.local_root = local_root
        self._local_bucket_path = Path(self.local_root, self.bucket)
        self._local_prefix_path = Path(self.local_root, self.bucket, self.prefix)
        self.local_path = str(self._local_prefix_path)
        self._s3_client = get_s3_client(**kwargs)

    def exists(self) -> bool:
        assert not s3_object_exists(self._s3_client, self.bucket, self.prefix), \
            f'An object was found at s3://{self.bucket}/{self.prefix} instead of a prefix. ' + \
            'Did you mean to use S3ObjectTarget instead?'
        return s3_prefix_exists(self._s3_client, self.bucket, self.prefix)

    @property
    def path(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    @property
    def metadata_path(self) -> str:
        return f"s3://{self.metadata_bucket}/{self.metadata_object_key}"

    @property
    def region(self) -> str:
        return get_bucket_location(self._s3_client, self.bucket)

    def default_metadata_bucket(self) -> str:
        return self.bucket

    def default_metadata_object_key(self, metadata_inside_prefix: bool = False) -> str:
        """
        When `metadata_object_key` is not provided, this function is used
        to determine the object key of the metadata file. By default, the
        metadata file will be stored *alongside* the prefix: i.e.
        `s3://{bucket}/{prefix}.metadata.json`. This is necessary when the
        contents of the prefix will be consumed by a system that will error
        at the presence of the metadata file: e.g. Spark.

        Using `metadata_inside_prefix`, the metadata can be stored
        *inside* the prefix: i.e. i.e. `s3://{bucket}/{prefix}/metadata.json`.

        Args:
            metadata_inside_prefix (bool, optional):
                when True, the metadata file will be located at
                `s3://{bucket}/{prefix}/metadata.json`. Defaults to False.

        Returns:
            str: object key of the metadata file
        """
        if not metadata_inside_prefix:
            return str(Path(self.prefix)) + '.metadata.json'
        else:
            return str(Path(self.prefix, 'metadata.json'))

    def write_metadata(self, metadata: dict):
        output: Dict[str, Any] = {
            'type': self.__class__.__name__,
            'path': self.path
        }
        objects = [o for o in self.list_remote_objects()]
        assert len(objects) > 0, f"Couldn't find any objects at {self.path}."
        if (self.metadata_contents_limit == None) or (len(objects) < self.metadata_contents_limit):
            contents = []
            for object in objects:
                content = {
                    'key': str(Path(object['key']).relative_to(self.prefix)),
                    'size': object['size'],
                    'etag': object['etag']
                }
                contents.append(content)
            output['contents'] = contents
        metadata['output'] = output
        return put_metadata_s3_object(
            s3_client=self._s3_client,
            bucket=self.metadata_bucket,
            object_key=self.metadata_object_key,
            data=metadata
        )

    def read_metadata(self) -> Optional[dict]:
        try:
            return get_metadata_s3_object(
                s3_client=self._s3_client,
                bucket=self.metadata_bucket,
                object_key=self.metadata_object_key
            )
        except self._s3_client.exceptions.NoSuchKey:
            return None

    def local_same_as_remote(self) -> bool:
        local_objects = self.list_local_objects()
        remote_objects = self.list_remote_objects()
        local_object_by_key = {o['key']: o for o in local_objects}
        remote_objects_by_key = {o['key']: o for o in remote_objects}
        # if different objects, return False
        if set(local_object_by_key.keys()) != set(remote_objects_by_key.keys()):
            return False
        # if any object's size doesn't match, return False
        for key in local_object_by_key.keys():
            local_object_size = local_object_by_key[key]['size']
            remote_object_size = remote_objects_by_key[key]['size']
            if local_object_size != remote_object_size:
                return False
        # otherise return True
        return True

    def list_local_objects(self) -> Generator[dict, None, None]:
        for file in self._local_prefix_path.glob('**/*'):
            if file.is_file():
                yield {
                    'key': str(file.relative_to(self._local_bucket_path)),
                    'size': file.stat().st_size
                }

    def list_remote_objects(self) -> Generator[dict, None, None]:
        paginator = self._s3_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in page_iterator:
            if 'Contents' in page:
                for object in page['Contents']:
                    if not object['Key'].endswith('/'):
                        yield {
                            'key': object['Key'],
                            'size': object['Size'],
                            'etag': object['ETag']
                        }

    def download(self, force_local_download=False) -> Path:
        if force_local_download or not self.local_same_as_remote():
            self.download_prefix()
        return self._local_prefix_path

    def download_prefix(self) -> Path:
        if self._local_prefix_path.exists() and self._local_prefix_path.is_dir():
            shutil.rmtree(self._local_prefix_path)  # delete existing files
        for object in self.list_remote_objects():
            local_filepath = Path(self._local_bucket_path, object['key'])
            local_filepath.parent.mkdir(exist_ok=True, parents=True)
            self._s3_client.download_file(
                self.bucket,
                object['key'],
                str(local_filepath)
            )
            assert local_filepath.exists(), f"{local_filepath} doesn't exist."
        return self._local_prefix_path

    def upload_prefix(self) -> None:
        self.delete_remote_prefix()
        for object in self.list_local_objects():
            self._s3_client.upload_file(
                str(Path(self._local_bucket_path, object['key'])),
                self.bucket,
                object['key']
            )
        
    def delete_remote_prefix(self) -> None:
        objects = [{'Key': obj['key']} for obj in self.list_remote_objects()]
        # boto3 delete_objects request can handle max of 1000 objects, so must batch.
        num_batches = math.ceil(len(objects) / 1000)
        for batch_idx in range(num_batches):
            objects_batch = objects[batch_idx * 1000: (batch_idx + 1) * 1000]
            self._s3_client.delete_objects(
                Bucket=self.bucket,
                Delete={'Objects': objects_batch}
            )

    def put_object(self, object: str, object_key: str):
        self._s3_client.put_object(
            Body=object,
            Bucket=self.bucket,
            Key=self.prefix + object_key
        )

    @contextmanager
    def tmp_read_path(self, force_local_download=False, **kwargs):
        assert not self._local_prefix_path.is_file(), f'{self._local_prefix_path} already exists as a file.'
        if force_local_download or not self.local_same_as_remote():
            self.download_prefix()
        with tempfile.TemporaryDirectory() as tmp_path:
            tmp_path = Path(tmp_path)
            shutil.rmtree(tmp_path)  # delete folder (for shutil.copytree)
            shutil.copytree(self.local_path, tmp_path)
            yield tmp_path

    @contextmanager
    def tmp_write_path(self, **kwargs):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            yield tmp_path
            assert tmp_path.exists(), f'{tmp_path} (for {self.path}) does not exist.'
            assert tmp_path.is_dir(), f'{tmp_path} (for {self.path}) is not a folder.'
            assert any(tmp_path.iterdir()), f'{tmp_path} (for {self.path}) is an empty folder.'
            self._local_prefix_path.parent.mkdir(exist_ok=True, parents=True)
            if self._local_prefix_path.is_dir():
                shutil.rmtree(self._local_prefix_path)  # delete folder
            shutil.copytree(tmp_path, self._local_prefix_path)
        self.upload_prefix()
