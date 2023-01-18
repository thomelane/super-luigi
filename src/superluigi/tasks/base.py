"""
Customized tasks that extend `luigi.Task` and task related utility functions.
"""
import luigi
from typing import Any, Callable, Union, Generator, Dict, List
from contextlib import contextmanager, ExitStack
import datetime
import hashlib


def build(tasks: Union[luigi.Task, List[luigi.Task]], central_scheduler=False):
    if not isinstance(tasks, list):
        tasks = [tasks]
    luigi.build(
        tasks=tasks,
        local_scheduler=not central_scheduler
    )


def apply_to_target(
    target: Union[luigi.Target, list, dict],
    fn: Callable, *args, **kwargs
):
    if isinstance(target, luigi.Target):
        return fn(target, *args, **kwargs)
    elif isinstance(target, list):
        return [apply_to_target(t, fn, *args, **kwargs) for t in target]
    elif isinstance(target, dict):
        return {k: apply_to_target(t, fn, *args, **kwargs) for k, t in target.items()}
    else:
        raise TypeError()


class BaseTask(luigi.Task):
    raise_exception = luigi.BoolParameter(significant=False, default=False)
    force = luigi.BoolParameter(significant=False, default=False)

    def __init_subclass__(cls):
        super().__init_subclass__()

        def run_with_ctx(func):
            def wrapper(self, **_):
                if hasattr(self, 'run_ctx'):
                    with self.run_ctx() as ctx_kwargs:
                        func(self, **ctx_kwargs)
                else:
                    func(self)
                self.force = False
            return wrapper

        cls.run = run_with_ctx(cls.run)

    def complete(self):
        """
        Complete will be checked before running a task, and the task will
        only be run if this method returns False. Luigi's standard method
        is to check to see if the task's target exists, but we add logic
        here to check the `force` parameter. If `force=True`, we
        always want to re-run the task, and so `complete` must always
        `return False` when scheduling is being performed, but then
        fallback to the standard method of checking completeness once the
        task has started. See `__init_subclass__` for this part.
        """
        if self.force:
            return False
        else:
            return super().complete()

    def on_failure(self, exception: Exception):
        """
        Called by Luigi when there is a failure, and this will re-raise the
        exception (i.e. not handle exception) when `raise_exception=True`.

        Args:
            exception (Exception): exception from the task

        Raises:
            Exception: exception from the task
        """
        if self.raise_exception:
            raise exception
        else:
            return super().on_failure(exception)

    @property
    def task_hash(self):
        """
        A hash of concat of `task_id`s from required tasks and current task.
        """
        requirements = self.requires()
        # requirements could output dict
        if isinstance(requirements, dict):
            # sort tasks by key
            requirements = [i[1] for i in sorted(requirements.items(), key=lambda e: e[0])]
        # requirements could output a task
        if isinstance(requirements, luigi.Task):
            requirements = [requirements]
        if requirements is None:
            requirements = []
        task_ids = [r.task_id for r in requirements] + [self.task_id]
        task_ids_str = "_".join(task_ids)
        hash_str = hashlib.md5(task_ids_str.encode('utf-8')).hexdigest()
        short_hash_str = hash_str[:16]  # 64 bit hash
        return short_hash_str

    @contextmanager
    def metadata_ctx(self) -> Generator[Dict[str, Any], None, None]:
        metadata = {}
        run_start_time = datetime.datetime.now(datetime.timezone.utc)
        metadata['input'] = apply_to_target(
            target=self.input(),
            fn=lambda e: e.read_metadata()
        )
        metadata['params'] = self.to_str_params(only_significant=True, only_public=True)
        metadata['task_family'] = self.task_family
        metadata['task_id'] = self.task_id
        metadata['task_hash'] = self.task_hash
        yield metadata
        run_end_time = datetime.datetime.now(datetime.timezone.utc)
        run_duration = run_end_time - run_start_time
        metadata['start_time'] = run_start_time.isoformat()
        metadata['end_time'] = run_end_time.isoformat()
        metadata['duration'] = run_duration.total_seconds()
        apply_to_target(
            target=self.output(),
            fn=lambda e: e.write_metadata(metadata)
        )


class ExternalTask(luigi.ExternalTask):
    pass


class Task(BaseTask):
    def run(self):
        raise NotImplementedError('Add `run` method to Task or use ExternalTask.')


class TaskWithMetadata(BaseTask):
    @contextmanager
    def run_ctx(self) -> Generator[Dict[str, Any], None, None]:
        with self.metadata_ctx() as metadata:
            yield {'metadata': metadata}

    def run(self, metadata):
        raise NotImplementedError('Add `run` method to Task or use ExternalTask.')


class AtomicTaskWithMetadata(BaseTask):
    @staticmethod
    def get_tmp_read_path(t, stack):
        return stack.enter_context(t.tmp_read_path())

    @staticmethod
    def get_tmp_write_path(t, stack):
        return stack.enter_context(t.tmp_write_path())

    @contextmanager
    def run_ctx(self) -> Generator[Dict[str, Any], None, None]:
        # ExitStack is used because the required contexts are dynamic
        with ExitStack() as stack:
            # metadata_ctx is first to __enter__, so is last to __exit__
            # metadata_ctx assumes output is written to path (not tmp_write_path)
            metadata = stack.enter_context(self.metadata_ctx())
            input_path = apply_to_target(
                target=self.input(),
                fn=self.get_tmp_read_path, stack=stack
            )
            output_path = apply_to_target(
                target=self.output(),
                fn=self.get_tmp_write_path, stack=stack
            )
            yield {
                'metadata': metadata,
                'input_path': input_path,
                'output_path': output_path
            }

    def run(self, input_path, output_path, metadata):
        raise NotImplementedError('Add `run` method to Task or use ExternalTask.')
