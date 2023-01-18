from pathlib import Path
import sys
from argparse import ArgumentParser
from importlib import import_module
import luigi
from typing import Union, Optional, Type, Dict

from superluigi.tasks.base import BaseTask, ExternalTask, apply_to_target
from superluigi.config import SUPERLUIGI_DEFAULT_TASK_CONFIG


current_dir = Path(__file__).parent
tasks_dir = Path(current_dir, 'tasks')
sys.path.append(str(tasks_dir))


def get_task_cls(module_path, task_name):
    module = import_module(module_path)
    task_cls = getattr(module, task_name)
    return task_cls


def parse_task_path(task_path):
    parts = task_path.split('.')
    module_path = ".".join(parts[:-1])
    task_name = parts[-1]
    return module_path, task_name


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--central-scheduler", action='store_true')
    args = parser.parse_args()
    return args


def raise_exception_fn(root_task):
    root_task.raise_exception = True
    [raise_exception_fn(t) for t in luigi.task.flatten(root_task.requires())]


def force_upstream_fn(root_task):
    root_task.force = True
    [force_upstream_fn(t) for t in luigi.task.flatten(root_task.requires())]


def force_upstream_to_fn(
    root_task,
    force_task=None,
    force_task_cls=None
):
    if isinstance(root_task, ExternalTask):
        return False  # can't force an ExternalTask
    if force_task:
        assert isinstance(force_task, BaseTask)
        is_force_task = root_task is force_task
    elif force_task_cls:
        is_force_task = isinstance(root_task, force_task_cls)
    else:
        is_force_task = False
    is_downstream_of_force_task = any([
        force_upstream_to_fn(t, force_task, force_task_cls)
        for t in luigi.task.flatten(root_task.requires())
    ])
    force = is_force_task or is_downstream_of_force_task
    root_task.force = root_task.force or force  # keep forced if already set
    return force


def set_config(config: Dict[str, Dict[str, str]] = {}):
    for section in config:
        for option in config[section]:
            value = config[section][option]
            assert isinstance(value, str)
            luigi.configuration.get_config().set(section, option, value)


def set_config_file(config_file: Optional[Path] = None):
    if config_file:
        assert Path(config_file).exists(), f"{config_file} doesn't exist"
        luigi.configuration.add_config_path(config_file)
        print(f'Using config at {config_file}')
    else:
        default_config_file = SUPERLUIGI_DEFAULT_TASK_CONFIG
        if Path(default_config_file).exists():
            luigi.configuration.add_config_path(default_config_file)
            print(f'Using config at {default_config_file}')


def get_output_path(target, local_output, force_local_download):
    if local_output:
        if hasattr(target, 'download'):
            return target.download(force_local_download=force_local_download)
    return target.path


def run_task(
    task: Union[luigi.Task, type],
    config: Optional[dict] = None,
    config_file: Optional[Union[Path, str]] = None,
    central_scheduler: bool = False,
    num_workers: int = 1,
    local_output: bool = False,
    force_local_download: bool = False,
    force: bool = False,
    force_upstream: bool = False,
    force_upstream_to_task: Optional[BaseTask] = None,
    force_upstream_to_task_cls: Optional[Type] = None,
    raise_exception: bool = False
):
    """
    Will run a task using specified config.

    Args:
        task (Union[luigi.Task, type]): the Task instance (or a Task class) to be run.
            When a Task class is provided the task will be instanciated using the config.
            i.e. the task's parameter values will be taken from the config.
        config (dict): dict containing config.
        config_file (Path): path to the config file.
        central_scheduler (bool, optional): Defaults to False.
            See https://luigi.readthedocs.io/en/stable/central_scheduler.html for details.
        num_workers (int, optional): Defaults to 1.
            Will use this number of workers when processing tasks.
            Can set `resources = {'max_workers': 1}` on a task to limit parallel execution.
        local_output (bool, optional): Defaults to False.
            Will download the `task` outputs to local file system.
            Useful for tasks that output to S3 targets.
        force_local_download (bool, optional): Defaults to False.
            Will force the `task` outputs to download (even if they are already downloaded and unchanged).
        force (bool, optional): Defaults to False.
            Will force the `task` to run (even when its targets already exist).
        force_upstream (bool, optional): Defaults to False.
            Will force the `task` to run and all of its upstream requirements
            too (even when their targets already exist).
        force_upstream_to_task (Optional[BaseTask], optional): Defaults to None.
            Will look upstream from `task` for `force_upstream_to_task` and, if
            found, will force `force_upstream_to_task`, `task` and their
            intermediate tasks to run (even when their targets already exist).
        force_upstream_to_task_cls (Optional[Type], optional): Defaults to None.
            Will look upstream from `task` for tasks of type
            `force_upstream_to_task_cls` and, if found, will force those
            `force_upstream_to_task_cls` tasks, the `task` and all their
            intermediate tasks to run (even when their targets already exist).
        raise_exception (bool, optional): Defaults to False.
            Will raise the exception rather than capturing it and failing the task.
            Useful for development and debugging.
    """
    # validate inputs
    assert not (raise_exception and (num_workers > 1)), \
        "Can't raise_exception when num_workers > 1."           
    assert not ((config is not None) and (config_file is not None)), \
        "Only a single `config` argument should be provided to `run_task`."
    assert not (raise_exception and (num_workers > 1)), \
        "Can't raise_exception when num_workers > 1."
    num_force_args = sum([force, force_upstream, force_upstream_to_task is not None, force_upstream_to_task_cls is not None])
    assert num_force_args <= 1, \
        "Only a single `force` argument should be provided to `run_task`."
    # Since the dependency graph is forked between workers, when the forced
    # task is complete and `self.force` is set to `False` (see
    # `run_with_ctx` of `BaseTask`) this only changes the state on the
    # current worker. Other workers have a copy of the dependency graph
    # where that task doesn't exist (i.e. where `self.force` is still True,
    # so it appears as if the task output doesn't exist)
    assert not ((num_force_args > 0) and (num_workers > 1)), \
        "Can't force tasks when running with multiple workers."

    # create an instance if task is a Task class
    if config:
        set_config(config)
    elif config_file:
        set_config_file(Path(config_file))
    if isinstance(task, type):
        task = task()

    # set force on root task and upstream
    if force:
        task.force = True
    elif force_upstream:
        force_upstream_fn(task)
    elif force_upstream_to_task or force_upstream_to_task_cls:
        force_upstream_to_fn(task, force_upstream_to_task, force_upstream_to_task_cls)

    # set raise_exception
    if raise_exception:
        raise_exception_fn(task)

    status = luigi.build(
        tasks=[task],
        local_scheduler=not central_scheduler,
        workers=num_workers,
    )

    if status:
        output_path = apply_to_target(
            target=task.output(),
            fn=lambda e: get_output_path(e, local_output, force_local_download)
        )
        print(f'Ouputs can be found at {output_path}')
        return output_path
    else:
        raise Exception('`luigi.build` failed. Check above the summary for error logs.')


if __name__ == "__main__":
    args = parse_args()
    module_path, task_name = parse_task_path(args.task)
    task_cls = get_task_cls(module_path, task_name)
    run_task(task_cls, args.config_file, args.central_scheduler)
