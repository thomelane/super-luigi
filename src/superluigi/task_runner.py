from pathlib import Path
import sys
from argparse import ArgumentParser
from importlib import import_module
import luigi
from typing import Union, Optional, Type
import os

from superluigi.tasks.base import BaseTask, apply_to_target


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
    parser.add_argument("--config", required=True)
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


def run_task(
    task: Union[luigi.Task, type],
    config: Optional[Path] = None,
    local_scheduler: bool = True,
    force: bool = False,
    force_upstream: bool = False,
    force_upstream_to_task: Optional[BaseTask] = None,
    force_upstream_to_task_cls: Optional[Type] = None,
    raise_exception: bool = False
):
    """
    Will run a task using specified config file.

    Args:
        task (Union[luigi.Task, type]): the Task instance (or a Task class) to be run.
            When a Task class is provided the task will be instanciated using the config.
            i.e. the task's parameter values will be taken from the config file.
        config (Path): path to the config file.
        local_scheduler (bool, optional): Defaults to True.
            See https://luigi.readthedocs.io/en/stable/central_scheduler.html for details.
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

    # load config
    if config:
        assert Path(config).exists(), f"{config} doesn't exist"
        luigi.configuration.add_config_path(config)
        print(f'Using config at {config}')
    else:
        default_config = os.getenv('SUPERLUIGI_DEFAULT_TASK_CONFIG', './superluigi.cfg')
        if Path(default_config).exists():
            luigi.configuration.add_config_path(default_config)
            print(f'Using config at {default_config}')

    # create an instance if task is a Task class
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
        local_scheduler=local_scheduler
    )

    if status:
        output_path = apply_to_target(
            target=task.output(),
            fn=lambda e: e.path
        )
        print(f'Ouputs can be found at {output_path}')


def run_task_and_download_output(
    task: Union[luigi.Task, type],
    config: Path = os.getenv('SUPERLUIGI_DEFAULT_TASK_CONFIG', './superluigi.cfg'),
    local_scheduler: bool = True,
    force: bool = False,
    force_upstream: bool = False,
    force_upstream_to_task: Optional[BaseTask] = None,
    force_upstream_to_task_cls: Optional[Type] = None,
    raise_exception: bool = False
) -> Union[Path, list, dict]:
    """
    Convienience function for running a task and downloading the output.
    Useful for tasks that output to S3 targets.

    Args:
        task (Union[luigi.Task, type]): the Task instance (or a Task class) to be run.
            When a Task class is provided the task will be instanciated using the config.
            i.e. the task's parameter values will be taken from the config file.
        config (Path): path to the config file.
        local_scheduler (bool, optional): Defaults to True.
            See https://luigi.readthedocs.io/en/stable/central_scheduler.html for details.
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

    Returns:
        Path: local path of the output after being downloaded.
            Could be a Path, or a more complex structure (e.g. list or dict) depending on
            the return in the task's `output` method.
    """
    def validate_output(target):
        assert hasattr(target, 'download'), \
            "Output target doesn't have a `download` method. Use `run_task` instead."

    output_path = apply_to_target(
        target=task.output(),
        fn=validate_output
    )
    run_task(
        task=task,
        config=config,
        local_scheduler=local_scheduler,
        force=force,
        force_upstream=force_upstream,
        force_upstream_to_task=force_upstream_to_task,
        force_upstream_to_task_cls=force_upstream_to_task_cls,
        raise_exception=raise_exception
    )
    output_path = apply_to_target(
        target=task.output(),
        fn=lambda e: e.download()
    )
    return output_path


if __name__ == "__main__":
    args = parse_args()
    module_path, task_name = parse_task_path(args.task)
    task_cls = get_task_cls(module_path, task_name)
    run_task(task_cls, args.config, not args.central_scheduler)
