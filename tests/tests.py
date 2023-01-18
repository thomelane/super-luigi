import pytest
from pathlib import Path

from superluigi.tasks.base import BaseTask
from superluigi.parameters import Parameter
from superluigi.targets import LocalFileTarget
from superluigi.task_runner import run_task


current_folder = Path(__file__).parent.resolve()


class TaskA(BaseTask):
    param = Parameter()

    def requires(self):
        return None

    def output(self):
        return LocalFileTarget(path=Path(current_folder, f'tmp/force_test/taska/{self.param}.txt').resolve())

    def run(self):
        path = self.output().path
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, 'w') as f:
            f.write('hello world')


class TaskB(BaseTask):
    param = Parameter()

    def requires(self):
        return TaskA(param='a')

    def output(self):
        return LocalFileTarget(path=Path(current_folder, f'tmp/orce_test/taskb/{self.param}.txt').resolve())

    def run(self):
        path = self.output().path
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, 'w') as f:
            f.write('hello world')


class TaskC(BaseTask):
    param = Parameter()

    def requires(self):
        return None

    def output(self):
        return LocalFileTarget(path=Path(current_folder, f'tmp/force_test/taskc/{self.param}.txt').resolve())

    def run(self):
        path = self.output().path
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, 'w') as f:
            f.write('hello world')


class TaskD(BaseTask):
    param = Parameter()

    def requires(self):
        return [TaskB(param='b'), TaskC(param='c')]

    def output(self):
        return LocalFileTarget(path=Path(current_folder, f'./tmp/force_test/taskd/{self.param}.txt').resolve())

    def run(self):
        path = self.output().path
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, 'w') as f:
            f.write('hello world')


class TaskE(BaseTask):
    param = Parameter()

    def requires(self):
        return [TaskB(param='b'), TaskC(param='c')]

    def output(self):
        return LocalFileTarget(path=Path(current_folder, f'./tmp/force_test/taske/{self.param}.txt').resolve())

    def run(self):
        raise ValueError('something is wrong!!!')


def test_run_task_success():
    task = TaskD(param='d')
    run_task(task, force=True, raise_exception=True)


def test_run_task_fail():
    task = TaskE(param='e')
    with pytest.raises(ValueError):
        run_task(task, force=True, raise_exception=True)
