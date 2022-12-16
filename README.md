# Super Luigi

Super Luigi is a package for task orchestration in Python.

It's based on Luigi, which is also a package for task orchestration in Python similar in concept to Make, but Super Luigi offers a number of enhancements.

## Luigi Benefits

Luigi is a great starting point for task orchestration and already has the following benefits:

* A simple way to structure complex workflows
* Just a Python package so is lightweight compared to infrastructure based solutions
* Minimal boilerplate in code
* Schedulers can run independent tasks in parallel
* Central scheduler has UI for viewing task progress and task dependency graph
* Works backwards through task dependency graph to create all dependencies
* Won't re-run task if its outputs already exist

Overall Luigi gives an simple and efficient way to run complex task workflows.

## Super Luigi Benefits

Super Luigi has all the benefits of Luigi, but additionally adds:

* Task Forcing (i.e. option to re-run parts of the task graph)
* Task Debugging (i.e. set breakpoints in task source code)
* Task Hashes (i.e. unique yet _re-creatable_ identifier for task)
* Target Lineage Tracking (i.e. adds a lineage metadata file alongside all targets)
* Local Syncing of Remote Targets (e.g. download/upload to/from S3 automatically)

Overall Super Luigi improves the developer experience and makes common tasks simpler.

## Installation

* `pip install -e .`

## Getting Started

```python
import pandas as pd
import joblib
from datetime import date

from superluigi.tasks import AtomicTaskWithMetadata
from superluigi.parameters import DateParameter, IntParameter
from superluigi.targets import S3ObjectTarget
from superluigi.task_runner import run_task

from ... import FeaturesTask, ModelTask


# define a task to make predictions for a given date and model version
class PredictionsTask(AtomicTaskWithMetadata):
    date = DateParameter()
    version = IntParameter()

    def requires(self):
        return {
            'features': FeaturesTask(date=self.date, version=self.version),
            'model': ModelTask(version=self.version)
        }
    
    def output(self):
        """
        S3ObjectTarget will automatically upload to S3,
        but also keep the output cached locally, to avoid unnecessary downloads from S3.
        You can reference parameters and/or task_id (which is a unique hash of the task
        and its dependencies) in the path. Useful for when there are a large number of
        parameters and you don't want to add them all to the path.
        """
        return S3ObjectTarget(
            bucket='superluigi',
            object_key=f'data/predictions/date={self.date:%Y%m%d}/{self.task_id}.csv'
        )

    def run(self, input_path, output_path, metadata):
        """
        Contains logic to create the task output given the task inputs
        
        `input_path` are temporary paths that contain the output of the dependencies
        `output_path` is a temporary path where you should store the output
        `metadata` is a dictionary that contains useful task lineage metadata,
            but it can be added to if you want to add custom metadata. e.g. see `row_count`.
        """
        df = pd.read_csv(input_path['features'])
        metadata["row_count"] = len(df)
        model = joblib.load(input_path['model'])
        preds = model.predict(df)
        preds.to_csv(output_path)


# specify which data and model version to use
task = PredictionsTask(date=date(2022, 12, 15), version=4)

# run the task to make the predictions
# if the model or features have already been created, they won't be re-run.
run_task(task)
# will now have the following objects in `superluigi` S3 bucket from this task:
# `data/predictions/date=20211203/73a90acaae2b1ccc0e969709665bc62f.csv`
# `data/predictions/date=20211203/73a90acaae2b1ccc0e969709665bc62f.csv.metadata.json`

# won't run the task again if the output already exists.
run_task(task)
# can force re-run of the task.
run_task(task, force=True)
# can force re-run of all tasks in the task graph.
run_task(task, force_upstream=True)
# can force re-run of certain branches of the task graph.
run_task(task, force_upstream_to_task_cls=FeaturesTask)

# can debug tasks by setting 'breakpoint on exception' with debugger and then using:
run_task(task, raise_exception=True)
```

See tutorial at `tutorial/superluigi_tutorial.py` for more details.
