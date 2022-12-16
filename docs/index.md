## Key Concepts

When working with Super Luigi, you'll want to understand the following concepts:

* Target: an output location for a Task (e.g. `S3ObjectTarget`)
* Task: encapsulate a procedure, its dependent Tasks and its output Target

Once you have defined your tasks (and dependencies), you'll want to use the `run_task` function to start the build.

## Getting Started

We'll use an example of generating machine learning predictions for given day and model version. Assume for now we have already created:

* a task to create the input features to the model (called `FeaturesTask`)
* a task for training the model (called `ModelTask`)


```python
from tutorial_utils import FeaturesTask, ModelTask
```

We'll continue with some other imports before defining our prediction task.


```python
from superluigi.tasks import AtomicTaskWithMetadata
```

`AtomicTaskWithMetadata` isn't the simplest task class, but it's fully featured and a good default to use. More of its features will be explained later on.

Other tasks include `Task` (Luigi's base task), `ExternalTask` and `TaskWithMetadata`.


```python
from superluigi.parameters import DateParameter, IntParameter
```

Since we want predictions for a given day (date type) and a given model version (integer type).


```python
from superluigi.targets import S3ObjectTarget
```

Other targets include `LocalFileTarget`, `LocalFolderTarget` and `S3PrefixTarget`.

### Defining `PredictionsTask`

We're now ready to define our prediction task called `PredictionsTask`.


```python
import pandas as pd
import joblib


class PredictionsTask(AtomicTaskWithMetadata):
    # parameters of the task
    date = DateParameter()
    version = IntParameter()
    # luigi requires parameters set like this instead of via constructor (e.g. __init__).
    # supports additional features like ensuring tasks with same params are singletons

    def requires(self):
        # dependencies of the task
        # can return None, a single task, a list or dictionary of tasks.
        # notice you can reference the parameters when defining the dependencies
        return {
            'features': FeaturesTask(date=self.date, version=self.version),
            'model': ModelTask(version=self.version)
        }
    
    def output(self):
        # can return None, a single target, a list or dictionary of targets
        # but it's highly recommended to return a single target
        # notice you can reference the parameters when defining the target
        return S3ObjectTarget(
            bucket='superluigi',
            object_key=f'data/date={self.date:%Y%m%d}/version={self.version}/predictions.csv'
        )
        # you can also reference self.task_id for a unique but re-creatable id for the task
        # useful for when there are a large number of parameters and you don't want to add them all to the path

    def run(self, input_path, output_path, metadata):
        # logic to create the task output given the task inputs
        # `input_path` are temporary paths that contain the output of the dependencies
        # `output_path` is a temporary path where you should store the output
        # `metadata` is a dictionary that contains task lineage metadata and can be added to
        df = pd.read_csv(input_path['features'])
        metadata["row_count"] = len(df)
        model = joblib.load(input_path['model'])
        preds = model.predict(df)
        preds.to_csv(output_path)
```

Some important features to highlight:

* You specify dependencies as tasks, rather than file/folder paths. When the output of a task you depend on doesn't exist, Super Luigi is able to create it (assuming that dependent task has a `run` method defined). If you just provided file/folder paths this wouldn't be possible. An added benefit is that it also avoids duplication of paths. You only need to define the path once as an output of the task, then other tasks that depend on that task can retrieve the path.
* All the boilerplate code for dealing with the file/storage system is abstracted away inside the target. As an example, `S3ObjectTarget` handles moving the output file from a local temporary directory (to a local cache and then) to the defined location in S3. When a subsequent task needs to use this output, the cache is checked first, and if the cached outputs are out of sync with S3, the outputs are pulled from S3.
* `AtomicTaskWithMetadata` handles the atomic write problem: i.e. leaving partially written files upon task failure. It's an issue for Luigi/Super Luigi because these files may make the task appear successful and complete to tasks that depend on it in the future. `AtomicTaskWithMetadata` solves this by creating a temporary path that can be referenced using `output_path` in the `run` method, and this temporary path will be moved to the correct location at the very end. A path move is much more likely to be atomic that the initial write(s) from the task. Additionally `input_path` provides temporary paths containing the input data, reducing the risk of corrupting the input data (e.g. running `shutil.rmtree()` in the `run` method!).

### Running `PredictionsTask`

So far we have created a class for the `PredictionsTask`, but we need an instance of the task to actually run.

Since we defined two parameters (`date` and `version`) we need to specify these when creating the task instance.


```python
from datetime import date

task = PredictionsTask(date=date(2022, 12, 15), version=4)
```

One of the most important functions in Super Luigi is `run_task`.


```python
from superluigi.task_runner import run_task
```

It's the entry point for creating the task and its dependencies (as required).

It has a number of useful parameters for configuring how the graph is executed, but we'll stick to the basics for now.


```python
run_task(task)
```

    ===== Luigi Execution Summary =====
    
    Scheduled 3 tasks of which:
    * 3 ran successfully:
        - 1 FeaturesTask(date=2021-12-03, version=4)
        - 1 ModelTask(version=4)
        - 1 PredictionsTask(date=2021-12-03, version=4)
    
    This progress looks :) because there were no failed tasks or missing dependencies
    
    ===== Luigi Execution Summary =====
    


    Ouputs can be found at s3://superluigi/data/date=20211203/version=4/predictions.csv


When running this task for the first time, we can see that 3 tasks were run successfully. We only provided a single task to run, but Super Luigi identified that its two dependencies didn't exists and so ran those both first. 

When executing `run_task` again, we see something different.


```python
run_task(task)
```

    ===== Luigi Execution Summary =====
    
    Scheduled 1 tasks of which:
    * 1 complete ones were encountered:
        - 1 PredictionsTask(date=2021-12-03, version=4)
    
    Did not run any tasks
    This progress looks :) because there were no failed tasks or missing dependencies
    
    ===== Luigi Execution Summary =====
    


    Ouputs can be found at s3://superluigi/data/date=20211203/version=4/predictions.csv


No tasks were executed this time because the output of the provided task already exists: we didn't need to run it.

### Re-Running `PredictionsTask`

Assume for a moment that we made a mistake in the `run` method of the `PredictionsTask`.

We can make the fix to the code, but `run_task` won't run the task again because its output already exists.

We can force tasks to re-run by using the following arguments to `run_task`:

* `force` (`bool`): Will force the task to run (even when its targets already exist).
* `force_upstream` (`bool`): Will force the task to run and all of its upstream dependencies too (even when their targets already exist).
* `force_upstream_to_task` or `force_upstream_to_task_cls` for partial forcing of task graph.


```python
run_task(task, force=True)
```

    ===== Luigi Execution Summary =====
    
    Scheduled 3 tasks of which:
    * 2 complete ones were encountered:
        - 1 FeaturesTask(date=2021-12-03, version=4)
        - 1 ModelTask(version=4)
    * 1 ran successfully:
        - 1 PredictionsTask(date=2021-12-03, version=4)
    
    This progress looks :) because there were no failed tasks or missing dependencies
    
    ===== Luigi Execution Summary =====
    


    Ouputs can be found at s3://superluigi/data/date=20211203/version=4/predictions.csv


Assuming we made a mistake with an upstream task, e.g. `FeaturesTask`, we could force using `force_upstream_to_task_cls` like so:


```python
run_task(task, force_upstream_to_task_cls=FeaturesTask)
```

    ===== Luigi Execution Summary =====
    
    Scheduled 3 tasks of which:
    * 1 complete ones were encountered:
        - 1 ModelTask(version=4)
    * 2 ran successfully:
        - 1 FeaturesTask(date=2021-12-03, version=4)
        - 1 PredictionsTask(date=2021-12-03, version=4)
    
    This progress looks :) because there were no failed tasks or missing dependencies
    
    ===== Luigi Execution Summary =====
    


    Ouputs can be found at s3://superluigi/data/date=20211203/version=4/predictions.csv


## Summary

In this tutorial, we saw how to define a task class, create a task and run it. When defining the task class: we specified parameters, defined the dependencies in `requires`, defined the output target in `output` and placed the code to create the task in `run`. Once the task class was created, we created an instance of it, before running the task using `run_task` (with optional forcing).
