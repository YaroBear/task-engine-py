from collections import defaultdict, deque
from typing import Dict

from engine.config_provider import get_default_config_provider
from engine.task_tracker import TaskTracker
from reporters import plan_reporters

import concurrent.futures


tasks_registry = {}
tasks_dependencies = defaultdict(list[str])
context: Dict = dict()

config_provider = get_default_config_provider()
context.update(config_provider.get_config())

def run_with_thread_pool_executor(taskTracker: TaskTracker):
    taskTracker.task.perform_task()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_with_thread_pool_executor, tracker): tracker for tracker in taskTracker.dependentTrackers}
        concurrent.futures.as_completed(futures)

def run_task(taskTracker: TaskTracker) -> list[TaskTracker]:
    taskTracker.task.perform_task()

    return taskTracker.dependentTrackers

class PipelineExecutor:
    def __init__(self):
        self.config_provider = get_default_config_provider()
        self.tasks = tasks_registry
        self.dependencies = tasks_dependencies
        self.execution_order = self.build_execution_order()
        self.report_plan()

    def build_execution_order(self):
        in_degree = {key: 0 for key in self.tasks}
        for task, deps in self.dependencies.items():
            for dep in deps:
                in_degree[task] += 1

        # tasks with no dependencies
        queue = deque([task for task in self.tasks if in_degree[task] == 0])
        execution_order = []

        # Process tasks with in-degree of 0
        while queue:
            task = queue.popleft()
            execution_order.append(task)

            # Reduce the in-degree of dependent tasks
            for dep in self.tasks:
                if task in self.dependencies[dep]:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        queue.append(dep)

        if len(execution_order) != len(self.tasks):
            raise Exception("Circular dependency detected")
        
        return execution_order
    
    def get_independent_starting_tasks(self):
        task_trackers: dict[str, TaskTracker] = {task_name: TaskTracker(task) for task_name, task in self.tasks.items()}
        for task_name, dependency_names in self.dependencies.items():
            dependent_task_tracker = task_trackers[task_name]
            for dependency_name in dependency_names:
                prerequisite_task_tracker = task_trackers[dependency_name]
                prerequisite_task_tracker.dependentTrackers.append(dependent_task_tracker)
                dependent_task_tracker.prerequisiteTracker = prerequisite_task_tracker

        independent_tasks = [task_tracker 
                        for task_tracker in task_trackers.values() 
                        if task_tracker.prerequisiteTracker is None]

        # TODO check for circular deps

        return independent_tasks
    
    def report_plan(self):
        for reporter in plan_reporters:
            reporter.report(self.execution_order)

    def run(self):
        for task_name in self.execution_order:
            task = self.tasks[task_name]
            print(f"Executing {task_name}")
            task.perform_task()

    def run_with_thread_pool_executors(self, starting_task_trackers: list[TaskTracker]):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(run_with_thread_pool_executor, tracker): tracker for tracker in starting_task_trackers}
            concurrent.futures.as_completed(futures)

    # This is probably the better implementation
    # Only uses one thread pool executor, so prevents potentially exponential increase
    # in number of thread pool executors used.
    # Adapted from https://stackoverflow.com/a/51883938
    def run_with_thread_pool_executors_v2(self, starting_task_trackers: list[TaskTracker]):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(run_task, tracker) for tracker in starting_task_trackers]

            while futures:
                # As mentioned in stack overflow article,
                # This for loop continues when one of the "future" objects
                # completes, not after all of them have.
                for future in concurrent.futures.as_completed(futures):
                    futures.remove(future)

                    for taskDependency in future.result():
                        futures.append(executor.submit(run_task, taskDependency))

def register(depends_on=None):
    """Decorator to register a task class in the tasks registry"""
    def decorator(cls):
        key = cls.__name__
        if tasks_registry.get(key):
            raise Exception(f"Duplicate task name: {key}")

        tasks_registry[key] = cls(context)
        if depends_on:
            tasks_dependencies[key].append(depends_on.__name__)
        return cls
    return decorator
