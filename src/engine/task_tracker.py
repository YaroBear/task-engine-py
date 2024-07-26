from engine.task import Task

class TaskTracker:
    def __init__(self, task: Task):
        self.task: Task = task
        self.prerequisiteTracker: TaskTracker = None
        self.dependentTrackers: list[TaskTracker] = []