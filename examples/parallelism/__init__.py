import time
from engine import Task, register, PipelineExecutor

@register()
class FirstWaveTask(Task):
    def perform_task(self):
        print_task_start(self)
        time.sleep(2)
        print_task_end(self)

@register(depends_on=FirstWaveTask)
class SecondWaveTaskOne(Task):
    def perform_task(self):
        print_task_start(self)
        time.sleep(4)
        print_task_end(self)

@register(depends_on=FirstWaveTask)
class SecondWaveTaskTwo(Task):
    def perform_task(self):
        print_task_start(self)
        time.sleep(2)
        print_task_end(self)

@register(depends_on=SecondWaveTaskOne)
class ThirdWaveTaskOne(Task):
    def perform_task(self):
        print_task_start(self)
        time.sleep(1)
        print_task_end(self)

@register(depends_on=SecondWaveTaskTwo)
class ThirdWaveTaskTwo(Task):
    def perform_task(self):
        print_task_start(self)
        time.sleep(1)
        print_task_end(self)

def print_task_start(task: Task):
    print('Starting ' + type(task).__name__)

def print_task_end(task: Task):
    print('Finished ' + type(task).__name__)

if __name__ == "__main__":
    executor = PipelineExecutor()
    starting_tasks = executor.get_independent_starting_tasks()
    # executor.run_with_thread_pool_executors(starting_tasks)
    executor.run_with_thread_pool_executors_v2(starting_tasks)
