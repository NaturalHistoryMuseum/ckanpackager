import os
import multiprocessing
import time
from traceback import format_exc

class QueueClosed(Exception):
    """Exception raised when adding tasks on a queue that was closed"""
    pass


def _worker(task):
    """Function invoked to run a task"""
    logger = multiprocessing.get_logger()
    id = os.getpid()
    try:
        desc = str(task)
    except Exception as e:
        logger.error("Worker {} failed to get task description. {}".format(id, format_exc()))
        desc = '(unknown)'
    logger.info("Worker {} processing task {}".format(id, desc))
    try:
        task.run()
    except Exception as e:
        logger.error("Worker {} failed task {}. {}".format(id, desc, format_exc()))
    logger.info("Worker {} done with task {}".format(id, desc, e))
    return True


class TaskQueue():
    """A task queue

    The task queue spawns the given number of workers as sub-processes. Workers are re-started after handling a
    certain number of requests.

    Tasks are added via the 'add' method, which must provide an object implementing a method called 'run'. The str
    representation of the object will be used in the logs, so make it something short and unique.

    Important notes:
    - The 'run' method of the tasks are run in a sub-process. These should therefore be self contained and should not
    access API or objects from the parent process. If you need the task to communicate back use appropriate mechanisms
    such as multiprocessing.queue;

    - While the TaskQueue object spawns processes and threads, the object itself should only be used from a single
      thread/process;
    """
    def __init__(self, worker_count, requests_per_worker=None):
        if requests_per_worker == 0:
            requests_per_worker = None
        self._pool = multiprocessing.Pool(worker_count, maxtasksperchild=requests_per_worker)
        self._tasks = []
        self._processed_count = 0
        self._open = True

    def add(self, task):
        """Add a new task on the queue

        @param task: An object that implements a 'run' method with 0 arguments
        """
        if self._open:
            r = self._pool.apply_async(_worker, (task,))
            self._tasks.append(r)
            # Call flush here to ensure we clean up even if length/processed are never called.
            self._flush()
        else:
            raise QueueClosed()

    def length(self):
        """Returns the number of tasks in the queue that have not yet completed"""
        self._flush()
        return len(self._tasks)

    def processed(self):
        """Returns the number of tasks completed"""
        self._flush()
        return self._processed_count

    def terminate(self, timeout=5):
        """Close all the worker processes

        @param timeout: Number of seconds to wait before force stopping the tasks
        """
        if not self._open:
            return
        self._open = False
        self._pool.close()
        self._flush()
        time_waited = 0
        while len(self._tasks) > 0 and time_waited < timeout:
            time.sleep(0.1)
            time_waited += 0.1
            self._flush()
        self._pool.terminate()
        self._pool.join()

    def _flush(self):
        """Remove all the tasks that have completed and update counters"""
        new_tasks = []
        for t in self._tasks:
            if not t.ready():
                new_tasks.append(t)
            else:
                self._processed_count += 1
        self._tasks = new_tasks
