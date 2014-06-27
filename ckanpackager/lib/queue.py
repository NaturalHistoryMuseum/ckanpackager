import os
import time
import Queue
import multiprocessing
from threading import Thread, Event
from multiprocessing import Process, get_logger


def _worker(requests_per_worker, _queue):
    """Worker process"""
    requests_processed = 0
    worker_id = os.getpid()
    logger = get_logger()
    logger.info("Worker process {id} started".format(id=worker_id))
    while requests_per_worker == 0 or requests_processed < requests_per_worker:
        (cmd, task) = _queue.get(True)
        if cmd == 'process':
            logger.info("Worker process {id} processing task {task} ({count} of {total})".format(
                id=worker_id,
                task=task,
                count=requests_processed+1,
                total=requests_per_worker
            ))
            try:
                task.run()
            except Exception as e:
                logger.error("Worker process {id} failed to process task {task}: {error}".format(
                    id=worker_id,
                    task=task,
                    error=str(e)
                ))
            else:
                logger.info("Worker process {id} finished processing task {task}.".format(
                    id=worker_id,
                    task=task
                ))
            requests_processed += 1
        elif cmd == 'terminate':
            break
        else:
            logger.error("Worker process {id} got unknown command: {cmd}".format(id=worker_id, cmd=cmd))
    logger.info("Worker process {id} terminating.".format(id=worker_id))


class TerminationError(Exception):
    """Exception raised when 'terminate' fails to stop the processes/thread"""
    pass


class TaskQueue(object):
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

    - In some cases there might be a small delay in running a task, even if not all workers are busy. This is acceptable
      as the TaskQueue is meant for slow background processes.
    """
    def __init__(self, worker_count=10, requests_per_worker=0):
        """Create a new task queue

        @param worker_count: Number of workers
        @param requests_per_worker: Number of requests per worker
        """
        # Init variables
        self._pool = []
        self._worker_count = worker_count
        self._requests_per_worker = requests_per_worker
        self._added_to_queue = 0
        self._queue = multiprocessing.Queue()
        # Start populating thread
        self._stop_populate = Event()
        self._thread = Thread(target=self._populate)
        self._thread.start()

    def length(self):
        """Return the queue size (approx)"""
        return self._queue.qsize()

    def processed(self):
        """Return number of tasks processed (approx)"""
        return self._added_to_queue - self._queue.qsize()

    def add(self, task):
        """Add a new task on the queue

        @param task: A task object. This object should have a 'run' method that takes no argument. The 'run' method
        should not access Flask API, and should use multiprocessing for logging.
        """
        if self._worker_count == 0:
            return
        self._queue.put(('process', task))
        self._added_to_queue += 1

    def _populate(self):
        """Thread which checks for finished processes and spawns new workers"""
        while not self._stop_populate.isSet():
            new_pool = []
            for p in self._pool:
                if p.is_alive():
                    new_pool.append(p)
            self._pool = new_pool
            # Create new workers if needed
            while len(self._pool) < self._worker_count:
                p = Process(target=_worker, args=(self._requests_per_worker, self._queue))
                p.start()
                self._pool.append(p)
            # Wait a bit
            time.sleep(0.250)

    def terminate(self, timeout=5.0, force=True):
        """Close all the worker processes.

        @param timeout: Time to wait for the processes to finish gracefully.
        @force: If True, will force terminate processes. If False will raise if the processes haven't terminated.
        """
        # Ensure no more jobs get added and flush the queue of jobs.
        worker_count = self._worker_count
        self._worker_count = 0
        try:
            while True:
                self._queue.get_nowait()
        except Queue.Empty:
            pass

        # Stop the populate thread
        self._stop_populate.set()
        self._thread.join(2)
        if self._thread.is_alive():
            raise TerminationError()

        # Ask all the workers to finish once they've done their current job.
        for i in range(worker_count):
            # Each process should get this command no more than once - so set one for each worker.
            self._queue.put(('terminate', None))

        # Wait until they've stopped or we've reached our time out.
        time_spent = 0
        while time_spent < timeout and len(self._pool) > 0:
            time.sleep(0.1)
            time_spent += 0.1
            new_pool = []
            for p in self._pool:
                if p.is_alive():
                    new_pool.append(p)
            self._pool = new_pool

        # Force stop the remaining workers.
        logger = get_logger()
        for p in self._pool:
            if p.is_alive():
                if force:
                    logger.error("QueueTask: Force terminating worker {}".format(p.pid))
                    p.terminate()
                else:
                    raise TerminationError()

    def is_terminated(self):
        """Checks if the queue is terminated.

        @return: true if the thread and all processes have stopped.
        """
        if self._thread.is_alive():
            return False
        for p in self._pool:
            if p.is_alive():
                return False
        return True