"""Test the TaskQueue class"""
import logging
import os
import multiprocessing
import Queue
import time
from ckanpackager.lib.queue import TaskQueue, QueueClosed
from nose.tools import assert_equals, assert_in, assert_not_in, assert_raises

# Defines the delay, in second, we wait for certain process related operations (eg. starting a new process, closing a
# process, etc.) to take. 1 second should be enough on any system.
PROCESS_OP_TIME = 1.0


class DummyTask:
    """A dummy task we use to test the queue"""
    def __init__(self, input_queue, output_queue, pid_queue):
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._pid_queue = pid_queue

    def run(self):
        self._pid_queue.put(os.getpid())
        # Block until there's something on the input queue. Ideally tasks should not block - but this is useful
        # for testing.
        qinput = self._input_queue.get(True)
        if qinput > 0:
            time.sleep(qinput)
        self._output_queue.put(qinput)


class TestQueue:
    @classmethod
    def setup_class(cls):
        """Ensure the multiprocessing logger doesn't output anything"""
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.CRITICAL)
        #logger.addHandler(logging.StreamHandler())

    def _empty_queue(self, queue):
        """Empty the given queue by removing items until none are left

        Also returns the number of items removed. This is more reliable way to get the count than to call qsize.

        @return: The number of items removed
        """
        count = 0
        try:
            while True:
                queue.get_nowait()
                count += 1
        except Queue.Empty:
            pass
        return count

    def setUp(self):
        """Create the queues"""
        manager = multiprocessing.Manager()
        self._input_queue = manager.Queue()
        self._output_queue = manager.Queue()
        self._pid_queue = manager.Queue()
        self._task_queue = None

    def tearDown(self):
        """Terminate the task queue"""
        if self._task_queue:
            self._task_queue.terminate(0)
            self._task_queue = None
        self._empty_queue(self._input_queue)
        self._empty_queue(self._output_queue)
        self._empty_queue(self._pid_queue)


    def test_run(self):
        """Ensure the queue takes a task and runs it"""
        self._task_queue = TaskQueue(1)
        assert_equals(self._empty_queue(self._pid_queue), 0)
        self._task_queue.add(DummyTask(self._input_queue, self._output_queue, self._pid_queue))
        time.sleep(PROCESS_OP_TIME)
        assert_equals(self._empty_queue(self._pid_queue), 1)

    def test_terminate(self):
        """Test that once terminated we cannot add tasks onto a queue"""
        self._task_queue = TaskQueue(1)
        self._task_queue.terminate(0)
        assert_raises(QueueClosed, self._task_queue.add, (None,))

    def test_terminate_timeout(self):
        """Ensure terminates gives time for the tasks to finish"""
        self._task_queue = TaskQueue(1)
        self._task_queue.add(DummyTask(self._input_queue, self._output_queue, self._pid_queue))
        assert_equals(self._empty_queue(self._output_queue), 0)
        self._input_queue.put(3)
        self._task_queue.terminate(6)
        assert_equals(self._empty_queue(self._output_queue), 1)

    def test_terminate_interupt(self):
        """Ensure terminates interupts task after timeout"""
        self._task_queue = TaskQueue(1, 0)
        self._task_queue.add(DummyTask(self._input_queue, self._output_queue, self._pid_queue))
        assert_equals(self._empty_queue(self._output_queue), 0)
        self._input_queue.put(3)
        self._task_queue.terminate(0)
        time.sleep(3 + PROCESS_OP_TIME)
        assert_equals(self._empty_queue(self._output_queue), 0)

    def test_number_of_workers(self):
        """Ensure the queue spawns required number of distinct sub-processes"""
        self._task_queue = TaskQueue(5)
        for i in range(9):
            self._task_queue.add(DummyTask(self._input_queue, self._output_queue, self._pid_queue))
        pids = [os.getpid()]
        for i in range(5):
            # Should not raise!
            pid = self._pid_queue.get(True, 1)
            assert_not_in(pid, pids)
            pids.append(pid)
        assert_equals(self._empty_queue(self._pid_queue), 0)

    def test_max_workers(self):
        """Ensure the queue spawns new workers when requests_per_worker is reached"""
        self._task_queue = TaskQueue(2, 2)
        for i in range(6):
            self._task_queue.add(DummyTask(self._input_queue, self._output_queue, self._pid_queue))
        # Get the first two pids
        pid = self._pid_queue.get(True, 1)
        pid1 = self._pid_queue.get(True, 1)
        pids = [pid, pid1]
        # Allow the tasks to finish so the next two tasks can start
        self._input_queue.put(0)
        self._input_queue.put(0)
        # Get the next two pids, and ensure they are the same ones.
        pid2 = self._pid_queue.get(True, 1)
        pid3 = self._pid_queue.get(True, 1)
        assert_in(pid2, pids)
        assert_in(pid3, pids)
        # Allow the tasks to finish
        self._input_queue.put(0)
        self._input_queue.put(0)
        # Get the next two pids - should be new ones.
        pid4 = self._pid_queue.get(True, 1)
        pid5 = self._pid_queue.get(True, 1)
        assert_not_in(pid4, pids)
        assert_not_in(pid5, pids)
