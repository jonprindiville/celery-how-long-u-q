# -*- coding: utf-8 -*-
import datetime
from functools import wraps


class HowLongUQ(object):
    """
    Decorator for use with Celery tasks to enable measurement of time waiting
    in queue.

    NB: your task's arguments list *must* include a **kwargs (doesn't matter
    what you call it, but we need to be able to put our timestamp in there)

    Basic usage:

        @HowLongUQ()
        @task()
        def my_cool_task(arg1, arg2, **kwargs):
            pass

    Now whenever my_cool_task is run, before the task executes, it will print
    to the console something like:

        foo.bar.tasks.my_cool_task was queued for 1.975842s

    Printing to the console is not that cool, though. If you want to push that
    data to your logs or to a stat server, you can define your own function
    which accepts a Celery task and the float value of elapsed seconds (see
    default_hluq_run_hook for an example) and pass it in as run_hook at init-time:

        @HowLongUQ(run_hook=send_statsd_timer)
        @task()
        def my_cool_task(arg1, arg2, **kwargs):
            pass

    CAVEATS: Negative time intervals (or extra phantom time insertion) are
    possible if a leap second happens between your task being enqueued and your
    task being run. Also, keep in mind that the system clocks of the enqueuing
    and running systems may not be exactly synchronized -- NTP is not magic.
    """
    TIMESTAMP_KWARG = '_hluq_timestamp'

    @staticmethod
    def default_hluq_run_hook(the_task, elapsed_seconds):
        """
        Dumb default run hook, will print the task name and the elapsed seconds
        since the task was enqueued.

        Maybe you want to replace this with something that talks to statds or
        logger.
        """
        print '{} was queued for {}s'.format(the_task.name, elapsed_seconds)

    def __init__(self, run_hook=None):
        """
        Given run_hook, a callable that accepts (task, seconds) a Celery task
        and a decimal number of seconds elapsed, produces a decorator that can
        be used to wrap a Celery task.

        When calling .delay or .apply_async on the task the wrapped will
        include a timestamp in the tasks kwargs. When the task is actually run,
        the wrapper will compare that timestamp with the current time and call
        run_hook with the task and the elapsed time.

        You can use this to log how long your Celery task has spend in its
        queue before being executed. (e.g. just log to disk, push timers to
        graphite, etc)
        """
        self.hluq_run_hook = run_hook or self.default_hluq_run_hook
        self._original_delay = None
        self._original_apply_async = None
        self._original_run = None

    def __call__(self, the_task):
        """ Wrap delay, apply_async, run on the_task """

        # Wrap the delay method
        self._original_delay = the_task.delay
        @wraps(self._original_delay)
        def new_delay(*args, **kwargs):
            """ Wrap the original .delay, including a timestamp kwarg """
            if self.TIMESTAMP_KWARG not in kwargs:
                kwargs[self.TIMESTAMP_KWARG] = datetime.datetime.utcnow()
            self._original_delay(*args, **kwargs)
        the_task.delay = new_delay

        # Wrap the apply_async method
        self._original_apply_async = the_task.apply_async
        @wraps(self._original_apply_async)
        def new_apply_async(args=None, kwargs=None, **celery_options):
            """ Wrap the original .apply_async, including a timestamp kwarg """
            kwargs = kwargs or {}
            if self.TIMESTAMP_KWARG not in kwargs:
                kwargs[self.TIMESTAMP_KWARG] = datetime.datetime.utcnow()
            self._original_apply_async(args, kwargs, **celery_options)
        the_task.apply_async = new_apply_async

        # Wrap the run method
        self._original_run = the_task.run
        @wraps(self._original_run)
        def new_run(*args, **kwargs):
            """
            Wrap the original .run, extracting the timestamp kwarg that was
            inserted by our wrapped .delay or .apply_async method and call the
            hook with the elapsed time.

            We'll also remove the timestamp kwarg before allowing the original
            .run method to execute.
            """
            if self.TIMESTAMP_KWARG in kwargs:
                time_delta = datetime.datetime.utcnow() - kwargs[self.TIMESTAMP_KWARG]
                del kwargs[self.TIMESTAMP_KWARG]
                self.hluq_run_hook(the_task, time_delta.total_seconds())
            self._original_run(*args, **kwargs)
        the_task.run = new_run

        return the_task
