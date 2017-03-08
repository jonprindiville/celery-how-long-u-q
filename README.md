# celery-how-long-u-q

Just playing around with some ideas for getting an understanding of the time
that certain Celery tasks spend in their queues before getting run.


## Basic usage:

    @HowLongUQ()
    @task()
    def my_cool_task(arg1, arg2, **kwargs):
        # cool stuff goes here...

_NB: your task's arguments list *must* include a `**kwargs` (doesn't matter
what you call it, but `HowLongUQ` needs to be able to shove a timestamp in
there)_

Now whenever `my_cool_task` is run, before the task executes, it will print
to the console something like:

    foo.bar.tasks.my_cool_task was queued for 1.975842s


## Advanced usage:

Printing to the console is boring! If you want to push that data to your logs
or to a stat server, you can define your own function which accepts a Celery
task and the float value of elapsed seconds (see
`HowLongUQ.default_hluq_run_hook` for an example) and pass it in as `run_hook`
at init-time.

Below is an example which rounds to the nearest millisecond and passes the
timing data to [StatsD](http://statsd.readthedocs.io/en/latest/reference.html)
(assuming that the StatsD client is configured elsewhere):

    def send_statsd_timer(the_task, elapsed_seconds):
        """ Send task queue-time data in whole milliseconds to StatsD """
        stat_name = 'tasks.queue_time.{}'.format(the_task.name.replace('.', '-'))
        milliseconds = int(elapsed_seconds * 1000)
        statsd.timing(stat_name, milliseconds)

    @HowLongUQ(run_hook=send_statsd_timer)
    @task()
    def my_cool_task(arg1, arg2, **kwargs):
        # cool stuff goes here...
