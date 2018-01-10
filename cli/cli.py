# -*- coding: utf-8 -*-
"""
RQ command line tool
通过在rq命令(实际上为一个__main__模块来导入), 入口为main函数
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from functools import update_wrapper
import os
import sys

import click
from redis.exceptions import ConnectionError

from rq import Connection, get_failed_queue, __version__ as version
from rq.cli.helpers import (read_config_file, refresh,
                            setup_loghandlers_from_args,
                            show_both, show_queues, show_workers, CliConfig)
from rq.contrib.legacy import cleanup_ghosts
from rq.defaults import (DEFAULT_CONNECTION_CLASS, DEFAULT_JOB_CLASS,
                         DEFAULT_QUEUE_CLASS, DEFAULT_WORKER_CLASS)
from rq.exceptions import InvalidJobOperationError
from rq.utils import import_attribute
from rq.suspension import (suspend as connection_suspend,
                           resume as connection_resume, is_suspended)
from ..debug import MY


# Disable the warning that Click displays (as of Click version 5.0) when users
# use unicode_literals in Python 2.
# See http://click.pocoo.org/dev/python3/#unicode-literals for more details.
click.disable_unicode_literals_warning = True


shared_options = [
    click.option('--url', '-u',
                 envvar='RQ_REDIS_URL',
                 help='URL describing Redis connection details.'),
    click.option('--config', '-c',
                 envvar='RQ_CONFIG',
                 help='Module containing RQ settings.'),
    click.option('--worker-class', '-w',
                 envvar='RQ_WORKER_CLASS',
                 default=DEFAULT_WORKER_CLASS,
                 help='RQ Worker class to use'),
    click.option('--job-class', '-j',
                 envvar='RQ_JOB_CLASS',
                 default=DEFAULT_JOB_CLASS,
                 help='RQ Job class to use'),
    click.option('--queue-class',
                 envvar='RQ_QUEUE_CLASS',
                 default=DEFAULT_QUEUE_CLASS,
                 help='RQ Queue class to use'),
    click.option('--connection-class',
                 envvar='RQ_CONNECTION_CLASS',
                 default=DEFAULT_CONNECTION_CLASS,
                 help='Redis client class to use'),
    click.option('--path', '-P',
                 default='.',
                 help='Specify the import path.',
                 multiple=True)
]


def pass_cli_config(func):
    """
    1 装饰器, 其中wraps是update_wrapper的特殊用法;
        wraps/update_wrapper会消除正常wrapper的副作用, 例如func.__name__
    2 传递参数配置到子命令中
    """
    # add all the shared options to the command
    # 1 添加默认的选项
    for option in shared_options:
        func = option(func)

    # pass the cli config object into the command
    # 将cli传入各个子命令中
    def wrapper(*args, **kwargs):
        ctx = click.get_current_context()
        cli_config = CliConfig(**kwargs)
        return ctx.invoke(func, cli_config, *args[1:], **kwargs)

    return update_wrapper(wrapper, func)


# 参考:http://click.pocoo.org/5/
# 利用group实现子命令: rq worker [OPTIONS], worker/empty/info/requeue/resume等子命令
@click.group()
@click.version_option(version)
def main():
    """RQ command line tool.
    click: 用于快速构建命令行
    """
    # 运行时生效
    click.echo('RQ Main command entry.')
    #  pass


@main.command()
@click.option('--all', '-a', is_flag=True, help='Empty all queues')
@click.argument('queues', nargs=-1)
@pass_cli_config
def empty(cli_config, all, queues, **options):
    """Empty given queues."""

    if all:
        queues = cli_config.queue_class.all(connection=cli_config.connection,
                                            job_class=cli_config.job_class)
    else:
        queues = [cli_config.queue_class(queue,
                                         connection=cli_config.connection,
                                         job_class=cli_config.job_class)
                  for queue in queues]

    if not queues:
        click.echo('Nothing to do')
        sys.exit(0)

    for queue in queues:
        num_jobs = queue.empty()
        click.echo('{0} jobs removed from {1} queue'.format(num_jobs, queue.name))


@main.command()
@click.option('--all', '-a', is_flag=True, help='Requeue all failed jobs')
@click.argument('job_ids', nargs=-1)
@pass_cli_config
def requeue(cli_config, all, job_class, job_ids, **options):
    """Requeue failed jobs."""

    failed_queue = get_failed_queue(connection=cli_config.connection,
                                    job_class=cli_config.job_class)

    if all:
        job_ids = failed_queue.job_ids

    if not job_ids:
        click.echo('Nothing to do')
        sys.exit(0)

    click.echo('Requeueing {0} jobs from failed queue'.format(len(job_ids)))
    fail_count = 0
    with click.progressbar(job_ids) as job_ids:
        for job_id in job_ids:
            try:
                failed_queue.requeue(job_id)
            except InvalidJobOperationError:
                fail_count += 1

    if fail_count > 0:
        click.secho('Unable to requeue {0} jobs from failed queue'.format(fail_count), fg='red')


@main.command()
@click.option('--interval', '-i', type=float, help='Updates stats every N seconds (default: don\'t poll)')
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')
@click.option('--only-queues', '-Q', is_flag=True, help='Show only queue info')
@click.option('--only-workers', '-W', is_flag=True, help='Show only worker info')
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')
@click.argument('queues', nargs=-1)
@pass_cli_config
def info(cli_config, interval, raw, only_queues, only_workers, by_queue, queues,
         **options):
    """RQ command-line monitor."""

    if only_queues:
        func = show_queues
    elif only_workers:
        func = show_workers
    else:
        func = show_both

    try:
        with Connection(cli_config.connection):
            refresh(interval, func, queues, raw, by_queue,
                    cli_config.queue_class, cli_config.worker_class)
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)


@main.command()
@click.option('--burst', '-b', is_flag=True, help='Run in burst mode (quit after all work is done)')
@click.option('--logging_level', type=str, default="INFO", help='Set logging level')
@click.option('--name', '-n', help='Specify a different name')
@click.option('--results-ttl', type=int, help='Default results timeout to be used')
@click.option('--worker-ttl', type=int, help='Default worker timeout to be used')
@click.option('--verbose', '-v', is_flag=True, help='Show more output')
@click.option('--quiet', '-q', is_flag=True, help='Show less output')
@click.option('--sentry-dsn', envvar='SENTRY_DSN', help='Report exceptions to this Sentry DSN')
@click.option('--exception-handler', help='Exception handler(s) to use', multiple=True)
@click.option('--pid', help='Write the process ID number to a file at the specified path')
@click.argument('queues', nargs=-1)
@pass_cli_config
def worker(cli_config, burst, logging_level, name, results_ttl,
           worker_ttl, verbose, quiet, sentry_dsn, exception_handler,
           pid, queues, **options):
    """Starts an RQ worker."""

    settings = read_config_file(cli_config.config) if cli_config.config else {}
    MY(0, __file__, 'Worker命令最后的实际参数:', settings)
    # Worker specific default arguments
    queues = queues or settings.get('QUEUES', ['default'])
    sentry_dsn = sentry_dsn or settings.get('SENTRY_DSN')

    if pid:
        with open(os.path.expanduser(pid), "w") as fp:
            fp.write(str(os.getpid()))

    setup_loghandlers_from_args(verbose, quiet)

    try:

        cleanup_ghosts(cli_config.connection)
        exception_handlers = []
        for h in exception_handler:
            exception_handlers.append(import_attribute(h))

        if is_suspended(cli_config.connection):
            click.secho('RQ is currently suspended, to resume job execution run "rq resume"', fg='red')
            sys.exit(1)

        # 1 初始化 Queues 对象, 表示一个队列, 类名(rq.queue.Queue)
        # 1.1 涉及的队列: rq:queues, rq:queue:NAME
        # 1.2 默认情况下, 某一个队列, 例如bamboo, 会在两个地方都进行了实例化
        #   1.2.1 client: 实例化Queue, 并调用enqueue 
        #   1.2.2 server: 实例化 Queue, 调用blpop获取task, 实际运行时不会主动创建key
        #   1.2.3 注意: job 在enqueue_call时调用并实例化(create), 见rq/queue.py
        queues = [cli_config.queue_class(queue,
                                         connection=cli_config.connection,
                                         job_class=cli_config.job_class)
                  for queue in queues]
        # 2 初始化worker进程, 类名(rq.worker.Worker)
        # 2.1 初始化workers
        # 2.2 校验queues
        # 2.3 初始化failed_queue(rq:queue:failed)
        worker = cli_config.worker_class(queues,
                                         name=name,
                                         connection=cli_config.connection,
                                         default_worker_ttl=worker_ttl,
                                         default_result_ttl=results_ttl,
                                         job_class=cli_config.job_class,
                                         queue_class=cli_config.queue_class,
                                         exception_handlers=exception_handlers or None)
        MY(1, __file__,
           '\n\tQueues object:', queues, 
           '\n\tWorkers object:', worker,
           '\n\tJobs class:', [queue.job_class for queue in queues])

        # Should we configure Sentry?
        if sentry_dsn:
            from raven import Client
            from raven.transport.http import HTTPTransport
            from rq.contrib.sentry import register_sentry
            client = Client(sentry_dsn, transport=HTTPTransport)
            register_sentry(client, worker)

        # 3 开启正式处理
        # 3.1 register_birth:设置rq:worker:PC-NAME-PID, rq:worker:QUEUE_NAME
        # 3.2 进行loop和heartbeat循环
        worker.work(burst=burst, logging_level=logging_level)
    except ConnectionError as e:
        print(e)
        sys.exit(1)


@main.command()
@click.option('--duration', help='Seconds you want the workers to be suspended.  Default is forever.', type=int)
@pass_cli_config
def suspend(cli_config, duration, **options):
    """Suspends all workers, to resume run `rq resume`"""

    if duration is not None and duration < 1:
        click.echo("Duration must be an integer greater than 1")
        sys.exit(1)

    connection_suspend(cli_config.connection, duration)

    if duration:
        msg = """Suspending workers for {0} seconds.  No new jobs will be started during that time, but then will
        automatically resume""".format(duration)
        click.echo(msg)
    else:
        click.echo("Suspending workers.  No new jobs will be started.  But current jobs will be completed")


@main.command()
@pass_cli_config
def resume(cli_config, **options):
    """Resumes processing of queues, that where suspended with `rq suspend`"""
    connection_resume(cli_config.connection)
    click.echo("Resuming workers.")
