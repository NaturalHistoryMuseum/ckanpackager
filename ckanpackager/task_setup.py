from flask.config import Config
from celery.utils.log import get_task_logger
from ckanpackager.tasks.url_package_task import UrlPackageTask
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask
from ckanpackager.tasks.dwc_archive_package_task import DwcArchivePackageTask

config = Config(__file__)
config.from_object('ckanpackager.config_defaults')
config.from_envvar('CKANPACKAGER_CONFIG')

from celery import Celery

app = Celery('tasks', broker=config['CELERY_BROKER'])
app.conf.CELERY_DISABLE_RATE_LIMITS = True
app.conf.CELERY_ACCEPT_CONTENT = ['json']
app.conf.CELERY_TASK_SERIALIZER = 'json'
app.conf.CELERY_CREATE_MISSING_QUEUES = True
app.conf.CELERY_DEFAULT_QUEUE = 'slow'


@app.task
def run_task(task, request):
    """ Run/enqueue the given task for the given request
   
    Note that the request should be validated before
    this is called.
 
    @param task: Name of the task. One of package_url,
                 package_dwc_archive or package_datastore
    @param request: Dictinary containing the request
    """
    logger = get_task_logger(__name__)
    if task == 'package_url':
        UrlPackageTask(request, config).run(logger)
    elif task == 'package_dwc_archive':
        DwcArchivePackageTask(request, config).run(logger)
    elif task == 'package_datastore':
        DatastorePackageTask(request, config).run(logger)


def add_task(queue, task, request):
    """ Enqueue the given task on the given jobs queue
 
    Note that the request should be validated before this
    is called.

    @param queue: Queue to add this to. One of 'slow' or 'fast'
    @param task: Name of the tak. One of package_url,
                 package_dwc_archive or package_datastore
    @param request: Dictionary containing the request
    """ 
    run_task.apply_async(
        args=[task, request],
        queue=queue
    )
