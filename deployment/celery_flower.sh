#!/bin/bash

exec /usr/lib/ckanpackager/bin/celery -A ckanpackager.task_setup.app flower --logging=error --basic_auth=admin:secret