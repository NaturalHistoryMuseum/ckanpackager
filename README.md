ckanpackager
============

Overview
--------

A stand-alone service to pack a given CKAN resource in a ZIP file and email the link to a user.

**This is work in progress and is not currently functional**

Usage
-----

The application is under development and does yet contain a WSGI wrapper. Run manually by doing:

`CKANPACKAGER_CONFIG=[path to config file] python ckanpackager/application.py`

Once started you can make HTTP request on the host/port with the following URLs:

- _/status_ to obtain the status as a json object. Required parameters: `secret`;
- _/package_ to get a package created and a link emailed. Required parameters: `secret`, `resource_id` and `email`.
  Optional parameters: `filters`, `q`, `limit`.


Configuration
-------------

The configuration file is a python file providing the following settings:
- `HOST`: Set this to the hostname. 127.0.0.1 will serve on localhost only. Use 0.0.0.0 (or a specific interface IP) 
to server externally;
- `POST`: Port number;
- `WORKERS`: Number of worker processes. Each process will serve at most one request at a given time;
- `REQUESTS_PER_WORKER`: Number of requests before a worker is restarted;
- `SECRET`: Key that must be included in requests.
- `DEBUG`: Set to TRUE to enable debug mode.