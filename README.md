HERMES stream server
=====================

This software contains the stream server infrastructure
for the HERMES project.


Installation
--------------

This software runs on Linux.
In order to deploy it you need `python 2.7` with its development headers
and `virtualenv`.
Additionally, you need `curl` and its development packages,
as well as the `nginx` web server
and the git version control system.
Install all of them through your distribution's package manager.
For example, in Debian and Ubuntu:

```shell
sudo apt-get install python2.7 python2.7-dev python-virtualenv curl libcurl3 libcurl4-openssl-dev nginx-light git
```

Then, create a directory anywhere within your home account.
It will be referred in this guide as your installation's directory.
You will install the software's sources and all its python dependencies there,
within a `virtualenv` environment.
Place a terminal in that directory and enter the following commands:

```shell
virtualenv venv
. venv/bin/activate
pip install --upgrade setuptools
git clone https://github.com/hermes-smartcity/streamserver.git
cd streamserver
pip install -r requirements.txt
python setup.py install
cd ..
deactivate
```

After that, you need to configure the `nginx` web server,
which will act as a proxy server between the mobile apps
and the frontend servers:

```shell
sudo cp streamserver/deployment/nginx-experimental /etc/nginx/sites-available
cd /etc/nginx/sites-enabled
sudo ln -s ../sites-available/nginx-experimental .
cd -
sudo service nginx restart
```

You may want to configure some parameters in `/etc/nginx/nginx.conf` in order
to handle higher traffic loads.
For example, it's worth increasing
the `worker_processes` parameter (e.g. to 8)
the `worker_connections` parameter (e.g. to 2048)
as well as the `worker_rlimit_nofile` parameter (e.g. to 65536).
If you observe problems regarding limits to the number of file descriptors
look for documentation.
For example,
http://www.cyberciti.biz/faq/linux-unix-nginx-too-many-open-files/ .


Deployment
------------

This user guide shows how to deploy the system in *experimental mode*.
The TCP ports this mode uses are different from those used in production.
More specifically, these ports should be:

- For publishing events (`nginx` load balancer): 9220

- For the frontend servers that receive the events: 9210 to 9215

- For the backend stream: 9209 (http://localhost:9209/backend/dashboard.html)

- For the database stream: 9202 (http://localhost:9202/dbfeed/dashboard.html)

- For the REST server: 9201


The installation procedure installed the `supervisor` process control system
(http://supervisord.org/).
We use it to control the execution of all the processes
that compose the stream server.
You just need to run `supervisord` with the appropriate configuration file.
From your installation's main directory:

```shell
. venv/bin/activate
supervisord -c streamserver/deployment/deploy-experimental.conf
```
The system should now be up and running.
You can confirm it by checking the logs in the `logs-semserver` directory
or by entering the dashboards of the back-end and database streams.

In order to stop the system, run the command:

```shell
supervisorctl -c streamserver/deployment/deploy-experimental.conf shutdown
```

Before running these commands, remember that your `virtualenv` environment
must be active in your terminal.
Otherwise, the system won't be able to locate the `supervisor` executable files.
The `. venv/bin/activate` command activates it for your current terminal.
Closing the terminal or running `deactivate` deactivates it.
The terminal's prompt shows an indication when it's active.


Upgrading the software
------------------------

You can get, install and deploy new versions of this software:

```shell
. venv/bin/activate
cd streamserver
git pull
python setup.py install
cd ..
```

Follow the same procedure (except `git pull`)
if you've changed the sources and want to install
the software with your changes.
