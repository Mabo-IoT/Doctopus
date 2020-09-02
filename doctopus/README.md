# Overview

Update Doctopus to compatible with kafka.

## Support

Python3/Python2.7+

## Dependence

Depends on Redis.

## Usage

```
$ doctopus -h
usage:
doctopus project [-h] [-t {ziyan, chitu} ] [-v]

positional arguments:
  project               project name

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -t {ziyan,chitu}, --target {ziyan,chitu}
                        selelct the target, default ziyan
```

Manage command

```
$ python manage.py -h
usage:
python manage.py [-h] [-a ACTION] [-v] [-t {ziyan,chitu}] [-i IP] [-p PORT]

A distributed data collector.

optional arguments:
  -h, --help            show this help message and exit
  -a ACTION, --action ACTION
                        Run/test the project, default run
  -v, --version         show program's version number and exit
  -t {ziyan,chitu}, --target {ziyan,chitu}
                        selelct the target, default ziyan
  -i IP, --ip IP        Hostname or IP address on which to listen, default is
                        '0.0.0.0', which means 'all IP addresses on this
                        host'.
  -p PORT, --port PORT  TCP port on which to listen, default is '8000'.
```

### Web API

```
$ pip install httpie
```

1. Returns the current status data, json

```
$ http 127.0.0.1:8000/status    // return json data

$ http 127.0.0.1:8000/status?flush=1    // Refresh and return the latest data
```

2. Restart and Reload

```
$ http 127.0.0.1:8000/restart

$ http 127.0.0.1:8000/reload
```

3. Register the configuration information to etcd

```
$ http 127.0.0.1:8000/upload
```

### ziyan

Generate project catalogs:

```
$ doctopus project_name

$ cd project_name

$ python manage.py  // default listening on 0.0.0.0:8000
```

The generated project is a test project that can run

Write the logical and custom configuration of the fetch data in the plugin

### chitu

```
$ doctopus project_name -t chitu

$ cd project_name

$ python manage.py -t chitu // default listening on 0.0.0.0:8001
```
