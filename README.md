## Doctopus
A distributed data collector. The [ziyan](https://github.com/maboss-YCMan/ziyan) and [chitu](https://github.com/maboss-YCMan/chitu) upgrade version.

## Support

Python3.5+

## Dependence

Depends on Redis and Etcd

## Increased functionality

- Web self-check interface
- Support Web API used to reboot and reload the configuration
- The operating status is automatically registered to the remote Etcd database
- Generates a standard Confd configuration file for synchronizing remote configuration files via Confd

## Usage

```
$ doctopus_make -h
usage:
doctopus_make project [-h] [-t {ziyan, chitu} ]

positional arguments:
  project               project name

optional arguments:
  -h, --help            show this help message and exit
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

### ziyan

Generate project catalogs:

```
$ doctopus_make project_name

$ cd project_name

$ python manage.py
```

The generated project is a test project that can run

Write the logical and custom configuration of the fetch data in the plugin

### chitu

```
$ doctopus_make project_name -t chitu

$ cd project_name

$ python manage.py -t chitu
```
