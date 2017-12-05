# -*- coding: utf-8 -*-

import argparse
import glob
import os
import shutil
import traceback


def main():
    """
    Get the project name
    :return: None
    """
    parse = argparse.ArgumentParser(prog="Doctopus", usage="\ndoctopus project [-h] [-t {ziyan, chitu} ] [-v]")
    parse.add_argument('project', help='project name', nargs='?')
    parse.add_argument('-t', '--target', choices=['ziyan', 'chitu'], default='ziyan',
                       help='selelct the target, default ziyan')
    parse.add_argument('-v', '--version', action='version', default=None, version='%(prog)s 0.3.0')
    project = parse.parse_args().project
    target = parse.parse_args().target
    version = parse.parse_args().version

    if not project:
        parse.print_help()

    elif version:
        print(version)

    elif target == 'ziyan':
        make_ziyan(project)

    elif target == 'chitu':
        make_chitu(project)


def make_ziyan(name):
    """
    create a new project directory like follow:
    -name
    |
      -- conf\
    |
      -- confd\
       |
         -- conf.d\
       |
         -- templates\
    |
      -- lua\
    |
      -- plugins\
    |
      -- manage.py
    |
      -- confd.exe(confd)
    :param name: project name, str
    :return: None
    """
    try:
        if not os.path.exists(name):
            os.makedirs(name + '/conf')
            os.mkdir(name + '/lua')
            os.mkdir(name + '/plugins')
            filepath = os.path.split(os.path.realpath(__file__))[0]

            # copy conf files
            for file in glob.glob(filepath + '/conf/ziyan_conf.toml'):
                shutil.copyfile(file, name + '/conf/conf.toml')

            # copy confd dirs and files
            shutil.copytree(filepath + '/confd/', name + '/confd/')

            # copy lua files
            for file in glob.glob(filepath + '/conf/*.lua'):
                shutil.copyfile(file, name + '/lua/enque_script.lua')

            # copy userfiles
            for file in glob.glob(filepath + '/plugins/*.py'):
                base_name = os.path.basename(file)
                if base_name == 'plugin_prototype.py':
                    shutil.copyfile(file, name + '/plugins/' + 'your_plugin.py')
                else:
                    shutil.copyfile(file, name + '/plugins/' + base_name)

            shutil.copyfile(filepath + '/script/manage.py', name + '/manage.py')
    except Exception as e:
        traceback.print_exc()

def make_chitu(name):
    """
        create a new project directory like follow:
        -name
        |
          -- conf\
        |
          -- confd\
           |
             -- conf.d\
           |
             -- templates\
        |
          -- manage.py
        |
          -- confd.exe(confd)
        :param name: project name, str
        :return: None
        """
    try:
        if not os.path.exists(name):
            os.makedirs(name + '/conf')
            filepath = os.path.split(os.path.realpath(__file__))[0]

            # copy conf files
            for file in glob.glob(filepath + '/conf/chitu_conf.toml'):
                shutil.copyfile(file, name + '/conf/conf.toml')

            # copy confd dirs and files
            shutil.copytree(filepath + '/confd/', name + '/confd/')

            shutil.copyfile(filepath + '/script/manage.py', name + '/manage.py')
    except Exception as e:
        traceback.print_exc()
