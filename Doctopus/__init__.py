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
    namespace, project = argparse.ArgumentParser().parse_known_args()

    if project:
        if len(project) >= 2:
            print('error, just require one argument, but more')
        make_directory(project.pop())
    else:
        print('type project name, please')


def make_directory(name):
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
            for file in glob.glob(filepath + '/conf/*.toml'):
                base = os.path.basename(file)
                shutil.copyfile(file, name + '/conf/' + base)

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
