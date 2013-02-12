#!/usr/bin/env python

import os, random, shutil, socket

def copy_with_substitutions(from_filename, to_filename, substitutions):
    with open(to_filename, 'wb') as to_file:
        with open(from_filename, 'rb') as from_file:
            to_file.write(from_file.read().format(**substitutions))

def copy_dir_with_substitutions(from_dir, to_dir, substitutions):
    filenames = os.listdir(from_dir)
    for filename in filenames:
        copy_with_substitutions(
            os.path.join(from_dir, filename),
            os.path.join(to_dir, filename),
            substitutions)

if __name__ == '__main__':
    pristine_conf_dir = '/usr/class/cs149/hadoop-1.1.1/conf'
    template_conf_dir = '/usr/class/cs149/assignments/pa3/local-hadoop/conf'
    result_conf_dir = os.path.abspath('conf')

    hdfs_port = random.randint(20000, 50000)
    substitutions = {
        'host': socket.gethostname(),
        'hdfs_port': hdfs_port,
        }
    shutil.copytree(pristine_conf_dir, result_conf_dir)
    copy_dir_with_substitutions(
        template_conf_dir,
        result_conf_dir,
        substitutions)
