#!/usr/bin/env python

import getpass, os, subprocess

if __name__ == '__main__':
    hadoop_dir = '/usr/class/cs149/hadoop-1.1.1'
    hadoop_bin_dir = os.path.join(hadoop_dir, 'bin')
    hadoop_exe = os.path.join(hadoop_bin_dir, 'hadoop')

    hadoop_conf_dir = os.path.abspath('conf')

    os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
    subprocess.check_call([hadoop_exe, 'namenode', '-format'])
    subprocess.check_call(
        ['./hadoop-daemon.sh', '--config', hadoop_conf_dir, 'start', 'namenode'],
        cwd = hadoop_bin_dir)
    subprocess.check_call(
        ['./hadoop-daemon.sh', '--config', hadoop_conf_dir, 'start', 'datanode'],
        cwd = hadoop_bin_dir)
    subprocess.check_call(
        [hadoop_exe, '--config', hadoop_conf_dir, 'fs', '-mkdir', '/user'])
    subprocess.check_call(
        [hadoop_exe, '--config', hadoop_conf_dir, 'fs', '-mkdir', '/user/%s' % getpass.getuser()])
