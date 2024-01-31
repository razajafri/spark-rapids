

import errno
import logging
import subprocess

def __task():
    """
    Wraps access to the implicitly provided Ant task attributes map to reduce
    undefined-name linting warnings
    """
    return self

def __fail(message):
    "Fails this task with the error message"
    __task().fail(message)

def __shell_exec(shell_cmd):
    ret_code = subprocess.call(shell_cmd)
    if ret_code != 0:
        __fail("failed to execute %s" % shell_cmd)

def __generate_jars():
    clone_default = ['git', 'clone', 'https://github.com/nvidia/spark-rapids', '/tmp/spark-rapids-default']
    __shell_exec(clone_default)
    __shell_exec(['pushd', '/tmp/spark-rapids-default'])
    mvn_build = ['mvn', 'package', '-Dbuildver=311', '-DskipTests']
    __shell_exec(mvn_build)
    __shell_exec(['popd'])


def taskImpl():
