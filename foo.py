#!/usr/bin/env python
from cloudify import ctx
import subprocess
import os
print 'elo', os.environ
print subprocess.check_output(['getenforce'])
ctx.instance.runtime_properties['a'] = 42
print 'printed'
ctx.download_resource('foo.py')
ctx.logger.info('siema')
