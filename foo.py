#!/usr/bin/env python
from cloudify import ctx
import subprocess
import os
ctx.instance.runtime_properties['a'] = 42
print ctx.node.properties.get_all()
