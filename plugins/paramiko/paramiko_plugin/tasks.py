import os
import sys
import uuid
import threading
import requests
import tempfile
from six import exec_
from functools import wraps
from paramiko.client import SSHClient, WarningPolicy

from cloudify.proxy.server import CtxProxy
from cloudify.proxy.client import ScriptException
from cloudify.proxy import client as proxy_client
import cloudify.ctx_wrappers
from cloudify.exceptions import NonRecoverableError


UNSUPPORTED_SCRIPT_FEATURE_ERROR = \
    RuntimeError('ctx abort & retry commands are only supported in Cloudify '
                 '3.4 or later')
ILLEGAL_CTX_OPERATION_ERROR = RuntimeError('ctx may only abort or return once')
_NOTHING = object()


class NetstringMultiCtxProxy(object):
    def __init__(self):
        self.contexts = {}

    def handle(self, sock, *a):
        t = threading.Thread(target=self._handle_connection, args=(sock, ))
        t.start()

    def _handle_connection(self, sock):
        ns = self._get_netstring(sock)
        ctx_id, _, ns = ns.partition('\x00')
        ctx = self.contexts[ctx_id]
        resp = CtxProxy(ctx, '').process(ns)
        resp = '{0}:{1},'.format(len(resp), resp)
        sent = 0
        while sent < len(resp):
            sent += sock.send(resp[sent:])
        sock.close()

    def _get_netstring(self, sock):
        buf = ''
        size = None
        while True:
            data = sock.recv(1024)
            if not data:
                raise RuntimeError('Connection closed prematurely')
            buf += data

            if size is None:
                if ':' in buf:
                    size, _, buf = buf.partition(':')
                    size = int(size)
                elif len(buf) > 10:
                    raise ValueError('Malformed data')

            if size is not None:
                if len(buf) > size + 1:
                    raise RuntimeError('Malformed data')
                elif len(buf) == size + 1:
                    break
        return buf[:-1]


def handle_script_result(ctx):
    script_result = ctx._return_value
    if isinstance(script_result, ScriptException):
        if script_result.retry:
            return script_result
        else:
            raise NonRecoverableError(str(script_result))
    # this happens when more than 1 ctx operation is invoked or
    # the plugin runs an unsupported feature on older Cloudify
    elif isinstance(script_result, RuntimeError):
        raise NonRecoverableError(str(script_result))
    if script_result is _NOTHING:
        return
    return script_result


class _CtxWrapper(object):
    def __init__(self, ctx, client, remote_base_dir=None):
        self._ctx = ctx
        self._return_value = _NOTHING
        self._client = client
        self._remote_base_dir = remote_base_dir

    def __getattr__(self, k):
        return getattr(self._ctx, k)

    def returns(self, _value):
        if self._return_value is not _NOTHING:
            self._return_value = ILLEGAL_CTX_OPERATION_ERROR
            raise self._return_value
        self._return_value = _value

    def abort_operation(self, message=None):
        if self._return_value is not _NOTHING:
            self._return_value = ILLEGAL_CTX_OPERATION_ERROR
            raise self._return_value
        self._return_value = ScriptException(message)
        return self._return_value

    def retry_operation(self, message=None, retry_after=None):
        if self._return_value is not _NOTHING:
            self._return_value = ILLEGAL_CTX_OPERATION_ERROR
            raise self._return_value
        self.operation.retry(message=message, retry_after=retry_after)
        self._return_value = ScriptException(message, retry=True)
        return self._return_value

    def download_resource(self, resource_path, target_path=None):
        local_target_path = self._ctx.download_resource(resource_path)
        remote_target_path = self._get_remote_target_path(
            local_target_path, target_path)
        sftp = self._client.open_sftp()
        try:
            sftp.put(local_target_path, remote_target_path)
        finally:
            sftp.close()
        return remote_target_path

    def download_resource_and_render(self,
                                     resource_path,
                                     target_path=None,
                                     template_variables=None):
        local_target_path = self._ctx.download_resource_and_render(
            resource_path,
            template_variables=template_variables)
        remote_target_path = self._get_remote_target_path(
            local_target_path, target_path)
        sftp = self._client.open_sftp()
        try:
            sftp.put(local_target_path, remote_target_path)
        finally:
            sftp.close()
        return remote_target_path

    def _get_remote_target_path(self, local_target_path, target_path=None):
        if target_path:
            return target_path
        else:
            remote_work_dir = self._get_work_dir()
            self._client.exec_command('mkdir -p {0}'.format(remote_work_dir))
            return os.path.join(
                remote_work_dir, os.path.basename(local_target_path))

    def _get_work_dir(self):
        if self._remote_base_dir is None:
            _in, out, _err = self._client.exec_command('mktemp -d')
            self._remote_base_dir = out.read().strip()
        remote_work_dir = os.path.join(self._remote_base_dir, 'work')
        return remote_work_dir


def with_client(f):
    cache = {}
    connect_lock = threading.Lock()

    def _rename_kwargs(env):
        renames = [
            ('user', 'username'),
            ('host_string', 'hostname')
        ]
        for rename_from, rename_to in renames:
            if rename_from in env and rename_to in env:
                raise ValueError('Both {0} and {1} present'
                                 .format(rename_from, rename_to))
            if rename_from in env:
                env[rename_to] = env.pop(rename_from)

    @wraps(f)
    def _inner(ssh_env, no_cache=False, *args, **kwargs):
        _rename_kwargs(ssh_env)
        key = (ssh_env['hostname'],
               ssh_env.get('port', 22),
               ssh_env['username'])
        with connect_lock:
            if key not in cache or no_cache:
                client = SSHClient()
                client.set_missing_host_key_policy(WarningPolicy())
                client.connect(**ssh_env)
                if no_cache:
                    return f(*args, **kwargs)
                proxy = NetstringMultiCtxProxy()
                transport = client.get_transport()
                port = transport.request_port_forward('127.0.0.1', 0,
                                                      proxy.handle)
                proxy.proxy_url = 'netstring://127.0.0.1:{0}'.format(port)
                cache[key] = (client, proxy)
            kwargs['client'], kwargs['proxy'] = cache[key]
        try:
            return f(*args, **kwargs)
        finally:
            if no_cache:
                client.close()
    return _inner


@with_client
def run_script(ctx, script_path, client, proxy, env=None, use_sudo=False,
               stdin=None, **kwargs):
    _in, out, _err = client.exec_command('mktemp -d --tmpdir=/tmp cfy.XXXXXX')
    base_dir = out.read().strip()
    sftp = client.open_sftp()
    proxy_client_path = proxy_client.__file__
    if proxy_client_path.endswith('.pyc'):
        proxy_client_path = proxy_client_path[:-1]
    local_ctx_py_path = os.path.join(
        os.path.dirname(cloudify.ctx_wrappers.__file__), 'ctx-py.py')
    script_path = get_script(ctx.download_resource, script_path)
    script_filename = os.path.basename(script_path)
    for filename, target_filename in [
            (script_path, script_filename),
            (proxy_client_path, 'ctx'),
            (local_ctx_py_path, 'cloudify.py')]:
        remote_script_path = os.path.join(base_dir, target_filename)
        sftp.put(filename, remote_script_path)
        client.exec_command('chmod +x {0}'.format(remote_script_path))

    wrapped_ctx = _CtxWrapper(ctx, client)

    envvars_path = os.path.join(base_dir, 'envvars')
    ctx_id = uuid.uuid4().hex
    proxy.contexts[ctx_id] = wrapped_ctx
    with sftp.open(envvars_path, 'w') as f:
        f.write('export CTX_SOCKET_URL={0}\n'.format(proxy.proxy_url))
        f.write('export PATH={0}:/sbin:$PATH\n'.format(base_dir))
        f.write('export CTX_ID={0}\n'.format(ctx_id))
        if env:
            for k, v in env.items():
                f.write('export {0}={1}\n'.format(k, v))

    cmd = 'source {0} && {1} {2}'.format(
        envvars_path,
        'sudo' if use_sudo else '',
        os.path.join(base_dir, script_filename))
    script_in, script_out, script_err = client.exec_command(cmd)
    if stdin is not None:
        script_in.write(stdin)
    stdout = script_out.read()
    stderr = script_err.read()
    sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    client.exec_command('rm -fr {0}'.format(base_dir))

    sftp.close()
    proxy.contexts.pop(ctx_id)

    if script_out.channel.recv_exit_status() != 0:
        ctx.logger.error(stdout)
        raise ScriptException(stderr)
    return handle_script_result(wrapped_ctx)


@with_client
def run_task(ctx, client, tasks_file, task_name,
             task_properties=None, hide_output=None, **kwargs):
    if task_properties is None:
        task_properties = {}
    task = _get_task(ctx, tasks_file, task_name)
    ctx.logger.info('Running task: {0} from {1}'.format(task_name, tasks_file))
    return task(client=client, **task_properties)


def get_script(download_resource_func, script_path):
    split = script_path.split('://')
    schema = split[0]
    if schema in ['http', 'https']:
        response = requests.get(script_path)
        if response.status_code == 404:
            raise NonRecoverableError('Failed to download script: {0} ('
                                      'status code: {1})'
                                      .format(script_path,
                                              response.status_code))
        content = response.text
        suffix = script_path.split('/')[-1]
        script_path = tempfile.mktemp(suffix='-{0}'.format(suffix))
        with open(script_path, 'wb') as f:
            f.write(content)
        return script_path
    else:
        return download_resource_func(script_path)


def _get_task(ctx, tasks_file, task_name):
    ctx.logger.debug('Getting tasks file...')
    try:
        tasks_code = ctx.get_resource(tasks_file)
    except Exception as e:
        raise NonRecoverableError(
            "Could not get '{0}' ({1}: {2})".format(tasks_file,
                                                    type(e).__name__, e))
    exec_globs = exec_globals(tasks_file)
    try:
        exec_(tasks_code, _globs_=exec_globs)
    except Exception as e:
        raise NonRecoverableError(
            "Could not load '{0}' ({1}: {2})".format(tasks_file,
                                                     type(e).__name__, e))
    task = exec_globs.get(task_name)
    if not task:
        raise NonRecoverableError(
            "Could not find task '{0}' in '{1}'"
            .format(task_name, tasks_file))
    if not callable(task):
        raise NonRecoverableError(
            "'{0}' in '{1}' is not callable"
            .format(task_name, tasks_file))
    return task


def exec_globals(tasks_file):
    copied_globals = globals().copy()
    del copied_globals['exec_globals']
    copied_globals['__doc__'] = 'empty globals for exec'
    copied_globals['__file__'] = tasks_file
    copied_globals['__name__'] = 'fabric_tasks'
    copied_globals['__package__'] = None
    return copied_globals
