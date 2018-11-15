import logging

import tabulate
import toolz
from plumbum import local

log = logging.getLogger(__name__)

_CONTEXT_MANAGER_DAG = None


# TODO rename and generalize
def value(elem):
    if hasattr(elem, 'output'):
        return elem.output()
    elif type(elem) == list:
        return [value(e) for e in elem]
    elif type(elem) == tuple:
        return tuple([value(e) for e in elem])
    elif type(elem) == dict:
        return dict((x, value(y)) for x, y in elem.items())
    else:
        return elem


class Task(object):

    def __init__(self, f, *args, **kwargs):
        if 'output' not in kwargs.keys():
            raise TypeError('missing key "output"')
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.name = '%s.%s' % (f.__module__, f.__name__)

        # TODO will pipeline ever be explicit?
        pipeline = kwargs.pop('pipeline', None)

        if not pipeline and _CONTEXT_MANAGER_DAG:
            pipeline = _CONTEXT_MANAGER_DAG
        if pipeline:
            output = kwargs['output']
            if pipeline.working_dir and isinstance(output, str):
                self.kwargs['output'] = pipeline.working_dir / output
            pipeline.add_task(self)

    def __str__(self):
        """
        String representation.
        """
        def to_output(t):
            if isinstance(t, Task):
                return t.kwargs['output']
            return t
        kwargs = {k: to_output(v) for k, v in self.kwargs.items()}
        kwargs = toolz.dissoc(kwargs, 'output')
        if self.args:
            args = [to_output(arg) for arg in self.args]
            return f'{self.name}({args}, {kwargs})'
        else:
            return f'{self.name}({kwargs})'

    def __repr__(self):
        """
        Detailed representation.
        """
        return self.__str__()

    def __call__(self):
        self.run()

    def run(self):
        # TODO add atomicity
        output = self.output()
        if output.exists():
            print(f'{output} exists, skipping')
            return
        args = value(self.args)
        kwargs = value(self.kwargs)
        # log.info(f'Running: {self.f}({args},{kwargs})')
        kwargs_show = [f'{k}={str(v)}' for k, v in kwargs.items()]
        signature = ", ".join(list(args) + kwargs_show)
        print(f'Running: {self.name}({signature})')
        if not output.parent.exists():
            output.parent.mkdir()
        self.f(*args, **kwargs)

    # def hash(self):
    #     if not hasattr(self, '__hash'):
    #         M = hashlib.sha1()
    #         hash_update(M, [('name', self.name.encode('utf-8')),
    #                         ('args', self.args),
    #                         ('kwargs', self.kwargs)])
    #         self.__hash = M.hexdigest().encode('utf-8')
    #     return self.__hash

    def output(self):
        return self.kwargs['output']

    def parameters(self):
        params = toolz.valfilter(lambda x: not hasattr(x, 'output'), self.kwargs)
        params = toolz.keyfilter(lambda x: x is not 'output', params)
        return params

    def dependencies(self):
        queue = [self.args, self.kwargs.values()]
        while queue:
            deps = queue.pop()
            for dep in deps:
                if isinstance(dep, Task):
                    yield dep
                elif isinstance(dep, (list, tuple)):
                    queue.append(dep)
                elif isinstance(dep, dict):
                    queue.append(iter(dep.values()))


class ExternalTask(Task):

    def __init__(self, path):
        if isinstance(path, str):
            path = local.path(path)
        self.args = []
        self.kwargs = dict(output=path)
        self.name = path

        if _CONTEXT_MANAGER_DAG:
            pipeline = _CONTEXT_MANAGER_DAG
        pipeline.add_task(self)

    def __str__(self):
        return f'ExternalTask({self.output()})'

    def __repr__(self):
        return f'ExternalTask({self.output()})'

    def run(self):
        if not self.output().exists():
            raise Exception(f'{self}: path does not exist')

    def parameters(self):
        return None


class TaskGenerator(object):

    def __init__(self, f):
        self.f = f

    def __call__(self, *args, **kwargs):
        return Task(self.f, *args, **kwargs)


class Pipeline(object):

    def __init__(self, working_dir=None):
        self.tasks = []
        self._old_context_manager_dags = []
        if isinstance(working_dir, str):
            working_dir = local.path(working_dir)
        self.working_dir = working_dir

    def __enter__(self):
        global _CONTEXT_MANAGER_DAG
        self._old_context_manager_dags.append(_CONTEXT_MANAGER_DAG)
        _CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, _type, _value, _tb):
        global _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self._old_context_manager_dags.pop()

    def add_task(self, task):
        if task.kwargs['output'] in [t.output() for t in self.tasks]:
            return  # TODO warn if the dag id differs, i.e. two tasks are writing to the same file
        self.tasks.append(task)

    def run(self):
        if not hasattr(self, '_topological_tasks'):
            self._topological_tasks = topological_sort(self.tasks)
        for task in self._topological_tasks:
            task.run()

    def status(self):
        rows = [(t.name, t.parameters(), t.output(), t.output().exists()) for t in self.tasks]
        table = tabulate.tabulate(rows, tablefmt='fancy_grid', headers=['Name', 'Parameters', 'Filepath', 'Exists'])
        print(table)


def topological_sort(tasks):
    '''
    Sorts a list of tasks topologically. The list is sorted when
    there is never a dependency between tasks[i] and tasks[j] if i < j.
    '''
    sorted = []
    whites = set(tasks)

    def dfs(t):
        for dep in t.dependencies():
            if dep in whites:
                whites.remove(dep)
                dfs(dep)
        sorted.append(t)

    while whites:
        next = whites.pop()
        dfs(next)

    return sorted
