"""What the isolation step costs in Temporal and Restate, for comparison with
examples/bench_sessions.erl.

Temporal: SandboxedWorkflowRunner.prepare_workflow builds a workflow
sandbox, the way a new workflow run gets one (a fresh sys.modules in which
the workflow module is imported again; the standard library and pydantic
are passed through).

Restate: the per-invocation state its Python SDK creates (the VM state
machine). Restate has no isolation between invocations of one process.

Run with:
    uv venv /tmp/sdk-venv && uv pip install -p /tmp/sdk-venv temporalio restate-sdk pydantic
    /tmp/sdk-venv/bin/python examples/bench_sessions_sdks.py
"""

import asyncio
import os
import statistics
import sys
import tempfile
import time

WORKFLOW = '''
from dataclasses import dataclass
import json, decimal, datetime
from temporalio import workflow
%s

@dataclass
class Step:
    n: int

@workflow.defn
class Orders:
    @workflow.run
    async def run(self, state: dict) -> dict:
        n = state.get('n', 0) + 1
        return {'commands': [['activity', 'charge', {'n': n}]], 'state': {'n': Step(n).n}}
'''


def pct(xs, q):
    xs = sorted(xs)
    return xs[max(0, min(len(xs) - 1, round(q * len(xs)) - 1))]


def row(name, xs, unit='ms', scale=1e3):
    print('  %-48s p50 %9.3f %s  p99 %9.3f %s'
          % (name, pct(xs, .5) * scale, unit, pct(xs, .99) * scale, unit))


def timed(fn, n):
    fn()
    out = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        out.append(time.perf_counter() - t)
    return out


PYDANTIC_MODEL = '''
from pydantic import BaseModel

class Order(BaseModel):
    id: str
    amount: float
'''


async def temporal(name, extra):
    from temporalio.worker import UnsandboxedWorkflowRunner
    from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
    from temporalio.workflow import _Definition
    d = tempfile.mkdtemp()
    with open(os.path.join(d, name + '.py'), 'w') as f:
        f.write(WORKFLOW % extra)
    sys.path.insert(0, d)
    import importlib
    mod = importlib.import_module(name)
    defn = _Definition.must_from_class(mod.Orders)
    label = 'with a pydantic model' if extra else 'stdlib only'
    row('temporal sandbox per run, ' + label, timed(lambda: SandboxedWorkflowRunner().prepare_workflow(defn), 300))
    row('temporal unsandboxed, ' + label, timed(lambda: UnsandboxedWorkflowRunner().prepare_workflow(defn), 300))


def restate():
    from restate.vm import VMWrapper
    headers = [('content-type', 'application/vnd.restate.invocation.v5')]
    row('restate: per-invocation state', timed(lambda: VMWrapper(headers), 5000), 'us', 1e6)


if __name__ == '__main__':
    print('\n== isolation step per run / invocation ==')
    asyncio.run(temporal('bench_orders_wf', ''))
    asyncio.run(temporal('bench_orders_wf_pydantic', PYDANTIC_MODEL))
    restate()
