# pyright: strict, reportUnknownMemberType=false
"""
Understanding the Magic Behind Await:
------------------------------------
When you write "result = await coroutine_function()", Python actually desugars this into:

    coroutine = coroutine_function()            # obtain the coroutine object
    result = yield from coroutine.__await__()"  # delegate to the coroutine's generator

The __await__ method returns an iterator that follows the generator protocol. The
"yield from" expression delegates to this iterator until it's exhausted.


```python
def gen():
    for i in range(10):
        yield i

    # Returning will raise a StopIteration
    # The return value is attached to StopIteration.value
    return "hello"


x = gen()
while True:
    try:
        i = next(x)
        print(i)
    except StopIteration as exc:
        # Will print "hello"
        print(exc.value)
        break
```

"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Generator, Coroutine
from dataclasses import dataclass
from types import coroutine
from typing import Any


class Task:
    coroutine: Coroutine[YieldType, Any, Any]
    result: Any = None
    exception: Exception | None = None
    cancelled: bool = False

    def __init__(self, coroutine: Coroutine[YieldType, Any, Any]) -> None:
        self.coroutine = coroutine


@dataclass
class EventLoopRequest[T]: ...


@dataclass
class Spawn(EventLoopRequest[Task]):
    coroutine: Coroutine[Any, Any, Any]


@dataclass
class Join(EventLoopRequest[None]):
    task: Task


@dataclass
class Nothing(EventLoopRequest[None]): ...


YieldType = Spawn | Join | Nothing


@coroutine
def nothing():
    yield Nothing()


@coroutine
def spawn(coroutine: Coroutine[Any, Any, Any]) -> Generator[Spawn, Task, Task]:
    # NEW: recover the child task handle to pass it back to the parent.
    handle = yield Spawn(coroutine=coroutine)
    return handle


@coroutine
def join(task: Task) -> Generator[Join, None, None]:
    """Awaitable object that sends a request to be notified when a task completes."""
    # This yield back control to the event loop with a Join request.
    # The event loop will schedule back the parent task when the child task finishes.
    yield Join(task)


async def hello(name: str):
    await nothing()
    print(f"Hello, {name}!")


async def main():
    # Recover the child task handle.
    child = await spawn(hello("world"))

    # Wait for the child task to complete.
    await join(child)

    print("(after join)")


SendType = Any


def run_until_complete(main: Coroutine[Any, Any, Any]):
    task_queue: list[tuple[Task, SendType]] = [(Task(main), None)]

    watched_by: defaultdict[Task, list[Task]] = defaultdict(list)

    while len(task_queue) > 0:
        task, data = task_queue.pop(0)

        try:
            # Send back data to the coroutine,
            # and go the next 'yieldpoint' = await of task
            yielded = task.coroutine.send(data)

        except StopIteration as exc:
            # Last yieldpoint of task reached
            task.result = exc.value

            # Reschedule tasks waiting on this task
            task_queue.extend((t, None) for t in watched_by.pop(task, []))

        else:
            match yielded:
                case Spawn(coroutine):
                    child = Task(coroutine)
                    # Resume task which requested a spawn.
                    # It will send back `child` to the parent
                    # on the next iteration (insert at 0).
                    task_queue.insert(0, (task, child))

                    # Add the new child task to queue
                    task_queue.append((child, None))

                case Join(other_task):
                    watched_by[other_task].append(task)

                case Nothing():
                    # Debug request, continue parent
                    task_queue.append((task, None))


run_until_complete(main())
