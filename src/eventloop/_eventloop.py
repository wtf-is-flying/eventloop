"""Understanding the Magic Behind Await:
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

import heapq
import selectors
import socket
import time
import typing
from collections import defaultdict
from dataclasses import dataclass, field
from types import coroutine
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator
    from datetime import timedelta

# def clock() -> float: ...
clock = time.time


class Pending:
    """Task is pending."""


@dataclass
class Finished:
    """Task is finished."""

    value: Any


@dataclass
class Task:
    coroutine: Coroutine[YieldType, Any, Any]
    state: Pending | Finished = field(default_factory=Pending)


## EventLoopRequest -----------------------------------------------------------
##   EventLoopRequest are types that are sent to the event loop.
##   They are implementation details of the async runtime.
##   They allow the runtime to know what to actually do.
##   User of this 'library' would use the corresponding functions instead.
## ----------------------------------------------------------------------------


class EventLoopRequest: ...


@dataclass
class Spawn(EventLoopRequest):
    coroutine: Coroutine[Any, Any, Any]


@coroutine
def spawn(coroutine: Coroutine[Any, Any, Any]) -> Generator[Spawn, Task, Task]:
    """Spawn a coroutine in the background, returning a handle to it.

    The result can then be obtained using `join`.
    This function is currently async because the only way to send events
    to the event loop is via an async function.
    """
    handle = yield Spawn(coroutine=coroutine)
    return handle


@dataclass
class Join(EventLoopRequest):
    task: Task


@coroutine
def join(task: Task) -> Generator[Join]:
    """Wait for a task to complete."""
    # This yield back control to the event loop with a Join request.
    # The event loop will schedule back the parent task when the child task finishes.
    yield Join(task)


@dataclass
class Sleep(EventLoopRequest):
    delay: timedelta


@coroutine
def sleep(delay: timedelta) -> Generator[Sleep]:
    """Sleep for the given delay."""
    yield Sleep(delay=delay)


@dataclass
class Receive(EventLoopRequest):
    socket: socket.socket


@coroutine
def recv(socket: socket.socket, size: int) -> Generator[Receive, None, bytes]:
    """Receive data from a socket."""
    # Ensure that is socket is non-blocking.
    socket.setblocking(False)

    # This yield will return when the socket is ready for read
    yield Receive(socket=socket)

    # Now, we can read data from the socket
    return socket.recv(size)


@dataclass
class Send(EventLoopRequest):
    socket: socket.socket


@coroutine
def send(socket: socket.socket, data: bytes) -> Generator[Send]:
    """Send data on a socket."""
    # Ensure that is socket is non-blocking.
    socket.setblocking(False)

    # Loop until all data is sent
    while data:
        # This yield will return when the socket is ready for write
        yield Send(socket=socket)

        # Now, we can write some data to the socket.
        # `size` is the amount of data written.
        size = socket.send(data)
        data = data[size:]


@dataclass
class Nothing(EventLoopRequest): ...


@coroutine
def nothing() -> Generator[Nothing]:
    """Do nothing, much like an `await sleep(0)`."""
    yield Nothing()


def socketpair() -> tuple[socket.socket, socket.socket]:
    """Get a pair of non-blocking sockets."""
    lhs, rhs = socket.socketpair()
    lhs.setblocking(False)
    rhs.setblocking(False)
    return lhs, rhs


## Async 'runtime' ------------------------------------------------------------
##   The equivalent of `asyncio.run()`.
## ----------------------------------------------------------------------------

YieldType = Spawn | Join | Sleep | Send | Receive | Nothing
SendType = Any


@dataclass(order=True)
class SleepingTask:
    """Sleeping task.

    Sleeping tasks can be ordered by resume time.
    """

    task: Task = field(compare=False)
    resume_at: float


@dataclass
class SelectorData:
    read: list[Task] = field(default_factory=list)
    write: list[Task] = field(default_factory=list)


def run_until_complete(main: Coroutine[Any, Any, Any]):
    """Run a coroutine until completion."""
    # All currently running async functions
    task_queue: list[tuple[Task, SendType]] = [(Task(main), None)]
    watched_by: defaultdict[Task, list[Task]] = defaultdict(list)

    # heapq of sleeping tasks, orderd by increasing resume time
    sleeping_tasks: list[SleepingTask] = []

    io_selector = selectors.DefaultSelector()

    while True:
        if not task_queue:
            if not io_selector.get_map():
                # If task_queue and io_selector and sleeping_tasks are empty,
                # then there is nothing left to do!
                if not sleeping_tasks:
                    return

                # If no task is pending and we are not watching any socket,
                # sleep until the next sleeping task must be awoken.
                delay = max(0, sleeping_tasks[0].resume_at - clock())
                time.sleep(delay)

            else:
                timeout = (
                    max(0, sleeping_tasks[0].resume_at - clock())
                    if sleeping_tasks
                    else None
                )

                for key, mask in io_selector.select(timeout):
                    data = typing.cast(SelectorData, key.data)

                    if mask & selectors.EVENT_READ:
                        task_queue.extend((t, None) for t in data.read)
                        data.read.clear()

                    if mask & selectors.EVENT_WRITE:
                        task_queue.extend((t, None) for t in data.write)
                        data.write.clear()

                    # Stop watching socket if there is no more pending task
                    if not data.read and not data.write:
                        io_selector.unregister(key.fileobj)

        # Schedule tasks that have their sleep delay elapsed
        while sleeping_tasks and sleeping_tasks[0].resume_at < clock():
            st = heapq.heappop(sleeping_tasks)
            task_queue.append((st.task, None))

        # Run all tasks
        while task_queue:
            task, data = task_queue.pop(0)

            try:
                # Send back data to the coroutine, resuming it
                # and go the next 'yieldpoint' = await of coroutine
                yielded = task.coroutine.send(data)

            except StopIteration as exc:
                # Last yieldpoint of task reached
                task.state = Finished(value=exc.value)

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

                    case Join(child):
                        match child.state:
                            case Pending():
                                # Child task is currently in queue
                                watched_by[child].append(task)
                            case Finished(value):
                                # Child task finished.
                                # Resume the parent task immediately
                                # with the result from child
                                task_queue.insert(0, (task, value))

                    case Sleep(delay):
                        heapq.heappush(
                            sleeping_tasks,
                            SleepingTask(
                                task=task, resume_at=clock() + delay.total_seconds()
                            ),
                        )

                    case Send(socket):
                        try:
                            key = io_selector.get_key(socket)
                        except KeyError:
                            io_selector.register(
                                socket,
                                selectors.EVENT_WRITE,
                                SelectorData(write=[task]),
                            )
                        else:
                            data = typing.cast(SelectorData, key.data)
                            data.write.append(task)
                            io_selector.modify(
                                socket,
                                key.events | selectors.EVENT_WRITE,
                                data,
                            )

                    case Receive(socket):
                        try:
                            key = io_selector.get_key(socket)
                        except KeyError:
                            io_selector.register(
                                socket,
                                selectors.EVENT_READ,
                                SelectorData(read=[task]),
                            )
                        else:
                            data = typing.cast(SelectorData, key.data)
                            data.read.append(task)
                            io_selector.modify(
                                socket,
                                key.events | selectors.EVENT_READ,
                                data,
                            )

                    case Nothing():
                        # Debug request; continue parent
                        task_queue.append((task, None))

        if not task_queue and not sleeping_tasks and not io_selector.get_map():
            break
