## pynsq

`pynsq` is a Python NSQ client library.

It provides a high-level reader library for building consumers and two low-level modules for both
sync and async communication over the NSQ protocol (if you wanted to write your own high-level
functionality).

The async module is built on top of the [Tornado IOLoop][tornado] and as such requires `tornado` be
installed:

`$ pip install tornado`

### Reader

Reader provides high-level functionality for building robust NSQ consumers in Python on top of the
async module.

Multiple reader instances can be instantiated in a single process (to consume from multiple
topics/channels at once). Each specifying a set of tasks that will be called for each message over
that channel. Tasks are defined as a dictionary of string names -> callables passed as
`all_tasks` during instantiation.

`preprocess_method` defines an optional callable that can alter the message data before other task
functions are called.

`validate_method` defines an optional callable that returns a boolean as to weather or not this
message should be processed.

`async` determines whether handlers will do asynchronous processing. If set to True, handlers must
accept a keyword argument called `finisher` that will be a callable used to signal message
completion (with a boolean argument indicating success).

The library handles backoff as well as maintaining a sufficient RDY count based on the # of
producers and your configured `max_in_flight`.

Here is an example that demonstrates synchronous message processing:

```python
import nsq

def task1(message):
    print message
    return True

def task2(message):
    print message
    return True

all_tasks = {"task1": task1, "task2": task2}
r = nsq.Reader(all_tasks, lookupd_http_addresses=['127.0.0.1:4161'], 
        topic="nsq_reader", channel="asdf")
nsq.run()
```

And async:

```python
"""
This is a simple example of async processing with nsq.Reader.

It will print "deferring processing" twice, and then print
the last 3 messages that it received.

Note in particular that we pass the `async=True` argument to Reader(),
and also that we cache a different finisher callable with
each message, to be called when we have successfully finished
processing it.
"""
import nsq

buf = []

def process_message(message, finisher):
    global buf
     # cache both the message and the finisher callable for later processing
    buf.append((message, finisher))
    if len(buf) >= 3:
        print '****'
        for msg, finish_fxn in buf:
            print msg
            finish_fxn(True) # use finish_fxn to tell NSQ of success
        print '****'
        buf = []
    else:
        print 'deferring processing'
    
all_tasks = {"task1": process_message}
r = nsq.Reader(all_tasks, lookupd_http_addresses=['127.0.0.1:4161'],
        topic="nsq_reader", channel="async", async=True)
nsq.run()
```

[tornado]: https://github.com/facebook/tornado
