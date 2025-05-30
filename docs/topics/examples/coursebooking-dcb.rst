.. _DCB example 2:

DCB 2 - Coding the specification
================================

This example introduces the `specification <https://dcb.events/specification/>`_ of "dyanamic
consistency boundaries" and shows how to implement the "course subscriptions" challenge in Python
using the basic objects and methods of DCB. Whilst the code in this example is relatively verbose,
the DCB approach can be understood directly without any extra abstractions. The
:doc:`next example </topics/examples/coursebooking-dcb-refactored>` presents a refactored and
hopefully more refined style for DCB that is perhaps more usable.


Enrolment with DCB
------------------

The :class:`~examples.coursebookingdcb.application.EnrolmentWithDCB` class implements the
:class:`~examples.coursebooking.interface.EnrolmentInterface` interface, with its methods for registering
students and courses, for students joining courses, and for listing students for a course and
the courses for a student.

The application class :class:`~eventsourcing.dcb.application.DCBApplication` is a convenience introduced
to select and construct a concrete DCB event store implementation, according to its environment variable
configuration, just like the library's original :ref:`application class <Application objects>` does for
event-sourced aggregate recorders. This means we can easily run the application with an in-memory
persistence, and with PostgreSQL.


.. literalinclude:: ../../../examples/coursebookingdcb/application.py
    :pyobject: EnrolmentWithDCB


The test case is the same enrolment test case used in the :doc:`previous example </topics/examples/coursebooking>`,
but this time executed with the :class:`~examples.coursebookingdcb.application.EnrolmentWithDCB` class above rather than
:class:`~examples.coursebooking.application.EnrolmentWithAggregates`. The test method is run twice, once with an
in-memory implementation of the DCB event store, and again using PostgreSQL.

.. literalinclude:: ../../../examples/coursebookingdcb/test_application.py
    :pyobject: TestEnrolmentWithDCB


DCB API
-------

The application methods are implemented using the DCB objects and methods
defined in the specification and explained in the :doc:`previous example </topics/examples/coursebooking>`.
The DCB classes used in this example are :class:`~eventsourcing.dcb.api.DCBEvent`,
:class:`~eventsourcing.dcb.api.DCBQuery`, :class:`~eventsourcing.dcb.api.DCBQueryItem`,
:class:`~eventsourcing.dcb.api.DCBAppendCondition` and :class:`~eventsourcing.dcb.api.DCBSequencedEvent`.

.. literalinclude:: ../../../eventsourcing/dcb/api.py
    :pyobject: DCBEvent

.. literalinclude:: ../../../eventsourcing/dcb/api.py
    :pyobject: DCBQuery

.. literalinclude:: ../../../eventsourcing/dcb/api.py
    :pyobject: DCBQueryItem

.. literalinclude:: ../../../eventsourcing/dcb/api.py
    :pyobject: DCBAppendCondition

.. literalinclude:: ../../../eventsourcing/dcb/api.py
    :pyobject: DCBSequencedEvent


The :class:`~eventsourcing.dcb.api.DCBRecorder` class defines an interface that has methods described in the DCB
specification for reading and appending DCB events. There is one enhancement, which is to return an :class:`int`
from the :func:`~eventsourcing.dcb.api.DCBRecorder.append` method. This supports returning the position of the
last appended event, so that user interfaces for systems implemented with CQRS, that have eventually consistent
"read" models, can transition from a "write" view to a "read" view and wait for new events to be processed,
avoiding the stale read model problem.

.. literalinclude:: ../../../eventsourcing/dcb/api.py
    :pyobject: DCBRecorder




In-memory DCB recorder
----------------------

The :class:`~eventsourcing.dcb.popo.InMemoryDCBRecorder` class implements the DCB event store interface
using only Python objects. You can see the query logic for selecting events implemented with nested
generator expressions, and the append condition logic that is implemented in the append method. DCB
events are stored in memory, and "deep copied" when appending and when reading to avoid any corruption
of sequenced events.

.. literalinclude:: ../../../eventsourcing/dcb/popo.py
    :pyobject: InMemoryDCBRecorder


Postgres DCB recorder v2
------------------------

A second attempt to implement the complex DCB query logic in Postgres is shown below. The
first attempt used an array column for tags and array operators to search for types and tags. It didn't
work very well. The :class:`~examples.coursebookingdcb.postgres_ts.PostgresDCBRecorderTS` class shown
below implements the DCB event store interface using the Postgres ``tsvector`` and ``tsquery`` types,
and a GIN index.

The type and tags of a DCB event are prefixed and concatenated into a ``tsvector`` string. A set of DCB
query items is similarly compounded into a logical ``tsquery`` that expresses the DCB query logic. Database
functions for appending and selecting events are defined, and a custom composite type is defined for efficiently
sending an array of DCB events to the database.

In this way, both the read and the append operations of this DCB event store can be executed as fast as possible
with a single database round-trip.

.. literalinclude:: ../../../examples/coursebookingdcb/postgres_ts.py
    :pyobject: PostgresDCBRecorderTS

Speedrun
---------

The performance of this Postgres implementation is shown below. The performance wasn't as terrible as the first
attempt using array columns and array operator. It accomplished 9960 operations in 30s, giving an average
of 3.012 milliseconds per operation. This is slightly more than 10% of the performance of the event-sourced
aggregates in the previous example, which has 10x more events in its database.

This performance might sound acceptable, but with further testing we saw that as the volume of recorded
events increased, with this implementation, the performance became worse and worse,
decreasing to only a few operations per second with 5 million stored events.

.. code-block::

 Dynamic Consistency Boundaries Speed Run: Course Subscriptions
 ==============================================================

 Per iteration: 10 courses, 10 students (120 ops)

 Running 'dcb-pg-ts' mode: EnrolmentWithDCBRefactored
     PERSISTENCE_MODULE: examples.coursebookingdcb.postgres_ts
     POSTGRES_DBNAME: course_subscriptions_speedrun_tt
     POSTGRES_HOST: 127.0.0.1
     POSTGRES_PORT: 5432
     POSTGRES_USER: eventsourcing
     POSTGRES_PASSWORD: <redacted>

 Events in database at start:  166,590 events


 Stopping after: 30s

 [0:00:01s]         3 iterations       360 ops     3233 μs/op     309 ops/s
 [0:00:02s]         6 iterations       720 ops     2853 μs/op     350 ops/s
 [0:00:03s]         9 iterations      1080 ops     3162 μs/op     316 ops/s
 [0:00:04s]        11 iterations      1320 ops     3142 μs/op     318 ops/s
 [0:00:05s]        14 iterations      1680 ops     3111 μs/op     321 ops/s
 [0:00:06s]        16 iterations      1920 ops     3414 μs/op     292 ops/s
 [0:00:07s]        19 iterations      2280 ops     3409 μs/op     293 ops/s
 [0:00:08s]        21 iterations      2520 ops     3416 μs/op     292 ops/s
 [0:00:09s]        24 iterations      2880 ops     3224 μs/op     310 ops/s
 [0:00:10s]        26 iterations      3120 ops     3344 μs/op     298 ops/s
 [0:00:11s]        29 iterations      3480 ops     3422 μs/op     292 ops/s
 [0:00:12s]        31 iterations      3720 ops     3383 μs/op     295 ops/s
 [0:00:13s]        34 iterations      4080 ops     3369 μs/op     296 ops/s
 [0:00:14s]        36 iterations      4320 ops     3398 μs/op     294 ops/s
 [0:00:15s]        39 iterations      4680 ops     3382 μs/op     295 ops/s
 [0:00:16s]        41 iterations      4920 ops     3367 μs/op     296 ops/s
 [0:00:17s]        44 iterations      5280 ops     3450 μs/op     289 ops/s
 [0:00:18s]        46 iterations      5520 ops     3530 μs/op     283 ops/s
 [0:00:19s]        49 iterations      5880 ops     2732 μs/op     366 ops/s
 [0:00:20s]        52 iterations      6240 ops     2421 μs/op     412 ops/s
 [0:00:21s]        56 iterations      6720 ops     2493 μs/op     400 ops/s
 [0:00:22s]        59 iterations      7080 ops     2717 μs/op     367 ops/s
 [0:00:23s]        62 iterations      7440 ops     2801 μs/op     357 ops/s
 [0:00:24s]        65 iterations      7800 ops     2730 μs/op     366 ops/s
 [0:00:25s]        68 iterations      8160 ops     2713 μs/op     368 ops/s
 [0:00:26s]        71 iterations      8520 ops     2860 μs/op     349 ops/s
 [0:00:27s]        74 iterations      8880 ops     2579 μs/op     387 ops/s
 [0:00:28s]        77 iterations      9240 ops     2726 μs/op     366 ops/s
 [0:00:29s]        80 iterations      9600 ops     2555 μs/op     391 ops/s
 [0:00:30s]        83 iterations      9960 ops     2733 μs/op     365 ops/s

 Events in database at end:  176,550 events  (9,960 new)

Clearly if DCB is to be a viable approach to developing business software, we will need to
rethink how it might be possible to implement the complex DCB query logic in a way that
might perform well in a heavy production environment. Let's see what we can do in the
:doc:`next example </topics/examples/coursebooking-dcb-refactored>`.

Code reference
--------------

.. automodule:: examples.coursebookingdcb.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.coursebookingdcb.postgres_ts
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
