.. _DCB example 3:

DCB 3 - Enrolment - refactored
==============================

This example shows the same "course booking" example as the :doc:`previous example </topics/examples/coursebooking-dcb>`,
using a refactored style for `dynamic consistency boundaries <https://dcb.events/>`_, rather than
standard style suggested by the `specification <https://dcb.events/specification/>`_.

Model-specific domain event classes,
:class:`~examples.coursebookingdcbrefactored.application.StudentRegistered`,
:class:`~examples.coursebookingdcbrefactored.application.CourseRegistered`,
:class:`~examples.coursebookingdcbrefactored.application.StudentJoinedCourse`.
are defined to help with type checking and code navigation. They are used instead of
the :class:`~examples.dcb.api.DCBEvent` class in the application code.

The base domain event class :class:`~examples.coursebookingdcbrefactored.eventstore.DomainEvent` is defined using
the Python :data:`msgspec` package which defines dataclasses from type annotations, and is currently the fastest available
Python serialisation library giving the smallest packed sizes for the bytes of serialised data in
:class:`~examples.dcb.api.DCBEvent` objects.

A :class:`~examples.coursebookingdcbrefactored.eventstore.Mapper` is used when writing events to convert subclass instances of
:class:`~examples.coursebookingdcbrefactored.eventstore.DomainEvent` to the :class:`~examples.dcb.api.DCBEvent`
class, and to convert back from instances of :class:`~examples.dcb.api.DCBSequencedEvent` to the
domain event classes when reading events. This also encapsulates the serialisation and deserialisation of event data
that was visible in the previous example.

The :class:`~examples.coursebookingdcbrefactored.eventstore.Selector` class is used instead of the :class:`~examples.dcb.api.DCBQuery`
and :class:`~examples.dcb.api.DCBQueryItem` classes to define the consistency boundary for the command method, and in the
query methods. The :class:`~examples.coursebookingdcbrefactored.eventstore.Selector` class uses the domain event classes to indicate
selected types rather than strings. Instances, and sequences of instances, of :class:`~examples.coursebookingdcbrefactored.eventstore.Selector`
are converted to :class:`~examples.dcb.api.DCBQuery` objects by the :class:`~examples.coursebookingdcbrefactored.eventstore.EventStore` class.

The abstract :class:`~examples.dcb.api.DCBEventStore` interface is encapsulated by the
:class:`~examples.coursebookingdcbrefactored.eventstore.EventStore` class, which uses the
same concrete :class:`~examples.dcb.popo.InMemoryDCBEventStore` and
:class:`~examples.dcb.postgres.PostgresDCBEventStoreTS` classes introduced in the previous example.
The :func:`~examples.coursebookingdcbrefactored.eventstore.EventStore.put`
and :func:`~examples.coursebookingdcbrefactored.eventstore.EventStore.get` methods of the
:class:`~examples.coursebookingdcbrefactored.eventstore.EventStore` class support passing either a single instance
of :class:`~examples.coursebookingdcbrefactored.eventstore.Selector` or a sequence, which simplifies code statements.

The :func:`EventStore.get() <examples.coursebookingdcbrefactored.eventstore.EventStore.get>` method is overloaded with
three method signatures. It returns only domain events by default. But has optional arguments to request the
return of events each with their sequenced position. And alternatively, to return a sequence of events with
along with a single position indicating the last known position. This last option is most useful in a command
method for subsequently appending new events with selectors and the last known position, to ensure consistency
of the recorded data according the the technique for dynamic consistency boundaries. In this example at least,
only the command method actually needs the sequenced positions, and it only needs the last position of the events
in its consistency boundary. The query methods in this example do not need to receive the sequenced positions of
the recorded events, and so call :func:`~examples.coursebookingdcbrefactored.eventstore.EventStore.get()` with its default of only
returning domain events.

These refactorings improve the readability and integrity of the code, with a visual appearance that is similar
to the declarative syntax used by the event-sourced aggregates in the first example. This style dramatically
reduces the source lines of code, leaving room for the addition of more methods. :-)

Application
-----------

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: EnrolmentWithDCBRefactored



Domain model
------------

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: Student

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: Course

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: StudentJoinedCourse

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: StudentLeftCourse

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: StudentAndCourse


Supporting abstractions
-----------------------

.. literalinclude:: ../../../eventsourcing/dcb/domain.py
    :pyobject: CanMutateEnduringObject

.. literalinclude:: ../../../eventsourcing/dcb/domain.py
    :pyobject: CanInitialiseEnduringObject

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: Decision

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: InitialDecision

.. literalinclude:: ../../../eventsourcing/dcb/persistence.py
    :pyobject: DCBMapper

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: MsgspecStructMapper

.. literalinclude:: ../../../eventsourcing/dcb/persistence.py
    :pyobject: DCBEventStore

.. literalinclude:: ../../../eventsourcing/dcb/persistence.py
    :pyobject: DCBRepository

.. literalinclude:: ../../../eventsourcing/dcb/domain.py
    :pyobject: EnduringObject

.. literalinclude:: ../../../eventsourcing/dcb/domain.py
    :pyobject: Group

Test case
---------


The test case is the same as the :doc:`first example </topics/examples/coursebooking>`, but executed
with the :class:`~examples.coursebookingdcbrefactored.application.EnrolmentWithDCBRefactored` class above.

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/test_application.py
    :pyobject: TestEnrolmentWithDCBRefactored

It also has some extra steps to cover the extra methods that were added to make further use of the more
declarative syntax for DCB.

Postgres DCB recorder v3
------------------------

A third attempt to implement implements the complex DCB query logic in Postgres is shown below. The
first attempt used array columns and array operator. It didn't work very well. The
:class:`~eventsourcing.dcb.postgres_tt.PostgresDCBRecorderTT` class shown below implements
the DCB event store interface using a secondary table of tags that is indexed with a B-tree.
The design of this implementation was focussed on selecting by tags first, which will typically
have high cardinality, and selecting by types and sequence position later in the query.
A multi-clause CTE statement is used to select events, passing the DCB query items as an composite
type array. A similar approach was tried for conditionally inserting event records, but this proved
to be suboptimal, and instead a stored procedure was developed that separates the "fail condition"
query from the conditional insertion of new events. This allows each part to be planned separately,
achieving much better performance. The index of sequence positions on the main table covers the
event types, which allows the results of selecting from the table of tags to be efficiently joined
with the main table and for events to filtered by type using only the indexes. The speedrun performance
report below shows how much better this third approach is than the version using ``tsvector`` with a GIN
index.

.. literalinclude:: ../../../eventsourcing/dcb/postgres_tt.py
    :start-at: DB_TYPE_NAME

Speedrun
--------

The performance of :class:`~eventsourcing.dcb.postgres_tt.PostgresDCBRecorderTT` is demonstrated in the report
below. With 7 million recorded events, it is more than 5x faster than the previous example.

With sub-millisecond response times, this implementation closes the performance gap with event-sourced aggregates,
running through at more than 60% of the first example. This is a good result, considering the much greater complexity
of the persistence model required for DCB.

Another point of interest is the number of new events. The "one fact" magic of DCB can be seen by looking at the
number of new events at the end of the report (58,440). The number of new events is exactly the same as the number of
completed application command operations. If you look again at the speedrun report for event-sourced aggregates, you
will see there are quite a lot more events recorded than actual operations. That's because the event-sourced aggregates
solution to the course subscriptions challenge generates two events each time a student joins a course, one from the
student aggregate, and one from the course aggregate. With the "one fact" magic of DCB there is just one cross-cutting event.

.. code-block::

 Dynamic Consistency Boundaries Speed Run: Course Subscriptions
 ==============================================================

 Per iteration: 10 courses, 10 students (120 ops)

 Running 'dcb-pg-tt' mode: EnrolmentWithDCBRefactored
     PERSISTENCE_MODULE: eventsourcing.dcb.postgres_tt
     POSTGRES_DBNAME: course_subscriptions_speedrun_tt
     POSTGRES_HOST: 127.0.0.1
     POSTGRES_PORT: 5432
     POSTGRES_USER: eventsourcing
     POSTGRES_PASSWORD: <redacted>

 Events in database at start:  7,066,477 events


 Stopping after: 30s

 [0:00:01s]        16 iterations      1920 ops      532 μs/op    1879 ops/s
 [0:00:02s]        32 iterations      3840 ops      510 μs/op    1956 ops/s
 [0:00:03s]        49 iterations      5880 ops      511 μs/op    1955 ops/s
 [0:00:04s]        64 iterations      7680 ops      544 μs/op    1835 ops/s
 [0:00:05s]        80 iterations      9600 ops      522 μs/op    1915 ops/s
 [0:00:06s]        96 iterations     11520 ops      515 μs/op    1940 ops/s
 [0:00:07s]       113 iterations     13560 ops      509 μs/op    1961 ops/s
 [0:00:08s]       129 iterations     15480 ops      511 μs/op    1953 ops/s
 [0:00:09s]       144 iterations     17280 ops      534 μs/op    1872 ops/s
 [0:00:10s]       160 iterations     19200 ops      521 μs/op    1916 ops/s
 [0:00:11s]       177 iterations     21240 ops      513 μs/op    1948 ops/s
 [0:00:12s]       193 iterations     23160 ops      508 μs/op    1967 ops/s
 [0:00:13s]       209 iterations     25080 ops      507 μs/op    1970 ops/s
 [0:00:14s]       226 iterations     27120 ops      509 μs/op    1961 ops/s
 [0:00:15s]       242 iterations     29040 ops      509 μs/op    1960 ops/s
 [0:00:16s]       259 iterations     31080 ops      509 μs/op    1964 ops/s
 [0:00:17s]       275 iterations     33000 ops      511 μs/op    1954 ops/s
 [0:00:18s]       291 iterations     34920 ops      509 μs/op    1962 ops/s
 [0:00:19s]       308 iterations     36960 ops      507 μs/op    1970 ops/s
 [0:00:20s]       324 iterations     38880 ops      510 μs/op    1957 ops/s
 [0:00:21s]       340 iterations     40800 ops      509 μs/op    1963 ops/s
 [0:00:22s]       357 iterations     42840 ops      510 μs/op    1959 ops/s
 [0:00:23s]       373 iterations     44760 ops      512 μs/op    1950 ops/s
 [0:00:24s]       389 iterations     46680 ops      508 μs/op    1966 ops/s
 [0:00:25s]       406 iterations     48720 ops      507 μs/op    1971 ops/s
 [0:00:26s]       422 iterations     50640 ops      508 μs/op    1967 ops/s
 [0:00:27s]       438 iterations     52560 ops      510 μs/op    1960 ops/s
 [0:00:28s]       455 iterations     54600 ops      508 μs/op    1967 ops/s
 [0:00:29s]       471 iterations     56520 ops      511 μs/op    1956 ops/s
 [0:00:30s]       487 iterations     58440 ops      508 μs/op    1967 ops/s

 Events in database at end:  7,124,917 events  (58,440 new)

Because the style of the refactored application code is very nice, and the performance of the Postgres
recorder is very good, this library now supports DCB by including this version of the Postgres DCB recorder,
an also the in-memory DCB recorders presented in the previous example, along with the abstractions for domain
models and application discussed above. If you are feeling playful, you can install the Python :data:`eventsourcing`
package and have fun experimenting with dynamic consistency boundaries in Python!


Code reference
--------------

.. automodule:: eventsourcing.dcb.domain
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: eventsourcing.dcb.persistence
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: eventsourcing.dcb.postgres_tt
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.coursebookingdcbrefactored.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

