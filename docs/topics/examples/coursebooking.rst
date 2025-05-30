.. _DCB example 1:

DCB 1 - Enrolment - introduction
================================

This example introduces the `course subscription <https://dcb.events/examples/course-subscriptions/>`_
challenge, used often when discussing `dynamic consistency boundaries <https://dcb.events/>`_. The
challenge is to enforce a rule when enrolling students on courses that no student can join more
than a given number of courses, and no course can accept more than a given number of students.

Introduction to DCB
-------------------

Dynamic consistency boundaries (DCB) is a new variant of event sourcing presented in a
`humorously provocative way <https://sara.event-thinking.io/2023/04/kill-aggregate-chapter-1-I-am-here-to-kill-the-aggregate.html>`_
as "killing the aggregate".

A novel scheme is proposed that uses a single sequence of events, an :ref:`application sequence <Overview>`
in the terminology of this library. How does it work?

Each event in DCB has one "type", some "data", and any number of "tags".
Recorded events also have an assigned "position" in the sequence, and for this reason are referred to as
"sequenced events". They correspond to the :ref:`stored event <Stored event objects>` and
:ref:`notification <Notification objects>` objects previously defined in this library. The important difference
is that events in DCB do not have an aggregate ID and version number.

When querying for events in a DCB application, sequenced events are selected from an event store. The event
store is given a query that has zero, one, or many "query items". Each query item may have zero, one, or many
"types", and zero, one, or many "tags". Optionally, the event store is also given a position in the sequence
of recorded events after which events should be selected.

An event matches a query item if either the event's type is mentioned in the query item's collection of types or, if
the query item has zero types, and then only if the event's tags are a superset of the query item's
tags.

In this way, a query item with more types will be more inclusive, and a query item with more tags
will be more restrictive.

Each query item will tend to add events to the set of events selected by the query. However, if a query
altogether has zero types and zero tags, then all events will be selected, optionally after a given position
in the sequence.

After selecting a set of events from the recorded sequence, a command method will usually "project" the selected
set of events into a "decision model". The command method uses the decision model to make its decision,
generating one or many new events, or raising an error.

The highest "last known position" at the time of the query is used when recording new events. Consistency of
recorded state is maintained by using that sequence position, along with the same query items used for selecting
events, to query for any other events recorded since the decision model was constructed.

The combination of the query items and the last known position is referred to as the "append condition". If
this condition fails, because other events have been recorded, then an "integrity error" is raised by the event store
and the new events are not recorded. Otherwise, if the append condition does not fail, then all the new events are
recorded in an atomic database transaction. Each recorded event is assigned a monotonically increasing position in
the application sequence, and thereby becomes a "sequenced event".

The command's query therefore defines the "consistency boundary" for the command in a "dynamic" way.

A command in a DCB application will usually perform at least two queries. The multi-dimensional
possibilities offered by combining a set of different query items is impressive. However, this presents
a technical challenge when implementing support for DCB applications. Firstly, the selections of
events have to be correct for all possible sets of query items. But then also, it is a challenge to
achieve performance times for DCB applications that is acceptable for users, if not comparable to that
enjoyed by functionally equivalent applications that use event-sourced aggregates.

The decision models for applications that use event-sourced aggregates are the event-sourced aggregates.
The set of events required to construct these decision models can be selected directly from segregated
aggregate sequences. The persistence model, and querying functionality, for event-sourced aggregates is
therefore relatively simple, straightforward, and fast. Command methods that depend on event-sourced
aggregates usually need to execute only one query. Nevertheless, an effort has been made in these examples
to implement support for DCB is a way that is both correct and performant. And there is always room for
improvement!

Aggregates and DCB
------------------

There aren't any explicit aggregate sequences in DCB unless they are defined by an application. Something
like event-sourced aggregates can be implemented in with DCB, by using a tag to indicate an aggregate ID.
Recorded events can cut across different aggregate sequences by having more than one tag.
This is the "one fact" magic of DCB.

The central critique motivating DCB is that the aggregates of DDD establish strict and rigid consistency
boundaries that may eventually become inappropriate and difficult to refactor. This may be true. We will
investigate later how comparatively easy or difficult it is to refactor sequences of events recorded by
DCB applications and by event-sourced applications.

Another of the arguments motivating DCB is that, `"by definition, the aggregate is the boundary of consistency"
<https://sara.event-thinking.io/2023/04/kill-aggregate-chapter-2-the-aggregate-does-not-fit-the-storytelling.html>`_
and so it is impossible to implement the "course subscriptions" challenge using event-sourced aggregates without
the accidental complexity of awkward tricks. As we shall see, this is not true.

Whatever the arguments are against aggregates, it is more important that a proposition be interesting than that
it be true. DCB is indeed an interesting novel proposition. We can return elsewhere to assessing and debating its
analysis of software development.

In this example, we are setting the scene for introducing the DCB approach, by defining and validating a test case.
The test case will be satisfied first with the standard "traditional" event-sourced aggregates. In
the :doc:`next example </topics/examples/coursebooking-dcb>` we will use the objects and methods described in the
"dynamic consistency boundaries"  specification. A third example presents a :doc:`refactored and hopefully more
refined </topics/examples/coursebooking-dcb-refactored>` style for DCB that is perhaps more usable, just like the
:ref:`declarative syntax <Declarative syntax>` for event-sourced aggregate is perhaps more usable than writing
business logic directly against an :ref:`application recorder <Application recorder>`.

With these examples, the DCB approach can be introduced and examined, and different styles and operational performances
can be compared.

Enrolment test case
-------------------

The test case below has students and courses being registered, with students joining courses, and some
particular conditions that should lead to particular errors. An application-under-test is exercised twice,
once without any configuration so that it will store events in memory, and once with configuration for
recording events in PostgreSQL.

.. literalinclude:: ../../../examples/coursebooking/enrolment_testcase.py
    :pyobject: EnrolmentTestCase

Enrolment interface
-------------------

The interface used by the test case is defined as an abstract base class,
:class:`~examples.coursebooking.interface.EnrolmentInterface`.

It defines methods for registering students, for registering courses, for joining
students with courses, for listing students for a course, and for listing courses
for a student

.. literalinclude:: ../../../examples/coursebooking/interface.py
    :pyobject: EnrolmentInterface

Exception classes used in the test case are also defined separately.

.. literalinclude:: ../../../examples/coursebooking/interface.py
    :pyobject: TooManyCoursesError

.. literalinclude:: ../../../examples/coursebooking/interface.py
    :pyobject: FullyBookedError

.. literalinclude:: ../../../examples/coursebooking/interface.py
    :pyobject: AlreadyJoinedError

.. literalinclude:: ../../../examples/coursebooking/interface.py
    :pyobject: StudentNotFoundError

.. literalinclude:: ../../../examples/coursebooking/interface.py
    :pyobject: CourseNotFoundError


Event-sourced aggregates
------------------------

The domain model shown below defines an event-sourced aggregate class :class:`~examples.coursebooking.domainmodel.Course`
for courses that students can join, and an event-sourced aggregate class :class:`~examples.coursebooking.domainmodel.Student`
for students that may join courses. These aggregate classes are implemented using the concise "declarative syntax" supported
by this library. These aggregate classes are coded to use string IDs as demonstrated
in :doc:`example 11  </topics/examples/aggregate11>`.


.. literalinclude:: ../../../examples/coursebooking/domainmodel.py
    :pyobject: Course

.. literalinclude:: ../../../examples/coursebooking/domainmodel.py
    :pyobject: Student


Enrolment with aggregates
-------------------------

The :class:`~examples.coursebooking.application.EnrolmentWithAggregates` class shown below implements
:class:`~examples.coursebooking.interface.EnrolmentInterface` using the event-sourced
:class:`~examples.coursebooking.domainmodel.Course` and :class:`~examples.coursebooking.domainmodel.Student`
aggregate classes.

Please note, the "consistency boundary" for joining a course involves atomically recording new events from more
than one aggregate, the student and the course. The preservation of recorded consistency is tested in the extra
test case below.

This meets the "course subscriptions" challenge with event-sourced aggregates, without tricks and without
accidental complexity. It shows that it is perfectly possible, entirely legitimate, and quite straightforward
to extend the transactional consistency boundary when using event-sourced aggregates to include more than one
aggregate. Indeed, this is a useful technique.

.. literalinclude:: ../../../examples/coursebooking/application.py
    :pyobject: EnrolmentWithAggregates

At the time of writing, this possibility is not mentioned in the list of
`traditional approaches <https://dcb.events/examples/course-subscriptions/#traditional-approaches>`_ on the dynamic
consistency boundaries website, which lists only three options: eventual consistency, larger aggregate, reservation
pattern.


Speedrun
--------

A "speedrun" script has been written, to help compare the support for dynamic consistency boundaries that we will
develop in the next examples with the performance of the support for event-sourced aggregates provided by this
library.

It iterates over a sequence of operations, first registering a number of students and a number of courses, and
then subscribing all the students on all the courses.

It was configured to register 10 students and 10 courses, and therefore to make 100 subscriptions in each iteration.

The performance report for the event-sourced aggregates solution is included below. Using PostgreSQL
as an event store, event-sourced aggregates accomplished 93,720 operations in 30s. That gives an
average of 0.320 milliseconds per operation, and a target for implementing DCB.

.. code-block::

 Dynamic Consistency Boundaries Speed Run: Course Subscriptions
 ==============================================================

 Per iteration: 10 courses, 10 students (120 ops)

 Running 'agg-pg' mode: EnrolmentWithAggregates
     PERSISTENCE_MODULE: eventsourcing.postgres
     POSTGRES_DBNAME: course_subscriptions_speedrun_tt
     POSTGRES_HOST: 127.0.0.1
     POSTGRES_PORT: 5432
     POSTGRES_USER: eventsourcing
     POSTGRES_PASSWORD: <redacted>
     POSTGRES_ENABLE_DB_FUNCTIONS: y

 Events in database at start:  5,568,325 events


 Stopping after: 30s

 [0:00:01s]        25 iterations      3000 ops      335 μs/op    2981 ops/s
 [0:00:02s]        51 iterations      6120 ops      327 μs/op    3055 ops/s
 [0:00:03s]        76 iterations      9120 ops      328 μs/op    3048 ops/s
 [0:00:04s]       102 iterations     12240 ops      325 μs/op    3069 ops/s
 [0:00:05s]       127 iterations     15240 ops      326 μs/op    3061 ops/s
 [0:00:06s]       153 iterations     18360 ops      321 μs/op    3106 ops/s
 [0:00:07s]       179 iterations     21480 ops      323 μs/op    3089 ops/s
 [0:00:08s]       205 iterations     24600 ops      322 μs/op    3105 ops/s
 [0:00:09s]       231 iterations     27720 ops      322 μs/op    3096 ops/s
 [0:00:10s]       256 iterations     30720 ops      323 μs/op    3093 ops/s
 [0:00:11s]       282 iterations     33840 ops      321 μs/op    3107 ops/s
 [0:00:12s]       308 iterations     36960 ops      319 μs/op    3134 ops/s
 [0:00:13s]       334 iterations     40080 ops      320 μs/op    3115 ops/s
 [0:00:14s]       360 iterations     43200 ops      320 μs/op    3120 ops/s
 [0:00:15s]       386 iterations     46320 ops      319 μs/op    3131 ops/s
 [0:00:16s]       413 iterations     49560 ops      318 μs/op    3135 ops/s
 [0:00:17s]       439 iterations     52680 ops      321 μs/op    3112 ops/s
 [0:00:18s]       465 iterations     55800 ops      319 μs/op    3128 ops/s
 [0:00:19s]       491 iterations     58920 ops      317 μs/op    3150 ops/s
 [0:00:20s]       517 iterations     62040 ops      318 μs/op    3143 ops/s
 [0:00:21s]       544 iterations     65280 ops      313 μs/op    3192 ops/s
 [0:00:22s]       570 iterations     68400 ops      317 μs/op    3146 ops/s
 [0:00:23s]       596 iterations     71520 ops      317 μs/op    3152 ops/s
 [0:00:24s]       622 iterations     74640 ops      318 μs/op    3142 ops/s
 [0:00:25s]       649 iterations     77880 ops      317 μs/op    3152 ops/s
 [0:00:26s]       675 iterations     81000 ops      316 μs/op    3156 ops/s
 [0:00:27s]       701 iterations     84120 ops      315 μs/op    3171 ops/s
 [0:00:28s]       728 iterations     87360 ops      315 μs/op    3167 ops/s
 [0:00:29s]       754 iterations     90480 ops      314 μs/op    3178 ops/s
 [0:00:30s]       781 iterations     93720 ops      314 μs/op    3175 ops/s

 Events in database at end:  5,740,145 events  (171,820 new)


Testing the consistency boundary
--------------------------------

The extra test case below shows that extending the transactional consistency boundary when using
event-sourced aggregates to include more than one aggregate is technically sound, by checking
that the recorded consistency of the course-student nexus is guarded against concurrent operations.

.. literalinclude:: ../../../examples/coursebooking/test_application.py
    :pyobject: TestEnrolmentConsistency

The meaning of "not less than" is "greater than or equal to". It has been a common misapprehension
that the "consistency boundary" notion in DDD is equal to one aggregate. The actual idea from DDD
is that a database transactional consistency boundary must not be less than one aggregate. A
consistency boundary that includes more than one aggregate, or indeed other things, has always
been permitted by DDD.

Nevertheless, there are other reasons why DCB is an interesting novel approach for event sourcing,
so let's continue by :doc:`implementing the specification </topics/examples/coursebooking-dcb>` directly.

Code reference
--------------

.. automodule:: examples.coursebooking.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.coursebooking.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.coursebooking.interface
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
