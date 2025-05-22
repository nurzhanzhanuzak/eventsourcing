.. _DCB example 1:

DCB 1 - Course booking - aggregates
===================================

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
in the terminology of this library, without requiring aggregate sequences.

Each event in DCB has one "type", some "data", and any number of "tags".
Recorded events also have an assigned "position" in the sequence, and for this reason are referred to as
"sequenced events". They correspond to the :ref:`stored event <Stored event objects>` and
:ref:`notification <Notification objects>` objects previously defined in this library. The important difference
is that events in DCB do not have an aggregate ID and version number.

A command method in a DCB application will usually begin by selecting from an event store a set of sequenced
events. The event store is given a query that has zero, one, or many "query items". Each query item may have
zero, one, or many "types", and zero, one, or many "tags". Optionally, the event store is also given a position
in the sequence of recorded events after which events should be selected.

When querying for events, sequenced events are selected that match at least one query item, and only those
recorded after a given position in the sequence if such a position is given.

An event matches a query item if either the event's type is mentioned in the query item's collection of types or
the query item has zero types, and then only if the event's collection of tags is a superset of the query items's
collection of tags.

In this way, a query item with more types will be more inclusive, and a query item with more tags
will be more restrictive. And each query item will tend to add events to the set of events selected by the query.

However, if a query altogether has zero types and zero tags, then all events will be selected, optionally after a
given position in the sequence.

After selecting a set of events from the recorded sequence, a command method will then "project" the selected
set of events into a "decision model". The command method uses the decision model to make its decision,
generating one or many new events, or raising an error.

The highest "last known" position from the set of events involved in constructing the decision model is used when
recording new events. Consistency of recorded state is maintained by using that sequence position, along with the
same query items used for selecting events, to query for any other events recorded since the decision model was
constructed.

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
boundaries that may eventually become inappropriate and difficult to refactor. This maybe true, and we should
also investigate elsewhere how comparatively easy or difficult it is to refactor sequences of events
recorded by DCB applications.

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

.. literalinclude:: ../../../examples/coursebooking/test_application.py
    :pyobject: TestEnrolment

Enrolment interface
-------------------

The interface used by the test case is defined as a Python protocol class.

.. literalinclude:: ../../../examples/coursebooking/interface.py
    :pyobject: Enrolment

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

The :class:`examples.coursebooking.application.EnrolmentWithAggregates` class shown below uses the event-sourced
:class:`~examples.coursebooking.domainmodel.Course` and :class:`~examples.coursebooking.domainmodel.Student`
aggregate classes from the domain model, and implements the enrolment protocol with its methods for registering
students and for registering courses, for joining students with courses, for listing students for a course, and
for listing courses for a student.

This meets the "course subscriptions" challenge with event-sourced aggregates, without tricks and without
accidental complexity, showing that it is possible, and entirely legitimate, to extended the transactional
consistency boundary when using event-sourced aggregates to include more than one aggregate.

At the time of writing, this possibility is not mentioned in the list of
`traditional approaches <https://dcb.events/examples/course-subscriptions/#traditional-approaches>`_ on the dynamic
consistency boundaries website, which lists only three options: eventual consistency, larger aggregate, reservation
pattern.

Please note, the "consistency boundary" for joining a course involves atomically recording new events from more
than one aggregate, the student and the course. The preservation of recorded consistency is tested in the extra
test case below.


.. literalinclude:: ../../../examples/coursebooking/application.py
    :pyobject: EnrolmentWithAggregates

.. literalinclude:: ../../../examples/coursebooking/test_application.py
    :pyobject: TestEnrolmentWithAggregates

Testing the consistency boundary
--------------------------------

The extra test case below shows that extending the transactional consistency boundary when using
event-sourced aggregates to include more than one aggregate is technically sound, by checking
that the recorded consistency of the course-student nexus is guarded against concurrent operations.

.. literalinclude:: ../../../examples/coursebooking/test_application.py
    :pyobject: TestEnrolmentConsistency

It has been a common misapprehension that the "consistency boundary" notion in DDD is equal to
one aggregate (`"by definition, the aggregate is the boundary of consistency" <https://sara.event-thinking.io/2023/04/kill-aggregate-chapter-2-the-aggregate-does-not-fit-the-storytelling.html>`_).
The actual idea from DDD is that a database transactional consistency boundary must not be less
than one aggregate. The meaning of "not less than" is "greater than or equal to". A consistency boundary
that includes more than one aggregate, or indeed other things, has always been permitted by DDD.

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
