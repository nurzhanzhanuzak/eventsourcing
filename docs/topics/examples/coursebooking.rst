.. _DCB example 1:

DCB 1 - Course Booking
======================

This example shows how to implement the "course booking" example, used when
discussing dynamic consistency boundaries, with event-sourced aggregates.

Dynamic consistency boundaries is a new variant of event sourcing which was
presented in a talk provocatively entitled "killing the aggregate". The general
idea is that the aggregates of DDD establish consistency boundaries that are
not always appropriate, and a more general scheme is defined that involves
a single sequence of events (an "application sequence" in the terminology of
this library) in which each event has a type, data, and any number
of tags. Recorded events also have a position in the application sequence. A
command will query for events with types and tags, and project those events into
a state from which a decision can be made, generating a new event. Consistency of
recorded state is maintained by using the last sequence position known
when that decision was made, along with the same query, to check that no new events
have been recorded, and if this condition does not fail, then the new event is recorded
and assigned a position in the application sequence. Event-sourced aggregates can be
implemented in this scheme by using a tag to indicate the aggregate ID.

This example is implemented with event-sourced aggregates, showing that it is
possible to extended the transactional consistency boundary when using event-sourced
aggregates to include more than one aggregate. But mostly it is setting the scene with
a test case that will be satisfied using the standard dynamic consistency boundaries style,
which is supported by this library, and which offers possibilities beyond event-sourced
aggregates.

The next example will be implemented using the standard dynamic consistency boundaries style.

Domain model
------------

The domain model has an aggregate for courses and an aggregate for students.

.. literalinclude:: ../../../examples/coursebooking/domainmodel.py
    :pyobject: Course

.. literalinclude:: ../../../examples/coursebooking/domainmodel.py
    :pyobject: Student


Application
-----------

The application has methods for joining students with courses and for listing students and courses.

.. literalinclude:: ../../../examples/coursebooking/application.py
    :pyobject: Enrolment


Test case
---------

The test case shows students and courses being registered, with students joining courses.

.. literalinclude:: ../../../examples/coursebooking/test_application.py
    :pyobject: TestEnrolment


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

.. automodule:: examples.coursebooking.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

