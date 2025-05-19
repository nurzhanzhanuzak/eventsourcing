.. _DCB example 1:

1 - Course booking - with aggregates
====================================

This example shows how to implement the "course booking" example, used when
discussing `dynamic consistency boundaries <https://dcb.events/>`_, but with
standard event-sourced aggregates, demonstrating that the transactional consistency
boundary for event-sourced aggregates can include more than one aggregate instance.

Dynamic consistency boundaries is a new variant of event sourcing presented in a
humorously provocative way as "killing the aggregate". The general idea is that
the aggregates of DDD establish consistency boundaries that are
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

However, this example is implemented with event-sourced aggregates, using the declarative
syntax supported by this library, showing that it is possible to extended the transactional
consistency boundary when using event-sourced aggregates to include more than one aggregate.
But mostly it is setting the scene with a test case that will be satisfied in the
:doc:`next example </topics/examples/coursebooking-dcb>` using the standard
"dynamic consistency boundaries" style. With these two examples, the two styles
can be more easily compared.

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
The test is run twice, once with :class:`~eventsourcing.popo.POPOApplicationRecorder` and
then with the :class:`~tests.dcb_tests.postgres.PostgresApplicationRecorder`.

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

