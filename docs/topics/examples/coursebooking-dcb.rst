.. _DCB example 2:

2 - Course booking - with DCB style
===================================

This example shows how to implement the "course booking" example in Python, using the
standard style of `dynamic consistency boundaries <https://dcb.events/>`_, rather than
standard event-sourced aggregates. It closely follows the objects and methods
described in the `specification <https://dcb.events/specification/>`_.


Dynamic consistency boundaries is a new variant of event sourcing
that has been presented in a humorously provocative way as "killing the aggregate". The general
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
and assigned a position in the application sequence.

Application
-----------

The application has the same methods as the :doc:`previous example </topics/examples/coursebooking>`
for joining students with courses and for listing students and courses. The methods are implemented
using the classes,
:class:`~tests.dcb_tests.api.DCBAppendCondition`,
:class:`~tests.dcb_tests.api.DCBEvent`,
:class:`~tests.dcb_tests.api.DCBQuery`,
:class:`~tests.dcb_tests.api.DCBQueryItem`, and
:class:`~tests.dcb_tests.api.DCBSequencedEvent`.

The application class :class:`~tests.dcb_tests.application.DCBApplication` is a convenience introduced
by this library which constructs a concrete :class:`~tests.dcb_tests.api.DCBEventStore` according to its
environment variable configuration.

.. literalinclude:: ../../../examples/coursebookingdcb/application.py
    :pyobject: Enrolment


Test case
---------

The test case shows students and courses being registered, with students joining courses.
It is the same test as the :doc:`previous example </topics/examples/coursebooking>`.
The test is run twice, once with :class:`~tests.dcb_tests.popo.InMemoryDCBEventStore` and
then with :class:`~tests.dcb_tests.postgres.PostgresDCBEventStore`.

.. literalinclude:: ../../../examples/coursebookingdcb/test_application.py
    :pyobject: TestEnrolment


Code reference
--------------

.. automodule:: examples.coursebookingdcb.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.api
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.popo
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.postgres
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
