.. _DCB example 2:

3 - Course booking - refactored DCB
===================================

This example shows how the same "course booking" example as the :doc:`previous example </topics/examples/coursebooking-dcb>`,
using a refactored style for `dynamic consistency boundaries <https://dcb.events/>`_, rather than
standard style suggested by the `specification <https://dcb.events/specification/>`_.

Model-specific domain event classes,
:class:`~examples.coursebookingdcb2.application.StudentRegistered`,
:class:`~examples.coursebookingdcb2.application.CourseRegistered`,
:class:`~examples.coursebookingdcb2.application.StudentJoinedCourse`.
are defined to help with type checking and code navigation. They are used instead of
the :class:`~tests.dcb_tests.api.DCBEvent` class in the application code.

The base domain event class :class:`~examples.coursebookingdcb2.mapper.DomainEvent` is defined using
the Python :data:`msgspec` package which defines dataclasses from type annotations, and is currently the fastest available
Python serialisation library giving the smallest packed sizes for serialised data in :class:`~tests.dcb_tests.api.DCBEvent`
objects.

A :class:`~examples.coursebookingdcb2.mapper.Mapper` is used when writing events to convert subclass instances of
:class:`~examples.coursebookingdcb2.mapper.DomainEvent` to the :class:`~tests.dcb_tests.api.DCBEvent`
class, and to convert back from instances of :class:`~tests.dcb_tests.api.DCBSequencedEvent` to the
domain event classes when reading events. This also encapsulates the serialisation and deserialisation of event data
that was visible in the previous example.

The :class:`~examples.coursebookingdcb2.mapper.Selector` class is used instead of the :class:`~tests.dcb_tests.api.DCBQuery`
and :class:`~tests.dcb_tests.api.DCBQueryItem` classes to define the consistency boundary for the command method, and in the
query methods. The :class:`~examples.coursebookingdcb2.mapper.Selector` class uses the domain event classes to indicate
selected types rather than strings. Instances, and sequences of instances, of :class:`~examples.coursebookingdcb2.mapper.Selector`
are converted to :class:`~tests.dcb_tests.api.DCBQuery` objects by the :class:`~examples.coursebookingdcb2.mapper.EventStore` class.

The :class:`~tests.dcb_tests.api.DCBEventStore` interface is encapsulated by the
:class:`~examples.coursebookingdcb2.mapper.EventStore` class. The :func:`~examples.coursebookingdcb2.mapper.EventStore.put`
and :func:`~examples.coursebookingdcb2.mapper.EventStore.get` methods of the
:class:`~examples.coursebookingdcb2.mapper.EventStore` class support passing either a single instance
of :class:`~examples.coursebookingdcb2.mapper.Selector` or a sequence, which simplifies code statements.

The :func:`EventStore.get <examples.coursebookingdcb2.mapper.EventStore.get>` method is overloaded with
three method signatures. It returns only domain events by default. But has optional arguments to request the
return of events each with their sequenced position. And alternatively, to return a sequence of events with
along with a single position indicating the last known position. This last option is most useful in a command
method for subsequently appending new events with selectors and the last known position, to ensure consistency
of the recorded data according the the technique for dynamic consistency boundaries. In this example at least,
only the command method actually needs the sequenced positions, and it only needs the last position of the events
in its consistency boundary. The query methods in this example do not need to receive the sequenced positions of
the recorded events, and so call :func:`~examples.coursebookingdcb2.mapper.EventStore.get()` with its default of only
returning domain events.

These refactorings improve the readability and integrity of the code, reducing the source lines of code by 34%
(from 167 sloc to 107). However, it's worth noting that this example application code still has greater than 50%
more code than the domain model and application code using aggregates in the
:doc:`first example </topics/examples/coursebooking>` (70 sloc).

Application
-----------

.. literalinclude:: ../../../examples/coursebookingdcb2/application.py
    :pyobject: StudentRegistered

.. literalinclude:: ../../../examples/coursebookingdcb2/application.py
    :pyobject: CourseRegistered

.. literalinclude:: ../../../examples/coursebookingdcb2/application.py
    :pyobject: StudentJoinedCourse

.. literalinclude:: ../../../examples/coursebookingdcb2/application.py
    :pyobject: Enrolment


Test case
---------

.. literalinclude:: ../../../examples/coursebookingdcb2/test_application.py
    :pyobject: TestEnrolment


Code reference
--------------

.. automodule:: examples.coursebookingdcb2.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.coursebookingdcb2.mapper
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
