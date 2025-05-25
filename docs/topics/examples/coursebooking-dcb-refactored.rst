.. _DCB example 3:

DCB 3 - Course booking - refactored
===================================

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

These refactorings improve the readability and integrity of the code, reducing the source lines of code by 34%
(from 164 sloc to 109). However, it's worth noting that this example application code still has 47%
more code than the domain model and application code using aggregates in the
:doc:`first example </topics/examples/coursebooking>` (74 sloc).

Application
-----------

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: StudentRegistered

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: CourseRegistered

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: StudentJoinedCourse

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/application.py
    :pyobject: EnrolmentWithDCBRefactored


Test case
---------

The test case is the same as the :doc:`first example </topics/examples/coursebooking>`, but executed
with the :class:`~examples.coursebookingdcbrefactored.application.EnrolmentWithDCBRefactored` class above.

.. literalinclude:: ../../../examples/coursebookingdcbrefactored/test_application.py
    :pyobject: TestEnrolmentWithDCBRefactored



Code reference
--------------

.. automodule:: examples.coursebookingdcbrefactored.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.coursebookingdcbrefactored.eventstore
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
