.. _Aggregate example 4:

Aggregate 4 - Custom base classes
=================================

This example shows how to define and use your own mutable aggregate base class.


Base classes
------------

In this example, the base classes :class:`~examples.aggregate4.baseclasses.DomainEvent`
and :class:`~examples.aggregate4.baseclasses.Aggregate` are defined independently of the library.

The :class:`~examples.aggregate4.baseclasses.DomainEvent` class in this example is defined as a
frozen Python :class:`dataclass`.

.. literalinclude:: ../../../examples/aggregate4/baseclasses.py
    :pyobject: DomainEvent

The :class:`~examples.aggregate4.baseclasses.Aggregate` class in this example is coded to
have the common aspects of mutable aggregate objects. It is coded to conform with the library's
protocol classes :class:`~eventsourcing.domain.CollectEventsProtocol` and :class:`~eventsourcing.domain.MutableAggregateProtocol`
so that it can be used with the library's :class:`~eventsourcing.application.Application` class.

.. literalinclude:: ../../../examples/aggregate4/baseclasses.py
    :pyobject: Aggregate

It has a :class:`~examples.aggregate4.baseclasses.Aggregate.Snapshot` class, which
has a :func:`~examples.aggregate4.baseclasses.Aggregate.Snapshot.take` method that
can create a snapshot of an aggregate object.

It has a :func:`~examples.aggregate4.baseclasses.Aggregate.trigger_event` method, which
constructs new domain event objects, applies them to the aggregate, and appends them to an
internal list of "pending" events".

It has a :func:`~examples.aggregate4.baseclasses.Aggregate.collect_events` method, which
drains the internal list of new "pending" events", so that they can be recorded.

Like in :doc:`example 3  </topics/examples/aggregate3>`, it has an
:func:`~examples.aggregate4.baseclasses.Aggregate.apply` method which is
decorated with the :class:`@singledispatchmethod <eventsourcing.dispatch.singledispatchmethod>`
decorator. A method that supports the :class:`~examples.aggregate4.baseclasses.Aggregate.Snapshot` class
is registered, so that an aggregate can be reconstructed from a snapshot.

It also has a :func:`~examples.aggregate4.baseclasses.Aggregate.projector` class method, which
can reconstruct an aggregate from a list of domain events.


Domain model
------------

The :class:`~examples.aggregate4.domainmodel.Dog` class in this example does *not* use the
library's aggregate base class. It is expressed using the independent base classes
:class:`~examples.aggregate4.baseclasses.DomainEvent` and :class:`~examples.aggregate4.baseclasses.Aggregate`
defined above.

The aggregate event classes are explicitly defined, and the command method
bodies explicitly trigger events.

.. literalinclude:: ../../../examples/aggregate4/domainmodel.py
    :pyobject: Dog

Like in :doc:`example 3  </topics/examples/aggregate3>`, it has an
:func:`~examples.aggregate4.domainmodel.Dog.apply` method which is
decorated with the :class:`@singledispatchmethod <eventsourcing.dispatch.singledispatchmethod>`
decorator. Methods that support aggregate events classes are registered.


Application
-----------

As in the previous examples, the :class:`~examples.aggregate4.application.DogSchool`
application class simply uses the aggregate class as if it were a normal Python object
class. However, the aggregate projector function must be supplied when getting an
aggregate from the repository and when taking snapshots.

.. literalinclude:: ../../../examples/aggregate4/application.py
    :pyobject: DogSchool


Test case
---------

The :class:`~examples.aggregate4.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate4.application.DogSchool` application can be used.


.. literalinclude:: ../../../examples/aggregate4/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate4.baseclasses
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate4.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate4.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate4.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

