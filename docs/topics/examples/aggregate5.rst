.. _Aggregate example 5:

Aggregate 5 - Immutable aggregate
=================================

This example shows how to define and use your own immutable aggregate base class.

Base classes
------------

Like in the previous example, this example also does *not* use the library's
:ref:`aggregate base class <Aggregate base class>`.

The base class for aggregate events, :class:`~examples.aggregate5.baseclasses.DomainEvent`, is defined as
a "frozen" Python :class:`dataclass`.

.. literalinclude:: ../../../examples/aggregate5/baseclasses.py
    :pyobject: DomainEvent

The aggregate base class, :class:`~examples.aggregate5.baseclasses.Aggregate`, is also defined as a "frozen" Python
:class:`dataclass`. This has implications for the aggregate command methods, which must
return the events that they trigger.

It defines a :func:`~examples.aggregate5.baseclasses.Aggregate.trigger_event` method, which can be called by
aggregate command methods, and which does the common work of constructing an event object with an incremented
version number and a new timestamp.

It defines a :func:`~examples.aggregate5.baseclasses.Aggregate.projector` class method which reconstructs an aggregate
object by iterating over events, calling the aggregate class's :func:`~examples.aggregate5.baseclasses.Aggregate.mutate`
method for each event.

It also defines a :class:`~examples.aggregate5.baseclasses.Aggregate.Snapshot` class which is a
:class:`~examples.aggregate5.baseclasses.DomainEvent` that can carry the state of an aggregate,
and which has a :func:`~examples.aggregate5.baseclasses.Aggregate.Snapshot.take` method that can
construct a snapshot object from an aggregate object.

.. literalinclude:: ../../../examples/aggregate5/baseclasses.py
    :pyobject: Aggregate


Domain model
------------

The :class:`~examples.aggregate5.domainmodel.Dog` aggregate is defined as immutable frozen data class that extends
the aggregate base class. Event classes are explicitly defined and explicitly triggered in command methods.

It defines a :func:`~examples.aggregate5.domainmodel.Dog.mutate` method, which
evolves aggregate state by constructing a new instance of the aggregate class each time it is called,
according to the type of event it is called with.

.. literalinclude:: ../../../examples/aggregate5/domainmodel.py
    :pyobject: Dog

Application
-----------

The :class:`~examples.aggregate5.application.DogSchool` application in this example uses the library's
:class:`~eventsourcing.application.Application` class. It must receive the new events that are returned
by the aggregate command methods, and pass them to its :func:`~eventsourcing.application.Application.save`
method. The aggregate projector function must also be supplied when reconstructing an aggregate from the
repository, and when taking snapshots.


.. literalinclude:: ../../../examples/aggregate5/application.py
    :pyobject: DogSchool


Test case
---------

The :class:`~examples.aggregate5.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate5.application.DogSchool` application can be used.

.. literalinclude:: ../../../examples/aggregate5/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate5.baseclasses
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate5.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate5.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate5.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

