.. _Aggregate example 6:

Aggregate 6 - Functional style
==============================

This example shows how to define and use your own immutable aggregate base class with a more "functional"
style than :doc:`example 5  </topics/examples/aggregate5>`.

Base classes
------------

The :class:`~examples.aggregate6.baseclasses.DomainEvent` class is defined as a "frozen" Python :class:`dataclass`.

.. literalinclude:: ../../../examples/aggregate6/baseclasses.py
    :pyobject: DomainEvent

The :class:`~examples.aggregate6.baseclasses.Aggregate` base class in this example is also defined as a "frozen" Python
:class:`dataclass`.

.. literalinclude:: ../../../examples/aggregate6/baseclasses.py
    :pyobject: Aggregate

The :class:`~examples.aggregate6.baseclasses.Snapshot` class in this example is also defined as a "frozen" Python
:class:`dataclass` that extends :class:`~examples.aggregate6.baseclasses.DomainEvent`.

.. literalinclude:: ../../../examples/aggregate6/baseclasses.py
    :pyobject: Snapshot

A generic :class:`~examples.aggregate6.baseclasses.aggregate_projector` function is also defined, which takes
a mutator function and returns a function that can reconstruct an aggregate of a particular type from an iterable
of domain events.

.. literalinclude:: ../../../examples/aggregate6/baseclasses.py
    :pyobject: aggregate_projector



Domain model
------------

The :class:`~examples.aggregate6.domainmodel.Dog` aggregate class is defined as immutable frozen data class
that extends the aggregate base class.

.. literalinclude:: ../../../examples/aggregate6/domainmodel.py
    :pyobject: Dog

The aggregate event classes, :class:`~examples.aggregate6.domainmodel.DogRegistered` and
:class:`~examples.aggregate6.domainmodel.TrickAdded`, are explicitly defined as separate module level classes.

.. literalinclude:: ../../../examples/aggregate6/domainmodel.py
    :pyobject: DogRegistered

.. literalinclude:: ../../../examples/aggregate6/domainmodel.py
    :pyobject: TrickAdded

The aggregate commands, :func:`~examples.aggregate6.domainmodel.register_dog` and
:func:`~examples.aggregate6.domainmodel.add_trick` are defined as module level functions.

.. literalinclude:: ../../../examples/aggregate6/domainmodel.py
    :pyobject: register_dog

.. literalinclude:: ../../../examples/aggregate6/domainmodel.py
    :pyobject: add_trick

The mutator function, :func:`~examples.aggregate6.domainmodel.mutate_dog`, is defined as a module level function.

.. literalinclude:: ../../../examples/aggregate6/domainmodel.py
    :start-at: @singledispatch
    :end-before: project_dog

The aggregate projector function, :func:`~examples.aggregate6.domainmodel.project_dog`, is defined as a module
level function by calling :func:`~examples.aggregate6.baseclasses.aggregate_projector` with
:func:`~examples.aggregate6.domainmodel.mutate_dog` as the argument.

.. literalinclude:: ../../../examples/aggregate6/domainmodel.py
    :start-at: project_dog


Application
-----------

The :class:`~examples.aggregate6.application.DogSchool` application in this example uses the library's
:class:`~eventsourcing.application.Application` class. It must receive the new events that are returned
by the aggregate command methods, and pass them to its :func:`~eventsourcing.application.Application.save`
method. The aggregate projector function must also be supplied when reconstructing an aggregate from the
repository, and when taking snapshots.

.. literalinclude:: ../../../examples/aggregate6/application.py
    :pyobject: DogSchool


Test case
---------

The :class:`~examples.aggregate6.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate6.application.DogSchool` application can be used.

.. literalinclude:: ../../../examples/aggregate6/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate6.baseclasses
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate6.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate6.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate6.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

