.. _Aggregate example 1:

Aggregate 1 - Declarative syntax
================================

This example shows the :class:`~examples.aggregate1.domainmodel.Dog` class that was described in detail
when describing the library's declarative syntax for aggregates in the
:doc:`tutorial </topics/tutorial>` and :doc:`module docs </topics/modules>`.

Domain model
------------


The :class:`~examples.aggregate1.domainmodel.Dog` class in this example uses the library's
:ref:`aggregate base class <Aggregate base class>` and the :ref:`event decorator <Event decorator>`
to define aggregate event classes from command method signatures. The event class names
are given as the argument to the event decorator. The event attributes are defined automatically
by the decorator to match the command method arguments. The bodies of the command methods are used
to evolve the state of an aggregate instance, both when a new event is triggered and when an aggregate
is reconstructed from stored events.

.. literalinclude:: ../../../examples/aggregate1/domainmodel.py
    :pyobject: Dog


Application
-----------

The :class:`~examples.aggregate1.application.DogSchool` application class in this example uses the
library's :ref:`application base class <Application objects>`. It fully encapsulates the
:class:`~examples.aggregate1.aggregate.Dog` aggregate, defining command and query methods
that use the event-sourced aggregate class as if it were a normal Python object class.

.. literalinclude:: ../../../examples/aggregate1/application.py
    :pyobject: DogSchool


Test case
---------

The :class:`~examples.aggregate1.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate1.application.DogSchool` application can be used.

.. literalinclude:: ../../../examples/aggregate1/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate1.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.aggregate1.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.aggregate1.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

