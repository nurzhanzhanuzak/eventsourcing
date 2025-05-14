.. _Aggregate example 11:

Aggregate 11 - String IDs
=========================

This example shows an aggregate that uses the library's declarative syntax for aggregates, as described
in the :doc:`tutorial </topics/tutorial>` and :doc:`module docs </topics/modules>`, but with arbitrary
string IDs. Many users of KurrentDB, for example, prefer to prefix stream names with the name of a
stream category. This example shows how this style can be adopted when using this library.

Domain model
------------

The :class:`~examples.aggregate11.domainmodel.Dog` class in this example uses the library's
:ref:`aggregate base class <Aggregate base class>` and the :ref:`event decorator <Event decorator>`
to define aggregate event classes from command method signatures. The event class names
are given as the argument to the event decorator. The event attributes are defined automatically
by the decorator to match the command method arguments. The bodies of the command methods are used
to evolve the state of an aggregate instance, both when a new event is triggered and when an aggregate
is reconstructed from stored events.

.. literalinclude:: ../../../examples/aggregate11/domainmodel.py


Application
-----------

The :class:`~examples.aggregate11.application.DogSchool` application class in this example uses the
library's :ref:`application base class <Application objects>`. It fully encapsulates the
:class:`~examples.aggregate11.aggregate.Dog` aggregate, defining command and query methods
that use the event-sourced aggregate class as if it were a normal Python object class.

.. literalinclude:: ../../../examples/aggregate11/application.py


Test case
---------

The :class:`~examples.aggregate11.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate11.application.DogSchool` application can be used. It demonstrates
arbitrary string can be used as aggregate IDs, with both the POPO and SQLite persistence modules,
and of course with KurrentDB.

.. literalinclude:: ../../../examples/aggregate11/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate11.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.aggregate11.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.aggregate11.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
