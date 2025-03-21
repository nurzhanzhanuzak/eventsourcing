.. _Aggregate example 2:

Aggregate 2 - Explicit event classes
====================================

This example shows an aggregate that uses the library's :ref:`explicit syntax  <Using an explicitly defined event class>`
for defining aggregate events, as described in the :doc:`tutorial </topics/tutorial>` and :doc:`module docs </topics/modules>`. The
difference between this example and :doc:`example 1  </topics/examples/aggregate1>` is that the aggregate
event classes are defined explicitly.


Domain model
------------

The :class:`~examples.aggregate2.domainmodel.Dog` class in this example uses the library's
:ref:`aggregate base class <Aggregate base class>` and the :ref:`event decorator <Event decorator>`.
Event classes that match command method signatures are defined explicitly. Events are triggered by the decorator
when the command methods are called, and the bodies of the command methods are used by the events to mutate
the state of the aggregate, both after command methods are called and when reconstructing the state of an
aggregate from stored events.

.. literalinclude:: ../../../examples/aggregate2/domainmodel.py
    :pyobject: Dog


Application
-----------

As in :doc:`example 1  </topics/examples/aggregate1>`, the :class:`~examples.aggregate2.application.DogSchool`
application class in this example uses the library's :ref:`application base class <Application objects>`. It
fully encapsulates the :class:`~examples.aggregate2.domainmodel.Dog` aggregate, defining command and query methods
that use the event-sourced aggregate class as if it were a normal Python object class.

.. literalinclude:: ../../../examples/aggregate2/application.py
    :pyobject: DogSchool


Test case
---------

The :class:`~examples.aggregate2.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate2.application.DogSchool` application can be used.

.. literalinclude:: ../../../examples/aggregate2/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate2.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate2.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate2.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

