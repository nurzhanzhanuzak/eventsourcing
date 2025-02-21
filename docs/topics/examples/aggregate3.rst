.. _Aggregate example 3:

Aggregate 3 - Explicit trigger and apply
========================================

This example shows how to :ref:`explicitly trigger events <Triggering subsequent events>` within aggregate command
methods, and an :ref:`alternative style <Alternative styles for implementing aggregate projector>`
for implementing aggregate projector functions.

Domain model
------------

The :class:`~examples.aggregate3.domainmodel.Dog` class in this example uses the library's
:ref:`aggregate base class <Aggregate base class>`. However, this example does not use the event decorator
that was used in :doc:`example 1  </topics/examples/aggregate1>` and :doc:`example 2  </topics/examples/aggregate2>`,
but instead explicitly triggers aggregate events from within command method bodies, by calling
:class:`~eventsourcing.domain.Aggregate.trigger_event`.

It also defines a separate aggregate projector
function, :func:`~examples.aggregate3.domainmodel.Dog.apply` which is decorated with
:class:`@singledispatchmethod <eventsourcing.dispatch.singledispatchmethod>`. Event-specific methods are registered with the
:func:`~examples.aggregate3.domainmodel.Dog.apply` method, and invoked when the method is called with
that type of event. To make this work, an :class:`~examples.aggregate3.domainmodel.Dog.Event` class common to
all the aggregate's events is defined, which calls the aggregate's :func:`~examples.aggregate3.domainmodel.Dog.apply`
method.


.. literalinclude:: ../../../examples/aggregate3/domainmodel.py
    :pyobject: Dog


Application
-----------

As in :doc:`example 1  </topics/examples/aggregate1>` and :doc:`example 2  </topics/examples/aggregate2>`,
the :class:`~examples.aggregate3.application.DogSchool` application class in this example uses the library's
:ref:`application base class <Application objects>`. It fully encapsulates the
:class:`~examples.aggregate3.aggregate.Dog` aggregate, defining command and query methods
that use the event-sourced aggregate class as if it were a normal Python object class.

.. literalinclude:: ../../../examples/aggregate3/application.py
    :pyobject: DogSchool


Test case
---------

The :class:`~examples.aggregate3.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate3.application.DogSchool` application can be used.

.. literalinclude:: ../../../examples/aggregate3/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate3.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate3.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate3.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

