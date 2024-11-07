.. _Aggregate example 5:

Aggregate 5 - Immutable aggregate
=================================

This example shows another variation of the ``Dog`` aggregate class used
in the tutorial and module docs.

Like in the previous example, this example also does *not* use the library's
:class:`~eventsourcing.domain.Aggregate` class. Instead, it defines its own
``Aggregate`` and ``DomainEvent`` base classes. In contrast to the previous
examples, the aggregate is defined as a frozen data class so that it is an
immutable object. This has implications for the aggregate command methods, which must
return the events that they trigger.

The ``Dog`` aggregate is an immutable frozen data class, but it is otherwise similar
to the previous example. It explicitly defines event classes. And it explicitly
triggers events in command methods. However, it has a ``mutate()`` method which
evolves aggregate state by constructing a new instance of the aggregate class
for each event.

The application code in this example must receive the new events that
are triggered when calling the aggregate command methods, and pass them
to the ``save()`` method. The aggregate projector function must also be
supplied when getting an aggregate from the repository and when taking snapshots.

Domain model
------------

.. literalinclude:: ../../../examples/aggregate5/domainmodel.py


Application
-----------


.. literalinclude:: ../../../examples/aggregate5/application.py


Test case
---------


.. literalinclude:: ../../../examples/aggregate5/test_application.py
