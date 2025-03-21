.. _Aggregate example 7:

Aggregate 7 - Pydantic and orjson
=================================

This example shows how to use Pydantic to define immutable aggregate and event classes.

The main advantage of using Pydantic here is that any custom value objects
used in the domain model will be automatically serialised and deserialised,
without needing also to define custom :ref:`transcoding<Transcodings>` classes.

This is demonstrated in the example below with the :class:`~examples.aggregate7.domainmodel.Trick` class,
which is used in both aggregate events and aggregate state, and which is reconstructed from serialised string
values, representing only the name of the trick, from both recorded aggregate events and from recorded snapshots.

Pydantic mapper and orjson transcoder
-------------------------------------

The application class in this example uses a :ref:`mapper<Mapper>` that supports Pydantic and a :ref:`transcoder<Transcoder>` that uses orjson.

The :class:`~examples.aggregate7.orjsonpydantic.PydanticMapper` class is a
:ref:`mapper<Mapper>` that supports Pydantic. It is responsible for converting
domain model objects to object types that orjson can serialise, and for
reconstructing model objects from JSON objects that have been deserialised by orjson.

.. literalinclude:: ../../../examples/aggregate7/orjsonpydantic.py
    :pyobject: PydanticMapper

The :class:`~examples.aggregate7.orjsonpydantic.OrjsonTranscoder` class is a
:ref:`transcoder<Transcoder>` that uses orjson, possibly the fastest JSON transcoder
available in Python.

.. literalinclude:: ../../../examples/aggregate7/orjsonpydantic.py
    :pyobject: OrjsonTranscoder


Pydantic model for immutable aggregate
--------------------------------------

The code below shows how to define base classes for immutable aggregates that use Pydantic.

.. literalinclude:: ../../../examples/aggregate7/immutablemodel.py


Domain model
------------

The code below shows how to define an immutable aggregate in a functional style, using the Pydantic module for immutable aggregates.

.. literalinclude:: ../../../examples/aggregate7/domainmodel.py


Application
-----------

The :class:`~examples.aggregate7.application.DogSchool` application in this example uses the library's
:class:`~eventsourcing.application.Application` class. It must receive the new events that are returned
by the aggregate command methods, and pass them to its :func:`~eventsourcing.application.Application.save`
method. The aggregate projector function must also be supplied when reconstructing an aggregate from the
repository, and when taking snapshots.

.. literalinclude:: ../../../examples/aggregate7/application.py
    :pyobject: DogSchool


Test case
---------

The :class:`~examples.aggregate7.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate7.application.DogSchool` application can be used.

.. literalinclude:: ../../../examples/aggregate7/test_application.py
    :pyobject: TestDogSchool


Code reference
--------------

.. automodule:: examples.aggregate7.immutablemodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate7.orjsonpydantic
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate7.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate7.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate7.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

