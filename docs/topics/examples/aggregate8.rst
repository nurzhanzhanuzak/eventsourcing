.. _Aggregate example 8:

Aggregate 8 - Pydantic with declarative syntax
==============================================

This example shows another variation of the ``Dog`` aggregate class used
in the tutorial and module docs.

Similar to the first example, the aggregate is expressed using the library's
declarative syntax. And similar to the previous example, the model events
are defined using Pydantic.

The application class in this example uses the persistence classes ``PydanticMapper``
and ``OrjsonTranscoder``, just like the previous example. Pydantic is responsible for
converting domain model objects to object types that orjson can serialise, and for
reconstructing model objects from JSON objects that have been deserialised by orjson.
The application class also uses the custom ``Snapshot`` class, which is defined as a
Pydantic model.

One advantage of using Pydantic here is that any custom value objects
will be automatically reconstructed without needing to define the
transcoding classes that would be needed when using the library's
default ``JSONTranscoder``. This is demonstrated in the example below
with the ``Trick`` class, which is used in both aggregate events and
aggregate state, and which is reconstructed from serialised string
values, representing only the name of the trick, from both recorded
aggregate events and from recorded snapshots.


Domain model
------------

.. literalinclude:: ../../../examples/aggregate8/domainmodel.py


Application
-----------


.. literalinclude:: ../../../examples/aggregate8/application.py


Persistence
-----------


.. literalinclude:: ../../../examples/aggregate8/persistence.py


Test case
---------


.. literalinclude:: ../../../examples/aggregate8/test_application.py
