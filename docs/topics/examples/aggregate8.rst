.. _Aggregate example 8:

Aggregate 8 - Pydantic with declarative syntax
==============================================

This example shows how to use Pydantic with the library's declarative syntax.

Similar to :doc:`example 1  </topics/examples/aggregate1>`, aggregates are expressed
using the library's declarative syntax. This is the most concise way of defining an
event-sourced aggregate.

Similar to :doc:`example 7  </topics/examples/aggregate7>`, domain event and custom value objects
are defined using Pydantic. The main advantage of using Pydantic here is that any custom value objects
used in the domain model will be automatically serialised and deserialised, without needing also to
define custom :ref:`transcoding<Transcodings>` classes.

Pydantic model for mutable aggregate
------------------------------------

The code below shows how to define base classes for mutable aggregates that use Pydantic.

.. literalinclude:: ../../../examples/aggregate8/mutablemodel.py


Domain model
------------

The code below shows how to define an aggregate using the Pydantic and the library's declarative syntax.

.. literalinclude:: ../../../examples/aggregate8/domainmodel.py


Application
-----------

The :class:`~examples.aggregate8.application.DogSchool` application in this example uses the library's
:class:`~eventsourcing.application.Application` class. It also uses the
:class:`~examples.aggregate7.orjsonpydantic.PydanticMapper` and
:class:`~examples.aggregate7.orjsonpydantic.OrjsonTranscoder` classes
from :doc:`example 7 </topics/examples/aggregate7>`.

.. literalinclude:: ../../../examples/aggregate8/application.py


Test case
---------

The :class:`~examples.aggregate8.test_application.TestDogSchool` test case shows how the
:class:`~examples.aggregate8.application.DogSchool` application can be used.

.. literalinclude:: ../../../examples/aggregate8/test_application.py


Code reference
--------------

.. automodule:: examples.aggregate8.mutablemodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate8.domainmodel
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate8.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

.. automodule:: examples.aggregate8.test_application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:

