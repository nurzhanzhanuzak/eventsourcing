.. _DCB example 2:

DCB 2 - Course booking - specification
======================================

This example introduces the `specification <https://dcb.events/specification/>`_ of "dyanamic
consistency boundaries" and shows how to implement the "course subscriptions" challenge in Python
using the basic objects and methods of DCB. Whilst the code in this example is relatively verbose,
the DCB approach can be understood directly without any extra abstractions. The
:doc:`next example </topics/examples/coursebooking-dcb-refactored>` presents a refactored and
hopefully more refined style for DCB that is perhaps more usable.


Enrolment with DCB
------------------

The :class:`~examples.coursebookingwithdcb.application.EnrolmentWithDCB` class implements the
:class:`~examples.coursebooking.interface.Enrolment` interface, with its methods for registering
students and courses, for students joining courses, and for listing students for a course and
the courses for a student. The methods are implemented using the DCB objects and event store methods
defined below and explained in the :doc:`previous example </topics/examples/coursebooking>`.

The application class :class:`~examples.dcb.application.DCBApplication` is a convenience introduced
to select and construct a concrete DCB event store implementation, according to its environment variable
configuration, just like the library's original :ref:`application class <Application objects>` does for
event-sourced aggregate recorders.


.. literalinclude:: ../../../examples/coursebookingdcb/application.py
    :pyobject: EnrolmentWithDCB


The test case is the same enrolement test case used in the :doc:`previous example </topics/examples/coursebooking>`,
but this time executed with the :class:`~examples.coursebookingdcb.application.EnrolmentWithDCB` class above rather than
:class:`~examples.coursebooking.application.EnrolmentWithAggregates`. The test method is run twice, once with an
in-memory implementation of the DCB event store, and again with a PostgreSQL implementation (see below).

.. literalinclude:: ../../../examples/coursebookingdcb/test_application.py
    :pyobject: TestEnrolmentWithDCB


DCB event store interface
-------------------------

The DCB classes used in this example are defined below, :class:`~examples.dcb.api.DCBEvent`,
:class:`~examples.dcb.api.DCBQuery`, :class:`~examples.dcb.api.DCBQueryItem`,
:class:`~examples.dcb.api.DCBAppendCondition` and :class:`~examples.dcb.api.DCBSequencedEvent`.

.. literalinclude:: ../../../examples/dcb/api.py
    :pyobject: DCBEvent

.. literalinclude:: ../../../examples/dcb/api.py
    :pyobject: DCBQuery

.. literalinclude:: ../../../examples/dcb/api.py
    :pyobject: DCBQueryItem

.. literalinclude:: ../../../examples/dcb/api.py
    :pyobject: DCBAppendCondition

.. literalinclude:: ../../../examples/dcb/api.py
    :pyobject: DCBSequencedEvent


The :class:`~examples.dcb.api.DCBEventStore` class defines an interface that has methods described in the DCB
specification for reading and appending DCB events. There is one enhancement, which is to return an :class:`int`
from the :func:`~examples.dcb.api.DCBEventStore.append` method. This supports returning the position of last appended
event, so that user interfaces for systems implemented with CQRS, that have eventually consistent "read" models,
can transition from a "write" view to a "read" view and wait for new events to be processed, avoiding the stale
read model problem.

.. literalinclude:: ../../../examples/dcb/api.py
    :pyobject: DCBEventStore




In-memory DCB event store
-------------------------

The :class:`~examples.dcb.popo.InMemoryDCBEventStore` class implements the DCB event store interface
using only Python objects. You can see the query logic for selecting events implemented with nested
generator expressions, and the append condition logic that is implemented in the append method. DCB
events are stored in memory, and "deep copied" when appending and when reading to avoid any corruption
of sequenced events.

.. literalinclude:: ../../../examples/dcb/popo.py
    :pyobject: InMemoryDCBEventStore


Postgres DCB event store
------------------------

The :class:`~examples.dcb.postgres.PostgresDCBEventStore` class implements the DCB event store interface
using Postgres. After experimenting with different approaches, this version implements the complex DCB
query logic in the database with the Postgres ``tsvector`` and ``tsquery`` types. The type and tags of a
DCB event are prefixed and concatenated into a ``tsvector`` string. A set of DCB query items is similarly
compounded into a logical ``tsquery`` that expressed the DCB query logic. Database functions for appending
and selecting events are defined, and a custom composite type is defined for efficiently sending an array of
DCB events to the database. In this way, both the read and the append operations of this DCB event store can
be executed as fast as possible with a single database round-trip.

.. literalinclude:: ../../../examples/dcb/postgres.py
    :pyobject: PostgresDCBEventStore


Code reference
--------------

.. automodule:: examples.coursebookingdcb.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.dcb.api
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.dcb.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.dcb.popo
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: examples.dcb.postgres
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
