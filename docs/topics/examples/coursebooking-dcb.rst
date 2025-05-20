.. _DCB example 2:

2 - Course booking - with DCB style
===================================

This example shows how to implement the "course booking" example in Python, using the
standard style of `dynamic consistency boundaries <https://dcb.events/>`_, rather than
standard event-sourced aggregates. It closely follows the objects and methods
described in the `specification <https://dcb.events/specification/>`_.

Application
-----------

The application has the same methods as the :doc:`previous example </topics/examples/coursebooking>`
for joining students with courses and for listing students for a course and courses for a student.
The methods are implemented using the "DCB" classes, :class:`~tests.dcb_tests.api.DCBEvent`,
:class:`~tests.dcb_tests.api.DCBQuery`, :class:`~tests.dcb_tests.api.DCBQueryItem`,
:class:`~tests.dcb_tests.api.DCBAppendCondition` and :class:`~tests.dcb_tests.api.DCBSequencedEvent`.

An abstract base class :class:`~tests.dcb_tests.api.DCBEventStore` is also defined. This abstract base
class for recording DCB events has been implemented twice, once as an in-memory recorder
:class:`~tests.dcb_tests.popo.InMemoryDCBEventStore`, and again for PostgreSQL with the
:class:`~tests.dcb_tests.postgres.PostgresDCBEventStore` class which uses SQL composite data types and
stored procedures to implement the complex query and append logic of DCB in a way that performs well.

The application class :class:`~tests.dcb_tests.application.DCBApplication` is a convenience introduced
by this library which constructs a concrete :class:`~tests.dcb_tests.api.DCBEventStore` recorder according
to its environment variable configuration, just like the library's original
:ref:`application class <Application objects>`.


.. literalinclude:: ../../../examples/coursebookingdcb/application.py
    :pyobject: EnrolmentWithDCB



Test case
---------

The test case is the same as the :doc:`previous example </topics/examples/coursebooking>`, but executed
with the :class:`~examples.coursebookingdcb.application.EnrolmentWithDCB` class above.

.. literalinclude:: ../../../examples/coursebookingdcb/test_application.py
    :pyobject: TestEnrolmentWithDCB


Code reference
--------------

.. automodule:: examples.coursebookingdcb.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.api
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.application
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.popo
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__

.. automodule:: tests.dcb_tests.postgres
    :show-inheritance:
    :member-order: bysource
    :members:
    :undoc-members:
    :special-members: __init__
