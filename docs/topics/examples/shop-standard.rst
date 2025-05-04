.. _Shop application example:

Application 6 - Shopping cart
=============================

This example suggests how a shopping cart might be implemented.


Domain model
------------

.. literalinclude:: ../../../examples/shopstandard/domain.py
    :pyobject: Product

.. literalinclude:: ../../../examples/shopstandard/domain.py
    :pyobject: CartItem

.. literalinclude:: ../../../examples/shopstandard/domain.py
    :pyobject: Cart

Application
-----------

.. literalinclude:: ../../../examples/shopstandard/application.py
    :pyobject: Shop

.. literalinclude:: ../../../examples/shopstandard/domain.py
    :pyobject: ProductDetails


Exceptions
----------

.. literalinclude:: ../../../examples/shopvertical/exceptions.py

Test
----

.. literalinclude:: ../../../examples/shopvertical/test.py
    :pyobject: TestShop
