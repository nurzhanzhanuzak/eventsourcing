class ProductNotFoundInShopError(Exception):
    pass


class ProductAlreadyInShopError(Exception):
    pass


class CartFullError(Exception):
    pass


class ProductNotInCartError(Exception):
    pass


class InsufficientInventoryError(Exception):
    pass


class CartAlreadySubmittedError(Exception):
    pass
