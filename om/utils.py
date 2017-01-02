#!coding: utf-8


def make_meta_fn(meta_name):
    """generate a `with_meta_class` function, aware `meta_name`
    Args:
        meta_name(str):

    Returns:
        function
    """
    def with_meta_class(meta_class):
        return meta_class(meta_name, (), {})
    return with_meta_class

