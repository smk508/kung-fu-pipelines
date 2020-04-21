import inspect

def foo(a: str = 'hii', b: int = 3) -> list:
    """ does stuff """
    return [a, b]

specs = inspect.getfullargspec(foo)