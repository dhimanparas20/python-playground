def test_decorator(func):
    def wrapper(*args, **kwargs):
        print("Before Methods call")
        result = func(*args, **kwargs)
        print("After Method called")
        return result
    return wrapper

@test_decorator
def hello(param: str):
    print(f"Hello {param}")

hello("chacha")
