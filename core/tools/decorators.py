import gc
import threading


def fire_and_forget(func):
    def threaded_func(*args, **kwargs):
        result = func(*args, **kwargs)
        gc.collect()  # Explicitly run the garbage collector
        return result

    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=threaded_func, args=args, kwargs=kwargs)
        thread.start()
    return wrapper