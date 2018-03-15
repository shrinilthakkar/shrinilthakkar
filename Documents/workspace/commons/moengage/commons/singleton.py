import threading


class SingletonMetaClass(type):
    def __init__(cls, name, bases, dictionary):
        super(SingletonMetaClass, cls).__init__(cls, bases, dictionary)
        cls._instance = None
        cls._singleton_lock = threading.Lock()

    def __call__(cls, *args):
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = super(SingletonMetaClass, cls).__call__(*args)
        return cls._instance
