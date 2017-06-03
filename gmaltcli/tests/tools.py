class MockCallable(object):
    def __init__(self):
        self.called = False
        self.args = ()
        self.kwargs = {}

    def __call__(self, *args, **kwargs):
        self.called = True
        self.args = args
        self.kwargs = kwargs
