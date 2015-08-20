__author__ = 'sarangis'

import types, copyreg, multiprocessing as mp

def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    cls_name = ''
    if func_name.startswith('__') and not func_name.endswith('__'):
        cls_name = cls.__name__.lstrip('_')
    if cls_name:
        func_name = '_' + cls_name + func_name
    return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
    func = None
    for cls in cls.mro():
        if func_name in cls.__dict__:
            func = cls.__dict__[func_name] # This will fail with the decorator, since parSquare is being wrapped around as executor
            break
        else:
            for attr in dir(cls):
                prop = getattr(cls, attr)
                if hasattr(prop, '__call__') and prop.__name__ == func_name:
                    func = cls.__dict__[attr]
                    break
    if func == None:
        raise KeyError("Couldn't find function %s withing %s" % (str(func_name), str(cls)))
    return func.__get__(obj, cls)

copyreg.pickle(types.MethodType, _pickle_method, _unpickle_method)

def parallel(f, callback):
    def temp(_):
        def chunks(self, l, n):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield l[i:i+n]

        def executor(*args):
            num_cpu_cores = 8
            _pool   = mp.Pool(num_cpu_cores)
            func = getattr(args[0], f.__name__) # This will get the actual method function so we can use our own pickling procedure
            callback_func = getattr(args[0], callback.__name__)
            _result = _pool.map_async(func, args[1], callback=callback_func)
            _pool.close()
            _pool.join()
            return _result
        return executor
    return temp

class ParallelBase(object):
    """ Use this class as the base class for running parallel methods.
        Override the runPar method in your class and you should be set to use this.
    """
    def __init__(self):
        self.result = None
        pass

    def runPar(self, arg):
        pass

    def runComplete(self, arg):
        pass

    def _map(self, arg):
        return self.runPar(arg)

    @parallel(_map, runComplete)
    def map(self, target, arg):
        pass


    # def _parSquare(self, num):
    #     return self.squareArg(num)
    #
    # @parallel(_parSquare)
    # def parSquare(self, num):
    #     pass


# class multiprocess_decorator(object):
#     def __init__(self, func):
#         print("Calling Decorator")
#         self.func = func
#         self.cache = {}
#         functools.update_wrapper(self, func)
#         self.num_cpu_cores = 1
#         self.pool = multiprocessing.Pool(self.num_cpu_cores)
#         self.chunked_data = []
#
#     def chunks(self, l, n):
#         """Yield successive n-sized chunks from l."""
#         for i in range(0, len(l), n):
#             self.chunked_data.append(l[i:i+n])
#
#     def __call__(self, *args, **kwargs):
#         print(args)
#         split_size = int(len(args[0]) / self.num_cpu_cores)
#
#         if (len(args[0]) % self.num_cpu_cores != 0):
#             split_size += 1
#
#         self.chunks(args[0], split_size)
#         print("__call__ function of decorator")
#
#         print(self.func)
#
#         jobs = []
#         for i in range(0, self.num_cpu_cores):
#             out_list = list()
#             process = multiprocessing.Process(target=self.func,
#                                               args=(self.chunked_data[i]))
#             jobs.append(process)
#
#         # Start the processes (i.e. calculate the random number lists)
#         for j in jobs:
#             j.start()
#
#         # Ensure all of the processes have finished
#         for j in jobs:
#             j.join()
#
#         # results = [self.pool.map(self.func, self.chunked_data[i]) for i in range(len(self.chunked_data))]
#
# # class MyMethod:
# #     def __init__(self):
# #         pass
#
# @multiprocess_decorator
# def worker(self, list=None):
#     print("In Worker")
#     yield [x*x for x in list]


# class MapReduce:
#     def __init__(self):
#         self.num_cpu_cores = 8
#         self.pool = multiprocessing.Pool(self.num_cpu_cores)
#         self.chunked_data = []
#
#     def _chunks(self, l, n):
#         """Yield successive n-sized chunks from l."""
#         for i in range(0, len(l), n):
#             self.chunked_data.append(l[i:i+n])
#
#     def map(self, target, tlist):
#         result = self.pool.map(target, tlist, chunksize=len(tlist) / self.num_cpu_cores)
#         print(result)
#         return self
#
#     def reduce(self):
#         return self
#
#
# def worker(tlist):
#     print(tlist)

# class Copier(object):
#     def __init__(self):
#         pass
#
#     def __call__(self, val):
#         yield val**2

