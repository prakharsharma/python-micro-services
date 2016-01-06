"""
"""


class BadServiceConfigError(RuntimeError):
    pass


class ServiceRegistrationError(RuntimeError):
    pass


class ServiceNotAvailableError(RuntimeError):
    pass


class ServiceFunctionNotAvailableError(RuntimeError):
    pass


class ServiceFunctionConfigError(RuntimeError):
    pass


class UnknownSocketTypeError(RuntimeError):
    pass


class StopServiceError(RuntimeError):
    pass


class ServiceTimeoutError(RuntimeError):

    def __init__(self, service, function, timeout, max_tries,
                 sleep_before_retry):
        self.service_name = service
        self.function_name = function
        self.timeout = timeout
        self.max_tries = max_tries
        self.sleep_before_retry = sleep_before_retry

    def __repr__(self):
        return 'ServiceTimeoutError(service=%s, function=%s, timeout=%s, ' \
               'max_tries=%s, sleep_before_retry=%s)' % \
               (self.service_name, self.function_name, self.timeout,
               self.max_tries, self.sleep_before_retry)


class ServiceError(RuntimeError):
    pass


class BadServiceRequestError(RuntimeError):
    pass


class ServiceHandlerUncaughtError(RuntimeError):
    pass


class BadServiceMessageHandlerError(RuntimeError):
    pass
