import logging
import threading

g_threadlocal = threading.local()
g_context_log_fields = ["domain_name", "user_name", "request_id"]

class DomainLogAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        extra = kwargs['extra']

        context = kwargs.pop('context', None)
        if not context:
            context = g_threadlocal

        for f in g_context_log_fields:
            if hasattr(context, f):
                v = getattr(context, f)
            else:
                v = "(undefined)"
            extra[f] = v
        return msg, kwargs

class DomainLogFilter(logging.Filter):
    """
    This log filter adds the defined fields for the epu logs.  It does not actually filter any log records out,
    it simply changes the contents of the log record based on thread specific data.
    """
    def filter(self, record):
        # set any value needed
        domain_info = ""
        for f in g_context_log_fields:
            if hasattr(g_threadlocal, f):
                v = getattr(g_threadlocal, f)
                epu_format = "%s %s=%s" % (epu_format, f, v)
        record.domain_info = domain_info.strip()
        return True


class EpuLoggerThreadSpecific():
    def __init__(self, **kw):
        self.kw = kw.copy()

    def __enter__(self):
        for key in self.kw:
            setattr(g_threadlocal, key, self.kw[key])
        return None

    def __exit__(self, type, value, traceback):
        for key in self.kw:
           delattr(g_threadlocal, key)
        return None



