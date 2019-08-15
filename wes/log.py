import unittest
import sys
from datetime import datetime

def _TIME():
    #return datetime.now().strftime('%Y/%m/%d %H:%m:%S.%f')[:-3]
    return datetime.now().strftime('%H:%m:%S.%f')[:-3]

def _generate_caller_name():
    return sys._getframe(3).f_code.co_name

def flush_io():
    sys.stdout.flush()

def _LOG_INT(prefix, msg):
    #print("%sLOG::%s: %s" % (prefix, generate_caller_name(), msg))
    print("%s%s LOG :: %s" % (prefix, _TIME(), str(msg)))
    flush_io()

# print colored short
def _LOG_RED_B(prefix, msg):
    print("\33[1;37;41m%s%s ERR ::\33[0m %s" % (prefix, _TIME(),  str(msg)))
    flush_io()

def _LOG_GREEN_B(prefix, msg):
    print("\33[1;37;42m%s%s LOG ::\33[0m %s" % (prefix, _TIME(),  str(msg)))
    flush_io()

def _LOG_YELLOW_B(prefix, msg):
    print("\33[1;37;43m%s%s WARN::\33[0m %s" % (prefix, _TIME(),  str(msg)))
    flush_io()

def _LOG_BLUE_B(prefix, msg):
    print("\33[1;37;44m%s%s NOTI::\33[0m %s" % (prefix, _TIME(),  str(msg)))
    flush_io()

# print colored long
def _LOG_RED_B_LONG(prefix, msg):
    print("\33[1;37;41m%s%s ERR :: %s\33[0m" % (prefix, _TIME(),  str(msg)))
    flush_io()

def _LOG_GREEN_B_LONG(prefix, msg):
    print("\33[1;37;42m%s%s LOG :: %s\33[0m" % (prefix, _TIME(),  str(msg)))
    flush_io()

def _LOG_YELLOW_B_LONG(prefix, msg):
    print("\33[1;37;43m%s%s WARN:: %s\33[0m" % (prefix, _TIME(),  str(msg)))
    flush_io()

def _LOG_BLUE_B_LONG(prefix, msg):
    print("\33[1;37;44m%s%s NOTI:: %s\33[0m" % (prefix, _TIME(),  str(msg)))
    flush_io()

MSELINE = 80
# cmd border
def _LOG_BORDER(LOG_CLBK, caller, result, cmd, name):
    _LOG_INT("", str(" %s cmd : %s"   % (caller, "*" * MSELINE)))
    LOG_CLBK(" %s cmd : ***  %s  *** %s *** %s ***" % (caller, result, name, cmd))
    _LOG_INT("", str(" %s cmd : %s\n" % (caller, "*" * MSELINE)))

############################################################################################
# API print
############################################################################################

# API time + no colors
def LOG_DBG(msg):
    _LOG_INT("", msg)
def LOG(msg):
    _LOG_INT("", msg)

# API time + short background colours
def LOG_OK(msg):
    _LOG_GREEN_B("", msg)
def LOG_ERR(msg):
    _LOG_RED_B("", msg)
def LOG_WARN(msg):
    _LOG_YELLOW_B("", msg)
def LOG_NOTI(msg):
    _LOG_BLUE_B("", msg)

# API time + full background colours
def LOG_OK_L(msg):
    _LOG_GREEN_B_LONG("", msg)
def LOG_ERR_L(msg):
    _LOG_RED_B_LONG("", msg)
def LOG_WARN_L(msg):
    _LOG_YELLOW_B_LONG("", msg)
def LOG_NOTI_L(msg):
    _LOG_BLUE_B_LONG("", msg)

# API cmd border
def LOG_BORDER_OK(caller, result, cmd, name):
    _LOG_BORDER(LOG_OK_L, caller, result, cmd, name)
def LOG_BORDER_ERR(caller, result, cmd, name):
    _LOG_BORDER(LOG_ERR_L, caller, result, cmd, name)

# API re border
def LOG_BORDER_RE(r, help_str, pattern):
        if len(r) > 0:
            LOG(" -------")
            LOG("  RE OK [%s] pattern '%s' found >> '%s' " % (help_str, pattern, r))
            LOG(" -------\n")
        else:
            LOG(" ------")
            LOG(" RE NOK [%s] pattern '%s' not found >> NOTHING ..." % (help_str, pattern))
            LOG(" ------\n")

###
def LOG_BEND_LOOP(obj, PRINT_METHOD, str_to_print):
    if obj is not None:
        obj.LOG(PRINT_METHOD, str_to_print)
    else:
        PRINT_METHOD(str(str_to_print))

############################################################################################
# TEST
############################################################################################

class TestClog(unittest.TestCase):
    def test_log_print(self):
        log_me = "log me"
        LOG(log_me)
        LOG_OK(log_me)
        LOG_OK_L(log_me)
        LOG_ERR(log_me)
        LOG_ERR_L(log_me)
        LOG_WARN(log_me)
        LOG_WARN_L(log_me)
        LOG_NOTI(log_me)
        LOG_NOTI_L(log_me)

        LOG_BORDER_OK("CALLER", "RES", "CMD", "NAME")
        LOG_BORDER_ERR("CALLER", "RES", "CMD", "NAME")

        r1 = [ ]
        LOG_BORDER_RE(r1, "help_str", "pattern")
        r1.append("one element")
        LOG_BORDER_RE(r1, "help_str", "pattern")

        LOG_BEND_LOOP(None, LOG, "mse LOG_BEND_LOOP")

if __name__ == '__main__':
     unittest.main()
