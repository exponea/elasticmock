import unittest
import sys
from datetime import datetime

def _TIME():
    #return datetime.now().strftime('%Y/%m/%d %H:%m:%S.%f')[:-3]
    return datetime.now().strftime('%H:%m:%S.%f')[:-3]

def _generate_caller_name():
    return sys._getframe(3).f_code.co_name

def _LOG_INT(prefix, user_string_list):
    #print("%sLOG::%s: %s" % (prefix, generate_caller_name(), ''.join(user_string_list)))
    print("%s%s LOG :: %s" % (prefix, _TIME(), ''.join(user_string_list)))

# print colored short
def _LOG_RED_B(prefix, user_string_list):
    print("\33[1;37;41m%s%s ERROR::\33[0m %s" % (prefix, _TIME(), ''.join(user_string_list)))

def _LOG_GREEN_B(prefix, user_string_list):
    print("\33[1;37;42m%s%s LOG ::\33[0m %s" % (prefix, _TIME(), ''.join(user_string_list)))

def _LOG_YELLOW_B(prefix, user_string_list):
    print("\33[1;37;43m%s%s WARN ::\33[0m %s" % (prefix, _TIME(), ''.join(user_string_list)))

def _LOG_BLUE_B(prefix, user_string_list):
    print("\33[1;37;44m%s%s NOTE ::\33[0m %s" % (prefix, _TIME(), ''.join(user_string_list)))

# print colored long
def _LOG_RED_B_LONG(prefix, user_string_list):
    print("\33[1;37;41m%s%s ERROR:: %s\33[0m" % (prefix, _TIME(), ''.join(user_string_list)))

def _LOG_GREEN_B_LONG(prefix, user_string_list):
    print("\33[1;37;42m%s%s LOG :: %s\33[0m" % (prefix, _TIME(), ''.join(user_string_list)))

def _LOG_YELLOW_B_LONG(prefix, user_string_list):
    print("\33[1;37;43m%s%s WARN :: %s\33[0m" % (prefix, _TIME(), ''.join(user_string_list)))

def _LOG_BLUE_B_LONG(prefix, user_string_list):
    print("\33[1;37;44m%s%s NOTE :: %s\33[0m" % (prefix, _TIME(), ''.join(user_string_list)))

MSELINE = 80
# cmd border
def _LOG_BORDER(LOG_CLBK, caller, result, cmd, name):
    _LOG_INT("", str(" %s cmd : %s"   % (caller, "*" * MSELINE)))
    LOG_CLBK(" %s cmd : ***  %s  *** %s *** %s ***" % (caller, result, name, cmd))
    _LOG_INT("", str(" %s cmd : %s\n" % (caller, "*" * MSELINE)))

############################################################################################
# API print
############################################################################################

# API no colors
def LOG_DBG2(user_string_list):
    print(user_string_list)
def TEST_MARK():
    print("\n==========================================================\n")

# API time + no colors
def LOG_DBG(user_string_list):
    _LOG_INT("", user_string_list)
def LOG(user_string_list):
    _LOG_INT("", user_string_list)

# API time + short background colours
def LOG_OK(user_string_list):
    _LOG_GREEN_B("", user_string_list)
def LOG_ERR(user_string_list):
    _LOG_RED_B("", user_string_list)
def LOG_WARN(user_string_list):
    _LOG_YELLOW_B("", user_string_list)
def LOG_NOTI(user_string_list):
    _LOG_BLUE_B("", user_string_list)

# API time + full background colours
def LOG_OK_L(user_string_list):
    _LOG_GREEN_B_LONG("", user_string_list)
def LOG_ERR_L(user_string_list):
    _LOG_RED_B_LONG("", user_string_list)
def LOG_WARN_L(user_string_list):
    _LOG_YELLOW_B_LONG("", user_string_list)
def LOG_NOTI_L(user_string_list):
    _LOG_BLUE_B_LONG("", user_string_list)

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

        LOG_BEND_LOOP(None, LOG, "miso LOG_BEND_LOOP")

if __name__ == '__main__':
     unittest.main()
