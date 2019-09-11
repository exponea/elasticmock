import unittest
import sys
from datetime import datetime

__all__ = ["Log"]

class LogInternal:
    @staticmethod
    def _time():
        #return datetime.now().strftime('%Y/%m/%d %H:%m:%S.%f')[:-3]
        return datetime.now().strftime('%H:%m:%S.%f')[:-3]

    @staticmethod
    def _generate_caller_name(level):
        return sys._getframe(level).f_code.co_name

    @staticmethod
    def _flush_io():
        sys.stdout.flush()

    @staticmethod
    def _log_int(prefix, msg):
        #print("%sLOG::%s: %s" % (prefix, generate_caller_name(), msg))
        print("%s%s LOG :: %s" % (prefix, LogInternal._time(), str(msg)))
        LogInternal._flush_io()

    # print colored short
    @staticmethod
    def _log_red_background(prefix, msg):
        print("\33[1;37;41m%s%s ERR ::\33[0m %s" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_green_background(prefix, msg):
        print("\33[1;37;42m%s%s LOG ::\33[0m %s" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_yellow_background(prefix, msg):
        print("\33[1;37;43m%s%s WARN::\33[0m %s" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_blue_background(prefix, msg):
        print("\33[1;37;44m%s%s NOTI::\33[0m %s" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    # print colored long
    @staticmethod
    def _log_red_background_long(prefix, msg):
        print("\33[1;37;41m%s%s ERR :: %s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_green_background_long(prefix, msg):
        print("\33[1;37;42m%s%s LOG :: %s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_yellow_background_long(prefix, msg):
        print("\33[1;37;43m%s%s WARN:: %s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_blue_background_long(prefix, msg):
        print("\33[1;37;44m%s%s NOTI:: %s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()


    # print colored msg
    @staticmethod
    def _log_red_background_msg(prefix, msg):
        print("%s%s ERR :: \33[1;37;41m%s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_green_background_msg(prefix, msg):
        print("%s%s LOG :: \33[1;37;42m%s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_yellow_background_msg(prefix, msg):
        print("%s%s WARN:: \33[1;37;43m%s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    @staticmethod
    def _log_blue_background_msg(prefix, msg):
        print("%s%s NOTI:: \33[1;37;44m%s\33[0m" % (prefix, LogInternal._time(),  str(msg)))
        LogInternal._flush_io()

    # cmd border
    MSELINE = 80
    @staticmethod
    def _log_border(log_clbk, caller, result, cmd, name):
        LogInternal._log_int("", str(" %s cmd : %s"   % (caller, "*" * LogInternal.MSELINE)))
        log_clbk(" %s cmd : ***  %s  *** %s *** %s ***" % (caller, result, name, cmd))
        LogInternal._log_int("", str(" %s cmd : %s\n" % (caller, "*" * LogInternal.MSELINE)))

############################################################################################
# API print
############################################################################################

class Log(LogInternal):
    # API time + no colors
    @staticmethod
    def log(msg):
        LogInternal._log_int("", msg)

    # API time + short background colours
    @staticmethod
    def ok(msg):
        LogInternal._log_green_background("", msg)
    @staticmethod
    def err(msg):
        LogInternal._log_red_background("", msg)
    @staticmethod
    def warn(msg):
        LogInternal._log_yellow_background("", msg)
    @staticmethod
    def notice(msg):
        LogInternal._log_blue_background("", msg)

    # API time + full background colours
    @staticmethod
    def ok2(msg):
        LogInternal._log_green_background_long("", msg)
    @staticmethod
    def err2(msg):
        LogInternal._log_red_background_long("", msg)
    @staticmethod
    def warn2(msg):
        LogInternal._log_yellow_background_long("", msg)
    @staticmethod
    def notice2(msg):
        LogInternal._log_blue_background_long("", msg)

    # API time + msg background colours
    @staticmethod
    def ok3(msg):
        LogInternal._log_green_background_msg("", msg)
    @staticmethod
    def err3(msg):
        LogInternal._log_red_background_msg("", msg)
    @staticmethod
    def warn3(msg):
        LogInternal._log_yellow_background_msg("", msg)
    @staticmethod
    def notice3(msg):
        LogInternal._log_blue_background_msg("", msg)


    # API cmd border
    @staticmethod
    def border_ok(caller, result, cmd, name):
        LogInternal._log_border(Log.ok2, caller, result, cmd, name)
    @staticmethod
    def border_err(caller, result, cmd, name):
        LogInternal._log_border(Log.err2, caller, result, cmd, name)

    # API re border
    @staticmethod
    def border_re(r, help_str, pattern):
        if len(r) > 0:
            Log.log(" -------")
            Log.log("  RE OK [%s] pattern '%s' found >> '%s' " % (help_str, pattern, r))
            Log.log(" -------\n")
        else:
            Log.log(" ------")
            Log.log(" RE NOK [%s] pattern '%s' not found >> NOTHING ..." % (help_str, pattern))
            Log.log(" ------\n")

    @staticmethod
    def tests_START():
            Log.log("-" * 80)
            Log.log(f"{LogInternal._generate_caller_name(2)} - START")
            Log.log("-" * 80)

    def tests_STOP():
            Log.log("-" * 80)
            Log.log(f"{LogInternal._generate_caller_name(2)} - STOP")
            Log.log("-" * 80)


############################################################################################
# TEST
############################################################################################

class TestClog(unittest.TestCase):
    def test_log_print(self):
        log_me = "log me"
        Log.log(log_me)
        Log.ok(log_me)
        Log.ok2(log_me)
        Log.ok3(log_me)
        Log.err(log_me)
        Log.err2(log_me)
        Log.err3(log_me)
        Log.warn(log_me)
        Log.warn2(log_me)
        Log.warn3(log_me)
        Log.notice(log_me)
        Log.notice2(log_me)
        Log.notice3(log_me)

        Log.border_ok("CALLER", "RES", "CMD", "NAME")
        Log.border_err("CALLER", "RES", "CMD", "NAME")

        r1 = []
        Log.border_re(r1, "help_str", "pattern")
        r1.append("one element")
        Log.border_re(r1, "help_str", "pattern")

        Log.tests_START()
        Log.tests_STOP()

if __name__ == '__main__':
     unittest.main()
