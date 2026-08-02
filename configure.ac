AC_PREREQ([2.71])
AC_INIT([easybd], [0.1.0], [])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AM_SILENT_RULES([yes])

AC_PROG_CC
AC_PROG_CXX
AC_PROG_INSTALL
AM_PROG_AR
LT_INIT

CXXFLAGS="$CXXFLAGS -std=c++20 -Wall -Wextra -Werror -fPIC"
CFLAGS="$CFLAGS -std=gnu99 -Wall -Wextra -Werror -fPIC"

AC_ARG_WITH([liburing],
    [AS_HELP_STRING([--without-liburing],
        [build without io_uring backend support (poll/libc backend only)])],
    [with_liburing=$withval], [with_liburing="yes"])

AM_CONDITIONAL([WITH_LIBURING], [test "x$with_liburing" = "xyes"])
AS_IF([test "x$with_liburing" = "xyes"], [
    PKG_CHECK_MODULES([LIBURING], [liburing >= 2.3])
])

AC_ARG_ENABLE([tests],
    [AS_HELP_STRING([--disable-tests], [do not build tests])],
    [enable_tests=$enableval], [enable_tests="yes"])
AM_CONDITIONAL([ENABLE_TESTS], [test "x$enable_tests" = "xyes"])
AS_IF([test "x$enable_tests" = "xyes"], [
    PKG_CHECK_MODULES([GTEST], [gtest gtest_main], [], [
        AC_MSG_WARN([gtest not found via pkg-config, tests may fail to build])
    ])
])

AC_CONFIG_FILES([
    Makefile
    easyio/Makefile
    easyio/src/Makefile
    easyio/tests/Makefile
    server/Makefile
    client/Makefile
    easybd.pc
])

AC_OUTPUT
