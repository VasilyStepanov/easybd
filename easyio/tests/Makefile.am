AM_CPPFLAGS = -I$(top_srcdir)/easyio/include $(GTEST_CFLAGS)

check_PROGRAMS = test_all
TESTS = test_all

test_all_SOURCES = \
	test_basics.cpp
test_all_LDADD = \
	$(top_builddir)/easyio/src/libeasyio.la \
	$(GTEST_LIBS)

test:
	$(MAKE) check TESTS=test_all
	./test_all
