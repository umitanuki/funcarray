MODULE_big = funcarray
DATA_built = funcarray.sql
DATA = uninstall_funcarray.sql
OBJS = funcarray.o
REGRESS = funcarray

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/map
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/contrib/contrib-global.mk
endif

funcarray.o: funcarray.sql.in
