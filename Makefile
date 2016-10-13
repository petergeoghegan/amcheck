short_ver = 0.2
long_ver = $(shell (git describe --tags --long '--match=v*' 2>/dev/null || echo $(short_ver)-0-unknown) | cut -c2-)

MODULE_big = amcheck
OBJS       = amcheck.o $(WIN32RES)

EXTENSION  = amcheck
DATA       = amcheck--0.2.sql
PGFILEDESC = "amcheck - verify the logical consistency of indexes"
DOCS       = README.md
REGRESS    = install_amcheck extern_sort_bytea \
	extern_sort_collations extern_sort_numeric

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

DEBUILD_ROOT = /tmp/amcheck

deb:
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	rsync -Ca --exclude=build/* ./ $(DEBUILD_ROOT)/
	cd $(DEBUILD_ROOT) && make -f debian/rules orig
	cd $(DEBUILD_ROOT) && debuild -us -uc -sa
	cp -a /tmp/amcheck_* /tmp/postgresql-9.* build/
