DISTRIBUTION := $(shell lsb_release -cs | tail -n 1)

build:
	python3 setup.py build
sdist:
	python3 setup.py sdist --formats=gztar
install:
	:build
	python3 setup.py install
deb:
	DEB_BUILD=1 python3 setup.py \
		--command-packages=stdeb.command sdist_dsc --suite  $(DISTRIBUTION) \
		--no-python2-scripts=True -c 10 --with-dh-systemd bdist_deb
clean:
	python3 setup.py clean --all
	rm -rf dist/ dist_deb/ deb_dist/ build/ *.egg-info/
purge-deb:
	apt purge clickhouse-backup
install-deb:
	apt install ./deb_dist/clickhouse-backup_*_all.deb