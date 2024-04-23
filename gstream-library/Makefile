PACKAGE_VERSION=0.1.0

BACKGROUND_SERVICE_DIST=../background-service/service/dist
WORKER_SERVICE_DIST=../worker-service/service/dist


create-build:
	poetry build


add-dists:
	if [ -d $(BACKGROUND_SERVICE_DIST) ]; then rm -Rf $(BACKGROUND_SERVICE_DIST); fi
	mkdir -p $(BACKGROUND_SERVICE_DIST)
	cp -r ./dist/gstream-$(PACKAGE_VERSION)-py3-none-any.whl $(BACKGROUND_SERVICE_DIST)

	if [ -d $(WORKER_SERVICE_DIST) ]; then rm -Rf $(WORKER_SERVICE_DIST); fi
	mkdir -p $(WORKER_SERVICE_DIST)
	cp -r ./dist/gstream-$(PACKAGE_VERSION)-py3-none-any.whl $(WORKER_SERVICE_DIST)
