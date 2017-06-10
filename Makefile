.PHONY: test docker push

IMAGE            ?= hjacobs/kube-job-cleaner
VERSION          ?= $(shell git describe --tags --always --dirty)
TAG              ?= $(VERSION)
GITHEAD          = $(shell git rev-parse --short HEAD)
GITURL           = $(shell git config --get remote.origin.url)
GITSTATUS        = $(shell git status --porcelain || echo "no changes")

default: docker

test:
	tox

docker: scm-source.json
	docker build -t "$(IMAGE):$(TAG)" .
	@echo 'Docker image $(IMAGE):$(TAG) can now be used.'

push: docker
	docker push "$(IMAGE):$(TAG)"

scm-source.json: .git
	@echo '{"url": "git:$(GITURL)", "revision": "$(GITHEAD)", "author": "$(USER)", "status": "$(GITSTATUS)"}' > scm-source.json

