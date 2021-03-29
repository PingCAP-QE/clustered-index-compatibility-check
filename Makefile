GO := CGO_ENABLED=0 go

.PHONY: FORCE build package clean

build: output/bin/clustered-index-compatibility-check

package: output/clustered-index-compatibility-check.tar.gz

clean:
	@rm -rfv output

output/bin/clustered-index-compatibility-check: FORCE
	$(GO) build -o $@ ./

output/clustered-index-compatibility-check.tar.gz: output/bin/clustered-index-compatibility-check
	tar -C output/bin -zcf $@ clustered-index-compatibility-check
