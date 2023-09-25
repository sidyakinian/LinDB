make test-single-node:
	go install .
	@echo Installed
	./maelstrom/maelstrom test -w lin-kv \
	--bin $(GOBIN)/lin-db \
	--time-limit 10 \
	--rate 10 \
	--node-count 1 \
	--concurrency 2n

make test-multi-node:
	go install .
	@echo Installed
	./maelstrom/maelstrom test -w lin-kv \
	--bin $(GOBIN)/lin-db \
	--time-limit 10 \
	--rate 10 \
	--node-count 2 \
	--concurrency 2n