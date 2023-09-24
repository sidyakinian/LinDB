# LinDB

This repo implements a simple distributed database in Go. It will be tested in Maelstrom, which is a framework by Jepsen for building and testing toy distributed systems.

LinDB is a linearizable key-value store. Linearizable basically means that even though it's distributed, it behaves as though it's not – the complexity is abstracted away. This ensures consistency, but comes with a tradeoff for availability – at any time, the majority of the nodes must be operational, or LinDB will become unavailable.

We can use Maelstrom's `lin-kv` workload to test that LinDB indeed fits the requirements of a linearizable KV store as follows:

```
./path_to_maelstrom_executable test -w lin-kv \
	--bin $(GOBIN)/lin-db \
	--time-limit 10 \
	--rate 10 \
	--node-count 2 \
	--concurrency 2n
```

Please make sure that `--node-count` is $\geq$ 1, since otherwise the system is not distributed.
