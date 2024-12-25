## independentBinLogMonitor

执行Flink任务过程中，Binlog监听分配独立的Slot计算资源不会与下游计算算子混合在一起。

如开启，带来的好处是运算时资源各自独立不会相互相互影响，弊端是，上游算子与下游算子独立在两个Solt中需要额外的网络传输开销

## startupOptions

* `Initial` : Takes a snapshot of structure and data of captured tables; useful if topics should be populated with a complete representation of the data from the captured tables.
* `Snapshot`: Takes a snapshot of structure and data like initial but instead does not transition into streaming changes once the snapshot has completed.
* `LatestOffset` (default): Takes a snapshot of the structure of captured tables only; useful if only changes happening from now onwards should be propagated to topics.