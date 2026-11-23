"""较大数据量下的性能检查（5 分片 / 4000 条明细）。

用法：python3 scripts/fuzz3.py
"""
import sys, random, time
sys.path.insert(0, "/workspace")
from decimal import Decimal
from rankapp.ranking import Record, ShardSlice, rank, TIE_STRICT, TIE_KEEP
random.seed(3)
t0=time.time()
# big: 5 shards, 400 skus, ~4000 recs
recs=[]
for i in range(4000):
    s=random.randrange(5)
    recs.append(Record(record_id="r%05d"%i, store=random.choice(["A","B"]), sku="sku-%03d"%random.randrange(400), qty=1, amount=Decimal(random.choice([1,2,3,5,8,13,21,34,55,-3,-8])), kind="sale", batch_id="b%d"%s))
shards=[ShardSlice.build("S%d"%s,"s",["b%d"%s],[r for r in recs if r.batch_id=="b%d"%s]) for s in range(5)]
for mode in (TIE_STRICT,TIE_KEEP):
    for n in (1,5,10,25,100):
        t=time.time()
        res=rank(mode,n,recs,shards,["A","B"])
        print(mode,n,"ok",res.verification.ok,"rounds",res.totals["rounds"],"time",round(time.time()-t,3))
print("total",round(time.time()-t0,2))
