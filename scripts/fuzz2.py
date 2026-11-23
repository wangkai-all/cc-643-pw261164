"""随机明细 × 全量分组排序的交叉验证：检查补取结果与停止条件。

用法：python3 scripts/fuzz2.py [轮数]
"""
import sys, random
sys.path.insert(0, "/workspace")
from decimal import Decimal
from rankapp.ranking import Record, ShardSlice, rank, TIE_STRICT, TIE_KEEP, aggregate, strict_entries, keep_tie_entries, ScoreLine

random.seed(11)

def reference(recs, n, mode, store="A"):
    agg=aggregate(recs,[store])
    lines=[ScoreLine(store=store, sku=k[1], net=v[0], source="t", count=v[1]) for k,v in agg.items() if k[0]==store]
    if mode==TIE_STRICT:
        return [(e.sku, e.net) for e in strict_entries(lines,n)]
    return [(e.sku,e.net) for e in keep_tie_entries(lines,n)[0]]

fails=0
TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
for trial in range(TRIALS):
    ns=random.randint(0,4); nsku=random.randint(1,14); n=random.randint(1,6)
    stores=random.sample(["A","B","C"], random.randint(1,3))
    recs=[]; rid=0
    for s in range(ns):
        for _ in range(random.randint(0, 12)):
            sku = "sku-%02d" % random.randrange(nsku)
            store=random.choice(stores)
            amt = random.choice([5,10,10,20,20,30,40,-5,0,7,13])
            rid+=1
            recs.append(Record(record_id="r%03d"%rid, store=store, sku=sku, qty=1, amount=Decimal(amt), kind="sale", batch_id="b%d"%s))
    shards=[]
    for s in range(ns):
        rs=[r for r in recs if r.batch_id=="b%d"%s]
        shards.append(ShardSlice.build("S%d"%s,"s",["b%d"%s],rs))
    for mode in (TIE_STRICT, TIE_KEEP):
        res=rank(mode,n,recs,shards,stores)
        if not res.verification.ok:
            fails+=1; print("VERIFY FAIL",trial,mode,res.verification.mismatches); break
        for st in stores:
            got=[(e.sku,e.net) for e in res.entries_by_store[st]]
            exp=reference(recs,n,mode,st)
            if got!=exp:
                fails+=1
                print("FAIL",trial,mode,"store",st,"n",n,"ns",ns)
                print(" got",got); print(" exp",exp)
                print(" recs",sorted([(r.record_id,r.store,r.sku,str(r.amount),r.batch_id) for r in recs]))
                for rr in res.reports[st].rounds: print("  ",rr)
                break
    if fails>3: break
print("fails",fails)
