import sys; sys.path.insert(0, __import__('os').path.dirname(__file__))
from common import *
rng=np.random.default_rng(7)
df=load_orders(); df=df[df.iso.map(lambda d: spy(d) is not None)]
J=pd.read_pickle(f"{SP}/proxy_intervals.pkl")
first=df.groupby('iso').en.min()
J['early']=J.t < J.iso.map(first)+pd.Timedelta(minutes=60)
def score(x):
    g=x[x.state=='green']; r=x[x.state=='red']
    return pd.Series(dict(ng=len(g),nr=len(r),D=(g.sold.mean() if len(g) else np.nan)-(r.sold.mean() if len(r) else np.nan)))
def ret(o): return o.pl.sum()/o.cost.sum() if o.cost.sum()>0 else np.nan
def spearman(a,b): return pd.Series(a).rank().corr(pd.Series(b).rank())
def perm_p(a,b,n=10000):
    rho=spearman(a,b); ra=pd.Series(a).rank().values; rb=pd.Series(b).rank().values
    sims=np.array([np.corrcoef(ra,rng.permutation(rb))[0,1] for _ in range(n)])
    return rho,(sims<=rho).mean()
def report(name,T,mincut=-0.20):
    rho,p=perm_p(T.D,T.out)
    h1=T[T.iso<'2026-07-01']; h2=T[T.iso>='2026-07-01']
    r1=spearman(h1.D,h1.out); r2=spearman(h2.D,h2.out)
    ok= rho<=mincut and p<0.025 and r1<0 and r2<0
    print(f"\n{name}: n={len(T)} days  rho={rho:+.2f}  one-sided p={p:.3f}  halves: {r1:+.2f} (n={len(h1)}) / {r2:+.2f} (n={len(h2)})  -> {'PASS' if ok else 'FAIL'}")
    T=T.copy(); T['tercile']=pd.qcut(T.D.rank(method='first'),3,labels=['low D','mid D','high D'])
    print(T.groupby('tercile',observed=True).agg(days=('D','size'),D=('D','mean'),ret_on_premium=('out','mean'),dollars=('usd','sum'),blowups=('usd',lambda s:(s<-500).sum())).round(3).to_string())
# ---------- Test 1
S1=J[J.early].groupby('iso').apply(score)
rows=[]
for iso,s in S1.iterrows():
    if s.ng<3 or s.nr<3: continue
    later=df[(df.iso==iso)&(df.en>=first[iso]+pd.Timedelta(minutes=60))]
    if later['Group ID'].nunique()<1: continue
    rows.append(dict(iso=iso,D=s.D,out=ret(later),usd=later.pl.sum()))
T1=pd.DataFrame(rows); report("TEST 1  early-window D -> rest-of-day return on premium",T1)
# ---------- Test 2
S2=J.groupby('iso').apply(score)
days=sorted(df.iso.unique()); nxt={d:days[i+1] for i,d in enumerate(days[:-1])}
rows=[]
for iso,s in S2.iterrows():
    if s.ng<5 or s.nr<5 or iso not in nxt: continue
    o=df[df.iso==nxt[iso]]
    rows.append(dict(iso=iso,nxt=nxt[iso],D=s.D,out=ret(o),usd=o.pl.sum()))
T2=pd.DataFrame(rows); report("TEST 2  today's D -> next trading day return on premium",T2)
# ---------- reference only
rows=[]
for iso,s in S2.iterrows():
    if s.ng<5 or s.nr<5: continue
    o=df[df.iso==iso]; rows.append(dict(iso=iso,D=s.D,out=ret(o),usd=o.pl.sum()))
R=pd.DataFrame(rows); print(f"\nREFERENCE (circular) same-day D vs same-day return: rho={spearman(R.D,R.out):+.2f} n={len(R)}")
el=S2[(S2.ng>=5)&(S2.nr>=5)].D
roll=el.rolling(3).mean().dropna()
rows=[dict(iso=i,D=v,out=ret(df[df.iso==nxt[i]])) for i,v in roll.items() if i in nxt]
R3=pd.DataFrame(rows); print(f"REFERENCE 3-day rolling D -> next day: rho={spearman(R3.D,R3.out):+.2f} n={len(R3)}")
print(f"\npooled D over all {len(J)} intervals: {score(J).D:+.3f}  (sell rate green {J[J.state=='green'].sold.mean():.3f}, red {J[J.state=='red'].sold.mean():.3f})")
