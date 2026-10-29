import sys; sys.path.insert(0, __import__('os').path.dirname(__file__))
import common
from common import *
df=load_orders(); df=df[df.iso.map(lambda d: spy(d) is not None)]
first=df.groupby('iso').en.min()
def sp(a,b): return pd.Series(a).rank().corr(pd.Series(b).rank())
def run(J,win,k):
    E=J[J.t<J.iso.map(first)+pd.Timedelta(minutes=win)]
    out=[]
    for iso,x in E.groupby('iso'):
        g=x[x.state=='green'];r=x[x.state=='red']
        if len(g)<k or len(r)<k: continue
        later=df[(df.iso==iso)&(df.en>=first[iso]+pd.Timedelta(minutes=win))]
        if not len(later): continue
        out.append((iso,g.sold.mean()-r.sold.mean(),later.pl.sum()/later.cost.sum(),later.pl.sum()))
    T=pd.DataFrame(out,columns=['iso','D','out','usd']); return T
J=pd.read_pickle(f"{SP}/proxy_intervals.pkl")
print("band $0.05 (pre-registered state)")
for win in [45,60,90]:
    for k in [2,3,4]:
        T=run(J,win,k); print(f"  window {win}m, >= {k}/{k}: n={len(T):3d} rho={sp(T.D,T.out):+.2f}  dollars-rho={sp(T.D,T.usd):+.2f}")
T=run(J,60,3)
loo=[sp(T.drop(i).D,T.drop(i).out) for i in T.index]
print(f"leave-one-day-out rho range (60m, >=3): {min(loo):+.2f} to {max(loo):+.2f}")
print("most influential days:", T.assign(infl=[sp(T.D,T.out)-l for l in loo]).sort_values('infl').head(3)[['iso','D','usd']].to_dict('records'))
common.SPY_BAND=0.10
J2=intervals(df,common.spy_state)
print("band $0.10")
for win in [45,60,90]:
    T=run(J2,win,3); print(f"  window {win}m, >= 3/3: n={len(T):3d} rho={sp(T.D,T.out):+.2f}")
print("\n--- confound check: is D just a red first hour? (pre-registered settings) ---")
T=run(J,60,3)
early_pl=[]; early_ret=[]; red_share=[]
for iso in T.iso:
    w=df[(df.iso==iso)&(df.en<first[iso]+pd.Timedelta(minutes=60))]
    early_pl.append(w.pl.sum()); early_ret.append(w.pl.sum()/w.cost.sum())
    x=J[(J.iso==iso)&(J.t<first[iso]+pd.Timedelta(minutes=60))]; red_share.append((x.state=='red').mean())
T['early_ret']=early_ret; T['red_share']=red_share
R=T[['D','out','early_ret','red_share']].rank()
c=R.corr()
print(c.round(2))
def partial(a,b,z):
    r=c; return (r.loc[a,b]-r.loc[a,z]*r.loc[b,z])/np.sqrt((1-r.loc[a,z]**2)*(1-r.loc[b,z]**2))
print(f"D vs rest-of-day, controlling for early return: {partial('D','out','early_ret'):+.2f}")
print(f"D vs rest-of-day, controlling for share of early time spent red: {partial('D','out','red_share'):+.2f}")
