import sys; sys.path.insert(0, __import__('os').path.dirname(__file__))
from common import *
df=load_orders(); P=load_options()
days=sorted({k[0] for k in P})
df=df[df.iso.isin(days)]
df=df[df.iso.map(lambda d: spy(d) is not None)]
df=df[df.apply(lambda r:(r.iso,r.ck) in P,axis=1)]
print("days:",sorted(df.iso.unique()),"orders:",df['Group ID'].nunique())
def opt_state(g,t):
    a=g.px_in.iloc[0]; b=opt_at(P,g.iso.iloc[0],g.ck.iloc[0],t)
    if np.isnan(b): return None
    m=(b-a)/a
    return 'green' if m>OPT_BAND else ('red' if m<-OPT_BAND else 'flat')
def both(g,t):
    s=spy_state(g,t); o=opt_state(g,t)
    return None if s is None or o is None else (s,o)
I=intervals(df,both)
I['spy']=I.state.str[0]; I['opt']=I.state.str[1]
ct=pd.crosstab(I.opt,I.spy,margins=True); print("\nrows = option state (truth), cols = SPY proxy"); print(ct)
gr=I[I.opt.isin(['green','red'])&I.spy.isin(['green','red'])]
agree=(gr.opt==gr.spy).mean()
po=agree; pe=sum((gr.opt==k).mean()*(gr.spy==k).mean() for k in ['green','red']); kappa=(po-pe)/(1-pe)
print(f"\nagreement on intervals where both are green/red: {agree:.0%}  (n={len(gr)}), Cohen kappa {kappa:.2f}")
print("when option is red, proxy says:", I[I.opt=='red'].spy.value_counts(normalize=True).round(2).to_dict())
print("when option is green, proxy says:", I[I.opt=='green'].spy.value_counts(normalize=True).round(2).to_dict())
def D(x,col):
    g=x[x[col]=='green']; r=x[x[col]=='red']
    return pd.Series(dict(n_g=len(g),n_r=len(r),sell_g=g.sold.mean() if len(g) else np.nan,sell_r=r.sold.mean() if len(r) else np.nan))
print("\nsell rate per 5-min interval, pooled:")
print(pd.DataFrame({'option':D(I,'opt'),'spy_proxy':D(I,'spy')}).round(3))
d=pd.concat({c:I.groupby('iso').apply(lambda x:D(x,c)) for c in ['opt','spy']},axis=1)
d[('opt','D')]=d[('opt','sell_g')]-d[('opt','sell_r')]; d[('spy','D')]=d[('spy','sell_g')]-d[('spy','sell_r')]
print("\nper-day D (green sell rate minus red sell rate):"); print(d[[('opt','D'),('spy','D'),('opt','n_g'),('opt','n_r'),('spy','n_g'),('spy','n_r')]].round(3))
print("rank corr of per-day D, option vs proxy:", round(d[('opt','D')].corr(d[('spy','D')],method='spearman'),2))
I.to_pickle(f"{SP}/calib_intervals.pkl")
