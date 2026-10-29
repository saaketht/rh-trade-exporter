import sys; sys.path.insert(0, __import__('os').path.dirname(__file__))
from common import *
I=pd.read_pickle(f"{SP}/calib_intervals.pkl")
d=I.groupby('iso').apply(lambda x: pd.Series({'o':(x[x.opt=='green'].sold.mean()-x[x.opt=='red'].sold.mean()),'s':(x[x.spy=='green'].sold.mean()-x[x.spy=='red'].sold.mean())})).dropna()
print("per-day D rank corr option vs proxy:", round(d.o.rank().corr(d.s.rank()),2), "n days", len(d))
df=load_orders(); df=df[df.iso.map(lambda d: spy(d) is not None)]
G=df.groupby('Group ID').agg(hold=('Hold Time (min)','max'),iso=('iso','first'))
print("orders held under 5 min:", f"{(G.hold<5).mean():.0%}", " under 10:", f"{(G.hold<10).mean():.0%}", " median hold", G.hold.median())
J=intervals(df,spy_state); J.to_pickle(f"{SP}/proxy_intervals.pkl")
c=J.groupby('iso').state.value_counts().unstack(fill_value=0)
for k in [3,5,8]:
    print(f"days with >= {k} green and >= {k} red intervals: {((c.get('green',0)>=k)&(c.get('red',0)>=k)).sum()} of {df.iso.nunique()}")
# first 60 minutes after the day's first entry
first=df.groupby('iso').en.min()
J['early']=J.apply(lambda r: r.t < first[r.iso]+pd.Timedelta(minutes=60),axis=1)
ce=J[J.early].groupby('iso').state.value_counts().unstack(fill_value=0)
for k in [2,3,5]:
    print(f"early window: days with >= {k} green and >= {k} red: {((ce.get('green',0)>=k)&(ce.get('red',0)>=k)).sum()}")
# days with any early interval and also later orders
later=df[df.en>=df.iso.map(first)+pd.Timedelta(minutes=60)].groupby('iso')['Group ID'].nunique()
print("days with orders entered after the early window:", (later>0).sum())
