import pandas as pd, numpy as np, json, glob, os
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
ROOT='/Users/saaketh/projects/rh-trade-exporter'
SP=os.path.dirname(os.path.abspath(__file__))
def load_orders():
    df=pd.read_csv(f'{ROOT}/outputs/spy_trades.csv')
    df['en']=pd.to_datetime(df['Date']+' '+df['Entry Time'].astype(str))
    df['ex']=df['en']+pd.to_timedelta(df['Hold Time (min)'].astype(float),unit='m')
    df['pl']=df['P/L ($)'].astype(float); df['cost']=df['Entry Cost'].abs().astype(float)
    df['px_in']=df['cost']/df['Qty']/100
    df['iso']=df.en.dt.date.astype(str)
    df['exp']=pd.to_datetime(df['Expiry Date']).dt.date.astype(str)
    df['ck']=df['exp']+'|'+df['Type'].str.lower()+'|'+df['Strike'].astype(float).map(lambda x:f"{x:g}")
    df['sign']=np.where(df['Type'].str.lower()=='call',1,-1)
    return df
_spy={}
def spy(iso):
    if iso not in _spy:
        p=f'{ROOT}/outputs/spy_intraday/{iso}.json'
        s=None
        if os.path.exists(p):
            j=json.load(open(p)); b=j.get('bars') or []
            if b:
                idx=pd.to_datetime([x['t'] for x in b],unit='s',utc=True).tz_convert(ET).tz_localize(None)
                s=pd.DataFrame({'o':[x['o'] for x in b],'c':[x['c'] for x in b]},index=idx).sort_index()
        _spy[iso]=s
    return _spy[iso]
def spy_at(iso,t):
    """SPY price at time t: close of the last 5-min bar that has fully ended by t; else open of bar containing t."""
    s=spy(iso)
    if s is None: return np.nan
    done=s[s.index+pd.Timedelta(minutes=5)<=t]
    if len(done): return done.c.iloc[-1]
    cur=s[s.index<=t]
    return cur.o.iloc[-1] if len(cur) else np.nan
def load_options(d=None):
    d=d or f'{SP}/vps_oi'; P={}
    for f in glob.glob(d+'/*.json'):
        j=json.load(open(f))
        for c in j['contracts']:
            if not c.get('bars'): continue
            ck=f"{c['expiry']}|{c['type']}|{float(c['strike']):g}"
            idx=pd.to_datetime([b['t'] for b in c['bars']],unit='s',utc=True).tz_convert(ET).tz_localize(None)
            step=1 if c.get('interval')=='minute' else 5
            P[(j['date'],ck)]=(pd.Series([b['c'] for b in c['bars']],index=idx).sort_index(),step)
    return P
def opt_at(P,iso,ck,t):
    v=P.get((iso,ck))
    if v is None: return np.nan
    s,step=v; done=s[s.index+pd.Timedelta(minutes=step)<=t]
    return done.iloc[-1] if len(done) else np.nan
def intervals(df, state_fn, step=5):
    """One record per order per 5-min interval held. state at interval start; sold = any exit of that order inside the interval."""
    rec=[]
    for gid,g in df.groupby('Group ID'):
        t0=g.en.iloc[0]; tend=g.ex.max()
        exits=list(g.ex)
        t=t0+pd.Timedelta(minutes=step)
        # first decision point: step minutes after entry; interval = (t, t+step]
        while t<tend:
            st=state_fn(g,t)
            if st is not None:
                sold=any(t<e<=t+pd.Timedelta(minutes=step) for e in exits)
                rec.append(dict(gid=gid,iso=g.iso.iloc[0],t=t,state=st,sold=sold))
            t+=pd.Timedelta(minutes=step)
    return pd.DataFrame(rec)
SPY_BAND=0.05  # $ SPY move inside which the order counts as flat
OPT_BAND=0.02  # 2% premium move inside which the order counts as flat
def spy_state(g,t):
    iso=g.iso.iloc[0]; a=spy_at(iso,g.en.iloc[0]); b=spy_at(iso,t)
    if np.isnan(a) or np.isnan(b): return None
    m=(b-a)*g.sign.iloc[0]
    return 'green' if m>SPY_BAND else ('red' if m<-SPY_BAND else 'flat')
