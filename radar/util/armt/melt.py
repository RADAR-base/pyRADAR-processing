#!/usr/bin/env python3
def melt_df(df):
    id_vars = [c for c in df if 'value.answers' not in c]
    melt = df.melt(id_vars=id_vars)
    melt['variable'] = [v.split('.')[-1] for v in melt['variable']]
    return melt

