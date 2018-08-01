#!/usr/bin/env python3
def biovotion_sort_upsample(df, time_idx='value.time'):
    df = df.sort_index(ascending=False).sort_values(by=time_idx, kind='mergesort')
    lens = df.groupby('value.time').apply(len).values
    deltas = np.array([i/j for j in lens for i in range(j)])
    df[time_idx] = (df[time_idx].astype(int) +
                    deltas.dot(10**9).astype(int)).astype(np.datetime64)
    return df
