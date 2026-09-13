"""Pinned dtype contracts. Preserve dtype names, NA and NaN separately."""
import json
import math
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
assert pd.__version__ == '3.0.5'

def encode(x):
    if x is pd.NA: return {'$missing':'NA'}
    if x is None: return None
    if isinstance(x,np.generic): x=x.item()
    if isinstance(x,float):
        if math.isnan(x): return {'$missing':'NaN'}
        if math.isinf(x): return {'$number':'Infinity' if x>0 else '-Infinity'}
        if x==0 and math.copysign(1,x)<0: return {'$number':'-0'}
    return x

cases=[]
dtypes=['object','bool','boolean','string','float32','float64','Float32','Float64']+[f'{prefix}{bits}' for prefix in ['int','uint','Int','UInt'] for bits in [8,16,32,64]]
inputs=[[],[None,None],[1,None],[True,None],['1','2'],[1.2,2.8],[0,1],[-1,2],['true','false'],['bad',''],[0.1,-0.0]]
for mode in ['constructor','astype']:
    for dtype in dtypes:
        for values in inputs:
            entry={'mode':mode,'dtype':dtype,'values':values}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    s=pd.Series(values,dtype=dtype) if mode=='constructor' else pd.Series(values,dtype=object).astype(dtype)
                entry['expected']={'dtype':str(s.dtype),'values':[encode(x) for x in s.tolist()]}
            except Exception as e:
                entry['error']=type(e).__name__
            cases.append(entry)
Path('test/fixtures/pandas-dtype-contracts.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':cases},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(cases)} dtype contracts')
