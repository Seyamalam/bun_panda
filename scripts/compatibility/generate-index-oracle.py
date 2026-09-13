"""Pinned scalar-index contracts; preserve numeric/string labels and NaN."""
import json
import math
from pathlib import Path
import pandas as pd
assert pd.__version__ == '3.0.5'

def encode(value):
    return {'$number':'NaN'} if isinstance(value,float) and math.isnan(value) else value

indexes=[[],[0],[1,2],['1','2'],[1,'1'],[float('nan'),1],[1,1],['a','a'],[float('nan'),float('nan')]]
cases=[]
for labels in indexes:
    for target in indexes:
        cases.append({'api':'Index.equals','labels':[encode(v) for v in labels],'target':[encode(v) for v in target], 'expected':bool(pd.Index(labels).equals(pd.Index(target)))})
        source=pd.Series(range(len(labels)),index=labels,dtype='int64',name='v')
        entry={'api':'reindex','labels':[encode(v) for v in labels],'target':[encode(v) for v in target]}
        try:
            result=source.reindex(target)
            entry['expected']={'dtype':str(result.dtype),'values':[encode(v) for v in result.tolist()],'index':[encode(v) for v in result.index.tolist()]}
        except Exception as e: entry['error']={'type':type(e).__name__,'message':str(e)}
        cases.append(entry)
Path('test/fixtures/pandas-index-contracts.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':cases},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(cases)} scalar index contracts')
