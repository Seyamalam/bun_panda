"""Regenerate focused value/metadata contracts using pandas 3.0.5.

Transport maps missing scalars to null, NumPy scalars to JSON primitives,
Series to values, and DataFrames to columns/index/data. Native pandas return
classes and dtypes are intentionally outside these focused regression tests.
The separate strict conformance command checks those differences.
"""
import json
import math
import warnings
from pathlib import Path
import numpy as np
import pandas as pd

assert pd.__version__ == '3.0.5', pd.__version__

def encode(x):
    if x is None or x is pd.NA or x is pd.NaT: return None
    if isinstance(x, np.generic): x = x.item()
    if isinstance(x, float):
        if math.isnan(x): return None
        if math.isinf(x): return {'$number': 'Infinity' if x > 0 else '-Infinity'}
        if x == 0 and math.copysign(1, x) < 0: return {'$number': '-0'}
    if isinstance(x, (pd.Series, pd.Index, np.ndarray)): return encode(x.tolist())
    if isinstance(x, (list, tuple)): return [encode(v) for v in x]
    if isinstance(x, dict): return {k: encode(v) for k,v in x.items()}
    return x

def cat(c): return {'categories': encode(c.categories), 'codes': encode(c.codes), 'ordered': c.ordered}

cases=[]
def add(api, values, args, fn, dtype=None):
    case={'id': f'{api}/{len(cases)}', 'api': api, 'values': values, 'args': args}
    if dtype is not None: case['dtype']=dtype
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            case['expected']=encode(fn())
        case['status']='ok'
    except (ValueError, TypeError) as e:
        case['status']='error'
        case['errorType']=type(e).__name__
        case['message']=str(e)
    cases.append(case)

numeric=[[], [1], [1,1,1,1], [1,1,1,5], [1,2,3,4], [None,None], [3,1,None,2], [-3,-1,0,1,3], [2.5,3.5,-2.5,-3.5,-0.5,0.5]]
for values in numeric:
    s=pd.Series(values, dtype='float64')
    for op in ['sum','prod','skew','kurt','count','is_unique','is_monotonic_increasing','is_monotonic_decreasing','hasnans']:
        add('Series.'+op, values, {}, lambda op=op: getattr(s,op)() if callable(getattr(s,op)) else getattr(s,op), dtype='float64')
    for decimals in [-1,0,1]:
        add('Series.round', values, {'decimals':decimals}, lambda: s.round(decimals), dtype='float64')
for values in [['b','a'], ['Z','a'], ['a','b','b'], ['b',None,'a'], [True,1,False,0,'1'], [], [10,2,1,None,10], [1,'1',None,1]]:
    s=pd.Series(values)
    add('Series.factorize',values,{},lambda:s.factorize())
    add('factorize',values,{},lambda:pd.factorize(np.array(values,dtype=object)))
    add('Categorical',values,{},lambda:cat(pd.Categorical(values)))
    for op in ['is_unique','is_monotonic_increasing','is_monotonic_decreasing']:
        add('Series.'+op,values,{},lambda op=op:getattr(s,op))
for categories in [['b','a'], ['a','b','a'], ['a',None], [1,'1'], [True,1], []]:
    add('Categorical',['b','a',None],{'categories':categories},lambda:cat(pd.Categorical(['b','a',None],categories=categories)))
for ordered in [False,True]:
    add('CategoricalDtype.equals',['a','b'],{'other':['b','a'],'ordered':ordered},lambda:pd.CategoricalDtype(['a','b'],ordered)==pd.CategoricalDtype(['b','a'],ordered))
for values in [['red','blue',None], [10,2,None,10], [], [None,None], [True,False,True]]:
    for drop_first in [False,True]:
        for dummy_na in [False,True]:
            df=pd.DataFrame({'id':range(len(values)),'color':pd.Series(values,dtype=object)},index=list(range(len(values))))
            df.index=[f'row-{i}' for i in range(len(values))]
            def result():
                d=pd.get_dummies(df,columns=['color'],drop_first=drop_first,dummy_na=dummy_na)
                return {'columns':d.columns.tolist(),'index':d.index.tolist(),'data':d.values.tolist()}
            add('get_dummies',values,{'drop_first':drop_first,'dummy_na':dummy_na},result)
# Series order, labels and names are part of these observations.
def series_result(value):
    if isinstance(value,pd.Series): return {'values':encode(value),'index':encode(value.index),'name':value.name}
    return encode(value)
for values in numeric:
    s=pd.Series(values,dtype='float64',index=[f'label-{i}' for i in range(len(values))],name='v')
    operations=[(op,[]) for op in ['cumsum','cumprod','cummin','cummax','ffill','bfill','dropna','isna','notna','unique','nunique','drop_duplicates','duplicated']]
    operations += [(op,[n]) for op in ['head','tail'] for n in [-2,0,2]]
    operations += [(op,[n]) for op in ['shift','diff'] for n in [-1,0,1]]
    operations += [('fillna',[0]),('round',[0]),('clip',[-1,2]),('between',[-1,2]),('repeat',[2])]
    for op,positional in operations:
        add('Series.'+op,values,{'positional':positional,'metadata':True},lambda op=op,positional=positional:series_result(getattr(s,op)(*positional)), dtype='float64')
strings=[['HELLO','hELLo','hello world',''],['  space  ','-12','+12','1.2',None],['a.b','acb','ba','',None],['😀é','éCOLE','ßeta',None],['a',42,True,None]]
for values in strings:
    s=pd.Series(values,dtype=object)
    operations=[(op,[]) for op in ['upper','lower','capitalize','title','strip','lstrip','rstrip','len']]
    operations += [('zfill',[5]),('slice',[1,3]),('get',[8]),('contains',['a.b']),('match',['a']),('count',['.'])]
    for op,positional in operations:
        add('Series.str.'+op,values,{'positional':positional},lambda op=op,positional=positional:getattr(s.str,op)(*positional), dtype='object')
Path('test/fixtures/pandas-value-contracts.json').write_text(json.dumps({'pandasVersion':pd.__version__,'transport':'missing scalars -> null; Series -> list; native dtypes/classes excluded','cases':cases},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(cases)} pandas value/metadata contracts')
