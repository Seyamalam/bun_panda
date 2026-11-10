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
inputs=[[],[None,None],[1,None],[True,None],['1','2'],[1.2,2.8],[0,1],[-1,2],['true','false'],['bad',''],[0.1,-0.0],[pd.NA,1],[np.nan,1],[np.nan,np.nan],[pd.NA,pd.NA],['a',pd.NA],[None,np.nan],[np.inf,-np.inf]]
for mode in ['constructor','astype']:
    for dtype in dtypes:
        for values in inputs:
            entry={'mode':mode,'dtype':dtype,'values':[encode(x) for x in values]}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    s=pd.Series(values,dtype=dtype) if mode=='constructor' else pd.Series(values,dtype=object).astype(dtype)
                entry['expected']={'dtype':str(s.dtype),'values':[encode(x) for x in s.tolist()]}
            except Exception as e:
                entry['error']=type(e).__name__
            cases.append(entry)
for mode in ['infer_objects','convert_dtypes']:
    for dtype in ['object','float32','float64','Float32','int8','uint8','Int8','bool','boolean','string']:
        for values in inputs:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    source=pd.Series(values,dtype=dtype)
            except Exception:
                continue
            entry={'mode':mode,'dtype':dtype,'values':[encode(x) for x in values]}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result=getattr(source,mode)()
                entry['expected']={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()]}
            except Exception as e:
                entry['error']=type(e).__name__
            cases.append(entry)
for dtype in ['Int16','UInt8','Float32','float64','boolean','string','object']:
    for values in [[1,None,1],[None,None,None],[]]:
        source=pd.Series(values,dtype=dtype,index=[f'row-{i}' for i in range(len(values))],name='v')
        operations=[('head',[-1]),('head',[0]),('tail',[-1]),('tail',[0]),('copy',[]),('dropna',[]),('drop_duplicates',[]),('sort_values',[]),('sort_index',[]),('repeat',[2]),('rename',['renamed']),('set_axis',[list(range(len(values)))])]
        operations.append(('take',[[0,-1] if len(values) else []]))
        for operation,args in operations:
            result=getattr(source,operation)(*args)
            cases.append({'mode':'selection','dtype':dtype,'values':[encode(x) for x in values],'operation':operation,'args':args,
                'expected':{'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist(),'name':result.name}})
for dtype in ['int16','Int16','float32','Float32','bool','boolean','string','str','object']:
    for values in [[1,2,1],[1,None,1],[None,None,None],[]]:
        try:
            source=pd.Series(values,dtype=dtype,index=[f'row-{i}' for i in range(len(values))],name='v')
        except Exception:
            continue
        mask=[i%2==0 for i in range(len(values))]
        operations=[('shift',[p]) for p in [-1,0,1]]+[('ffill',[]),('bfill',[]),('reindex',[source.index.tolist()+['new']])]
        operations += [('fillna',[v]) for v in [0,1.5,'x',True]]
        operations += [('where',[mask]),('mask',[mask]),('where',[mask,'x'])]
        for operation,args in operations:
            entry={'mode':'replacement','dtype':dtype,'values':[encode(x) for x in values],'operation':operation,'args':args}
            try:
                result=getattr(source,operation)(*args)
                entry['expected']={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist(),'name':result.name}
            except Exception as e:
                entry['error']=type(e).__name__
            cases.append(entry)
for values in inputs+[[1,True],[1,'1'],['001','false',None]]:
    source=pd.Series(values)
    cases.append({'mode':'inferred_constructor','dtype':str(source.dtype),'values':[encode(x) for x in values],
        'expected':{'dtype':str(source.dtype),'values':[encode(x) for x in source.tolist()]}})
for dtype,values in [('float64',[-.5,.5]),('string',['1.25',None]),('object',[1.25,None]),('object',[True,False]),('boolean',[True,None])]:
    source=pd.Series(values,dtype=dtype,index=['row-0','row-1'],name='v')
    entry={'mode':'selection','dtype':dtype,'values':[encode(x) for x in values],'operation':'round','args':[0]}
    try:
        result=source.round(0)
        entry['expected']={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist(),'name':result.name}
    except Exception as e:
        entry['error']=type(e).__name__
    result=source.to_frame().round(0)['v']
    entry['frameExpected']={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist(),'name':result.name}
    cases.append(entry)
queries=[]
columns={'i':'int16','n':'Int16','f':'float32','nf':'Float32','b':'bool','nb':'boolean','s':'string','st':'str','o':'object'}
frame=pd.DataFrame({key:pd.Series([1],dtype=dtype) for key,dtype in columns.items()})
selectors=['number','integer','floating','bool','boolean','object','str','string','int16','Int16','float32','float64',None,[]]
for include in selectors:
    for exclude in [None,'number']:
        entry={'api':'select_dtypes','include':include,'exclude':exclude,'columns':columns}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                entry['expected']=frame.select_dtypes(include=include,exclude=exclude).columns.tolist()
        except Exception as e:
            entry['error']=type(e).__name__
        queries.append(entry)
for left in ['int16','Int16','int64','float32','Float32','bool','object']:
    for right in ['int16','Int16','int64','float32','Float32','bool','object']:
        queries.append({'api':'equals','left':{'values':[1],'dtype':left},'right':{'values':[1],'dtype':right},'expected':pd.Series([1],dtype=left).equals(pd.Series([1],dtype=right))})
for left in [None,np.nan,pd.NA,pd.NaT]:
    for right in [None,np.nan,pd.NA,pd.NaT]:
        queries.append({'api':'equals','left':{'values':[{'$missing':'NaT'} if left is pd.NaT else encode(left)],'dtype':'object'},'right':{'values':[{'$missing':'NaT'} if right is pd.NaT else encode(right)],'dtype':'object'},'expected':pd.Series([left],dtype=object).equals(pd.Series([right],dtype=object))})
Path('test/fixtures/pandas-dtype-queries.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':queries},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(queries)} dtype query contracts')
Path('test/fixtures/pandas-dtype-contracts.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':cases},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(cases)} dtype contracts')

frame_cases=[]
for values in inputs+[[1,True],[1,'1'],['001','false',None]]:
    source=pd.DataFrame({'v':values},index=[f'row-{i}' for i in range(len(values))])
    for operation,args in [('copy',[]),('head',[0]),('tail',[-1]),('dropna',[]),('rename',[{'v':'renamed'}])]:
        result=source.rename(columns=args[0]) if operation=='rename' else getattr(source,operation)(*args)
        frame_cases.append({'values':[encode(x) for x in values], 'operation':operation,'args':args,
            'expected':{'dtypes':{str(c):str(t) for c,t in result.dtypes.items()},'columns':result.columns.tolist(), 'index':result.index.tolist(),'values':[[encode(v) for v in row] for row in result.itertuples(index=False,name=None)]}})
Path('test/fixtures/pandas-frame-dtypes.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':frame_cases},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(frame_cases)} frame inference contracts')

assignment_cases=[]
for dtype in ['int16','Int16','float32','Float32','bool','boolean','str','string','object']:
    for values in [[1,0],[1,None],['a',None],[None,None]]:
        try: source=pd.DataFrame({'v':pd.Series(values,dtype=dtype),'keep':pd.Series([None,None],dtype='object')},index=[0,1])
        except Exception: continue
        for incoming in [[3,4],[3,None],['x',None],[True,False]]:
            for input_dtype in [None,'object','Int16','Float32','string']:
                try: value=incoming if input_dtype is None else pd.Series(incoming,index=[1,0],dtype=input_dtype)
                except Exception: continue
                for operation in ['assign','insert','isetitem']:
                    result=source.copy()
                    if operation=='assign': result=result.assign(v=value)
                    elif operation=='insert': result.insert(1,'new',value)
                    else: result.isetitem(0,value)
                    assignment_cases.append({'dtype':dtype,'values':[encode(x) for x in values],'incoming':[encode(x) for x in incoming],'inputDtype':input_dtype,'operation':operation,
                      'expected':{'dtypes':{str(c):str(t) for c,t in result.dtypes.items()},'columns':result.columns.tolist(),'values':[[encode(v) for v in row] for row in result.itertuples(index=False,name=None)]}})
Path('test/fixtures/pandas-frame-assignment.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':assignment_cases},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(assignment_cases)} frame assignment contracts')

update_cases=[]
for dtype in ['int16','Int16','float32','Float32','bool','boolean','str','string','object']:
    for values in [[1,0],[1,None],['a',None],[None,None]]:
        try: source=pd.DataFrame({'v':pd.Series(values,dtype=dtype)})
        except Exception: continue
        for incoming in [[3,None],[1.5,None],[0.1,None],['x',None],['3',None],[True,None],[None,None],[32768,None]]:
            result=source.copy()
            entry={'dtype':dtype,'values':[encode(x) for x in values],'incoming':[encode(x) for x in incoming]}
            try:
                result.update(pd.DataFrame({'v':incoming}))
                entry['expected']={'dtype':str(result.dtypes.v),'values':[encode(x) for x in result.v.tolist()]}
            except Exception as e: entry['error']=type(e).__name__
            update_cases.append(entry)
Path('test/fixtures/pandas-frame-update.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':update_cases},indent=2,allow_nan=False)+'\n')
print(f'wrote {len(update_cases)} frame update contracts')

rank_cases=[]
for dtype,values in [('int16',[3,1,1]),('float32',[3,1,1]),('Int16',[1,None,1]),('Float32',[1,None,1]),('boolean',[True,None,False]),('string',['b',None,'A']),('str',['10','2',None]),('object',[1,'1',2]),('object',[None,None]),('float64',[np.inf,-np.inf,np.inf,np.nan]),('object',['😀','\ue000','A','a']),('float64',[]),('Int16',[])]:
    source=pd.Series(values,dtype=dtype,index=[f'row-{i}' for i in range(len(values))],name='v')
    for method in ['average','min','max','first','dense']:
        for na_option in ['keep','top','bottom']:
            for ascending,pct in [(True,False),(False,False),(True,True)]:
                options={'method':method,'na_option':na_option,'ascending':ascending,'pct':pct}
                entry={'mode':'selection','dtype':dtype,'values':[encode(x) for x in values],'operation':'rank','args':[options]}
                try:
                    for key,result in [('expected',source.rank(**options)),('frameExpected',source.to_frame().rank(**options)['v'])]:
                        entry[key]={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist(),'name':result.name}
                except Exception as e: entry['error']=type(e).__name__
                rank_cases.append(entry)
cases.extend(rank_cases)

arith_cases=[]
ops=['add','sub','mul','div','floordiv','mod','pow','eq','gt','ne','combine_first']
dtype_pairs=[('int16','int16'),('int16','Int16'),('Int16','int16'),('Int16','Float32'),('float32','float64'),('Float32','Int16'),('bool','bool'),('bool','int16'),('boolean','Int16'),('object','int16'),('string','int16'),('int64','int64')]
value_pairs=[([1,2,3],[4,5,0]),([1,None,3],[4,None,6]),([None,None],[None,None]),([2,4],[2,3])]
for ld,rd in dtype_pairs:
    for lv,rv in value_pairs:
        try:
            left=pd.Series(lv,dtype=ld); right=pd.Series(rv,dtype=rd)
        except Exception:
            continue
        for op in ops:
            entry={'mode':'arithmetic','dtype':ld,'values':[encode(x) for x in lv],
                   'operand':{'dtype':rd,'values':[encode(x) for x in rv]},'operation':op}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result=getattr(left,op)(right)
                entry['expected']={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist()}
            except Exception as e: entry['error']=type(e).__name__
            arith_cases.append(entry)
for ld in ['int16','Int16','float32','Float32','bool','boolean','int64']:
    for lv in [[1,2],[1,None]]:
        try: left=pd.Series(lv,dtype=ld)
        except Exception: continue
        for op in ['add','sub','mul','div','floordiv','mod','pow']:
            for scalar in [2,2.5]:
                entry={'mode':'arithmetic','dtype':ld,'values':[encode(x) for x in lv],
                       'operand':{'scalar':scalar},'operation':op}
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        result=getattr(left,op)(scalar)
                    entry['expected']={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist()}
                except Exception as e: entry['error']=type(e).__name__
                arith_cases.append(entry)
for li,ri in [([0,1],[1,2]),(['a','b'],['b','c']),([0,0],[0,1]),(['b','a'],['a','b'])]:
    left=pd.Series([1,2],index=li); right=pd.Series([10,20],index=ri)
    for op in ['add','gt','combine_first']:
        entry={'mode':'arithmetic','dtype':str(left.dtype),'values':[encode(x) for x in left.tolist()],'index':li,
               'operand':{'dtype':str(right.dtype),'values':[encode(x) for x in right.tolist()],'index':ri},'operation':op}
        try:
            result=getattr(left,op)(right)
            entry['expected']={'dtype':str(result.dtype),'values':[encode(x) for x in result.tolist()],'index':result.index.tolist()}
        except Exception as e: entry['error']=type(e).__name__
        arith_cases.append(entry)
cases.extend(arith_cases)
print(f'added {len(arith_cases)} arithmetic/alignment contracts')

def frame_spec(columns_dtypes,index=None):
    return pd.DataFrame({c:pd.Series(list(v),dtype=d,index=index) for c,(d,v) in columns_dtypes.items()})
def frame_entry(left,operation,right=None,scalar=None,left_index=None,right_index=None):
    entry={'mode':'frame_arithmetic',
           'dtypes':{c:d for c,(d,v) in left.items()},
           'columns':{c:[encode(x) for x in v] for c,(d,v) in left.items()},
           'operation':operation}
    if left_index is not None: entry['index']=left_index
    if scalar is not None:
        entry['operand']={'scalar':scalar}
    else:
        operand={'dtypes':{c:d for c,(d,v) in right.items()},'columns':{c:[encode(x) for x in v] for c,(d,v) in right.items()}}
        if right_index is not None: operand['index']=right_index
        entry['operand']=operand
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            source=frame_spec(left,index=left_index)
            operand=scalar if scalar is not None else frame_spec(right,index=right_index)
            result=getattr(source,operation)(operand)
        entry['expected']={'dtypes':{str(c):str(t) for c,t in result.dtypes.items()},
                           'columns':result.columns.tolist(),'index':result.index.tolist(),
                           'values':[[encode(v) for v in row] for row in result.itertuples(index=False,name=None)]}
    except Exception as e:
        entry['error']=type(e).__name__
    return entry

frame_arith_cases=[]
frame_ops=['add','sub','mul','div','floordiv','mod','pow','eq','gt','ne','combine_first']
def fvals(dtype,variant):
    if dtype in ('bool','boolean'): return ([True,False],[True,None])[variant]
    if dtype in ('str','string'): return (['x','y'],['x',None])[variant]
    return ([1,2],[1,None])[variant]
def build(columns_dtypes,variant):
    return {c:(d,fvals(d,variant)) for c,d in columns_dtypes.items()}
aligned_pairs=[
  ({'a':'int16','b':'int16'},{'a':'int64','b':'int16'}),
  ({'a':'Int16','b':'Float32'},{'a':'int64','b':'Float32'}),
  ({'a':'bool','b':'int16'},{'a':'int16','b':'int16'}),
  ({'a':'boolean','b':'Int16'},{'a':'Int16','b':'Int16'}),
  ({'a':'object','b':'float64'},{'a':'int64','b':'float64'}),
  ({'a':'uint8','b':'int32'},{'a':'int16','b':'int64'}),
  ({'a':'string','b':'int16'},{'a':'string','b':'int16'}),
]
for ld,rd in aligned_pairs:
    for variant in [0,1]:
        left=build(ld,variant); right=build(rd,variant)
        try: frame_spec(left); frame_spec(right)
        except Exception: continue
        ops=frame_ops if 'string' not in ld.values() and 'string' not in rd.values() else ['add','sub','mul','eq','gt','ne','combine_first']
        for op in ops:
            frame_arith_cases.append(frame_entry(left,op,right=right))
scalar_frames=[{'a':'int16','b':'float32'},{'a':'Int16','b':'Float64'},{'a':'bool','b':'uint8'},{'a':'string','b':'object'}]
for ld in scalar_frames:
    for variant in [0,1]:
        left=build(ld,variant)
        try: frame_spec(left)
        except Exception: continue
        ops=['add','sub','mul','div','floordiv','mod','pow','eq','gt','ne'] if 'string' not in ld.values() else ['add','sub','mul','eq','gt']
        for op in ops:
            for scalar in [2,2.5]:
                frame_arith_cases.append(frame_entry(left,op,scalar=scalar))
misaligned=[({'a':'int64','c':'Float32'},None),({'b':'int64','a':'int16'},None),({'a':'int64','b':'int16'},[1,2]),({'a':'int64','b':'int16'},[2,0]),({'c':'int64'},[2,3]),({'b':'string'},None)]
for right_dtypes,right_index in misaligned:
    left={'a':('int16',[1,2]),'b':('Int16',[3,4])}
    right={c:(d,fvals(d,0)) for c,d in right_dtypes.items()}
    for op in frame_ops:
        if 'string' in right_dtypes.values() and op not in ['add','sub','mul','eq','gt','ne','combine_first']: continue
        frame_arith_cases.append(frame_entry(left,op,right=right,right_index=right_index))
cases.extend(frame_arith_cases)
print(f'added {len(frame_arith_cases)} frame arithmetic contracts')

# ---------------------------------------------------------------------------
# Reduction contracts: scalar-returning Series ops and per-column frame ops.
# entry.args are the bun_panda positional args; pandas is called with kwargs.
# ---------------------------------------------------------------------------
reduction_calls = {
  'sum': [({}, [])], 'mean': [({}, [])], 'median': [({}, [])],
  'min': [({}, [])], 'max': [({}, [])], 'prod': [({}, [])],
  'count': [({}, [])], 'nunique': [({}, []), ({'dropna': False}, [False])],
  'all': [({}, []), ({'skipna': False}, [False])],
  'any': [({}, []), ({'skipna': False}, [False])],
  'std': [({}, []), ({'ddof': 0}, [0])], 'var': [({}, []), ({'ddof': 0}, [0])],
  'sem': [({}, []), ({'ddof': 0}, [0])],
  'skew': [({}, [])], 'kurt': [({}, [])],
  'idxmax': [({}, [])], 'idxmin': [({}, [])],
  'argmax': [({}, [])], 'argmin': [({}, [])],
  'quantile': [({'q': 0.25}, [0.25]), ({'q': 0.5}, [0.5])],
  'first_valid_index': [({}, [])], 'last_valid_index': [({}, [])],
  'agg': [({'func': 'sum'}, ['sum']), ({'func': 'min'}, ['min']),
          ({'func': 'mean'}, ['mean']), ({'func': 'count'}, ['count'])],
}
reduction_inputs = {
  'int8': [[3,1,2],[3,2,2],[]],
  'int16': [[3,1,2],[3,2,2],[]],
  'Int16': [[3,1,2],[3,None,2],[],[None,None]],
  'int64': [[3,1,2],[3,2,2],[]],
  'float32': [[3.5,1.5,2.5],[3.5,None,2.5],[]],
  'Float32': [[3.5,1.5,2.5],[3.5,None,2.5],[],[None,None]],
  'float64': [[3.5,1.5,2.5],[3.5,None,2.5],[np.inf,-np.inf],[]],
  'Float64': [[3.5,1.5,2.5],[3.5,None,2.5],[],[None,None]],
  'bool': [[True,False,True],[False,False],[]],
  'boolean': [[True,False,True],[True,None,False],[],[None,None]],
  'string': [['b','a','c'],['b',None,'c'],[]],
  'object': [[3,1,2],[1,'a',2],[None,None],[],['b','a','c'],[1.5,2.5]],
}
reduction_cases = []
for dtype, value_lists in reduction_inputs.items():
    for values in value_lists:
        try:
            source = pd.Series(values, dtype=dtype, index=[f'row-{i}' for i in range(len(values))], name='v')
        except Exception:
            continue
        for operation, variants in reduction_calls.items():
            for kwargs, args in variants:
                entry = {'mode': 'series_reduction', 'dtype': dtype,
                         'values': [encode(x) for x in values],
                         'operation': operation, 'args': args}
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        result = getattr(source, operation)(**kwargs)
                    entry['expected'] = encode(result)
                except Exception as e:
                    entry['error'] = type(e).__name__
                reduction_cases.append(entry)
cases.extend(reduction_cases)
print(f'added {len(reduction_cases)} series reduction contracts')

# Frame reductions: pandas returns a Series indexed by column; the harness
# compares the per-column record to {index, values}.
frame_reduce_specs = [
  {'a':('int16',[3,1,2]),'b':('float32',[1.5,None,0.5])},
  {'a':('Int16',[3,None,2]),'b':('Float32',[None,2.5,0.5]),'c':('boolean',[True,False,None])},
  {'a':('bool',[True,False,True]),'b':('string',['x','y','z'])},
  {'a':('float64',[np.inf,-np.inf,1.5]),'b':('object',[1,'x',2])},
  {'a':('int64',[1,2,3]),'b':('int64',[4,5,6])},
  {'a':('string',['x','y','x']),'b':('string',['p','q','p'])},
  {'a':('float64',[]),'b':('Int16',[])},
]
frame_reduction_ops = [
  ('sum', []), ('mean', []), ('median', []), ('min', []), ('max', []),
  ('prod', []), ('count', []), ('nunique', []), ('all', []), ('any', []),
  ('std', []), ('var', []), ('sem', []), ('skew', []), ('kurt', []),
  ('idxmax', []), ('idxmin', []), ('quantile', [0.5]), ('quantile', [0.25]),
]
frame_reduction_cases = []
for spec in frame_reduce_specs:
    index = [f'row-{i}' for i in range(len(next(iter(spec.values()))[1]))]
    source = frame_spec({c: (d, v) for c, (d, v) in spec.items()}, index=index)
    for operation, args in frame_reduction_ops:
        entry = {'mode': 'frame_reduction',
                 'dtypes': {c: d for c, (d, v) in spec.items()},
                 'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
                 'index': index,
                 'operation': operation, 'args': args}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = getattr(source, operation)(*args)
            entry['expected'] = {'index': result.index.tolist(),
                                 'values': [encode(x) for x in result.tolist()]}
        except Exception as e:
            entry['error'] = type(e).__name__
        frame_reduction_cases.append(entry)
cases.extend(frame_reduction_cases)
print(f'added {len(frame_reduction_cases)} frame reduction contracts')

# ---------------------------------------------------------------------------
# Series-returning ops whose frame counterparts differ (or don't exist):
# value_counts, drop, duplicated, isin, nlargest, nsmallest, describe,
# sort_values with na_position.
# ---------------------------------------------------------------------------
series_view_ops = [
  ('value_counts', [({'normalize': False, 'sort': True, 'ascending': False, 'dropna': True},
                     [{'normalize': False, 'sort': True, 'ascending': False, 'dropna': True}]),
                    ({'normalize': True, 'sort': True, 'ascending': False, 'dropna': True},
                     [{'normalize': True, 'sort': True, 'ascending': False, 'dropna': True}]),
                    ({'normalize': False, 'sort': True, 'ascending': False, 'dropna': False},
                     [{'normalize': False, 'sort': True, 'ascending': False, 'dropna': False}]),
                    ({'normalize': False, 'sort': True, 'ascending': True, 'dropna': True},
                     [{'normalize': False, 'sort': True, 'ascending': True, 'dropna': True}]),
                    ({'normalize': False, 'sort': False, 'ascending': False, 'dropna': True},
                     [{'normalize': False, 'sort': False, 'ascending': False, 'dropna': True}])]),
  ('duplicated', [({'keep': 'first'}, []), ({'keep': 'last'}, ['last']), ({'keep': False}, [False])]),
  ('isin', [({}, [[1, 2]]), ({}, [['a']])]),
  ('nlargest', [({}, [1]), ({}, [2]), ({}, [9])]),
  ('nsmallest', [({}, [1]), ({}, [2]), ({}, [9])]),
  ('describe', [({}, [])]),
  ('drop', [({'labels': ['row-1']}, [['row-1']]),
            ({'labels': 'row-2'}, ['row-2']),
            ({'labels': ['row-9']}, [['row-9']])]),
  ('sort_values', [({'ascending': False, 'na_position': 'first'}, [{'ascending': False, 'na_position': 'first'}]),
                   ({'ascending': True, 'na_position': 'first'}, [{'ascending': True, 'na_position': 'first'}])]),
]
series_view_inputs = {
  'int16': [[3,1,1],[3,None,1],[],[None,None]],
  'Int16': [[3,1,1],[3,None,1],[],[None,None]],
  'float32': [[3.5,1.5,1.5],[3.5,None,1.5],[]],
  'Float32': [[3.5,1.5,1.5],[3.5,None,1.5],[],[None,None]],
  'float64': [[3.5,1.5,1.5],[3.5,np.nan,1.5],[]],
  'bool': [[True,False,True],[True,False],[]],
  'boolean': [[True,False,True],[True,None,False],[]],
  'string': [['b','a','b'],['b',None,'a'],[]],
  'str': [['b','a','b'],[]],
  'object': [[3,1,1],[1,'a',2],[None,None],[],['b','a','b']],
}
series_view_cases = []
for dtype, value_lists in series_view_inputs.items():
    for values in value_lists:
        try:
            source = pd.Series(values, dtype=dtype, index=[f'row-{i}' for i in range(len(values))], name='v')
        except Exception:
            continue
        for operation, variants in series_view_ops:
            for kwargs, args in variants:
                entry = {'mode': 'series_view', 'dtype': dtype,
                         'values': [encode(x) for x in values],
                         'operation': operation, 'args': args}
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        if operation == 'sort_values':
                            result = source.sort_values(**kwargs)
                        else:
                            result = getattr(source, operation)(*args) if not kwargs else getattr(source, operation)(**kwargs)
                    if isinstance(result, pd.Series):
                        entry['expected'] = {'dtype': str(result.dtype),
                                             'values': [encode(x) for x in result.tolist()],
                                             'index': [encode(x) for x in result.index.tolist()],
                                             'indexName': result.index.name,
                                             'name': result.name}
                    else:
                        entry['expected'] = encode(result)
                except Exception as e:
                    entry['error'] = type(e).__name__
                series_view_cases.append(entry)
cases.extend(series_view_cases)
print(f'added {len(series_view_cases)} series view contracts')

# Array-returning Series ops: unique/argsort/mode return arrays (or lists);
# factorize returns (codes, uniques).
series_array_ops = ['unique', 'argsort', 'mode']
series_array_cases = []
for dtype, value_lists in series_view_inputs.items():
    for values in value_lists:
        try:
            source = pd.Series(values, dtype=dtype, index=[f'row-{i}' for i in range(len(values))], name='v')
        except Exception:
            continue
        for operation in series_array_ops:
            entry = {'mode': 'series_array', 'dtype': dtype,
                     'values': [encode(x) for x in values],
                     'operation': operation, 'args': []}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source, operation)()
                if isinstance(result, pd.Series):
                    entry['expected'] = {'values': [encode(x) for x in result.tolist()]}
                else:
                    entry['expected'] = {'values': [encode(x) for x in np.asarray(result).tolist()]}
            except Exception as e:
                entry['error'] = type(e).__name__
            series_array_cases.append(entry)
        entry = {'mode': 'series_array', 'dtype': dtype,
                 'values': [encode(x) for x in values],
                 'operation': 'factorize', 'args': []}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                codes, uniques = source.factorize()
            entry['expected'] = {'codes': [int(c) for c in codes.tolist()],
                                 'uniques': [encode(x) for x in np.asarray(uniques).tolist()]}
        except Exception as e:
            entry['error'] = type(e).__name__
        series_array_cases.append(entry)
cases.extend(series_array_cases)
print(f'added {len(series_array_cases)} series array contracts')

# ---------------------------------------------------------------------------
# Elementwise ops: identical semantics on Series and single-column frames.
# Entries ride the existing 'selection' mode and add frameExpected/frameError.
# ---------------------------------------------------------------------------
elementwise_inputs = {
  'int16': [[3,1,2],[3,None,2],[],[None,None]],
  'Int16': [[3,1,2],[3,None,2],[],[None,None]],
  'int64': [[3,1,2],[3,None,2],[]],
  'float32': [[3.5,1.5,2.5],[3.5,None,2.5],[]],
  'Float32': [[3.5,1.5,2.5],[3.5,None,2.5],[],[None,None]],
  'float64': [[3.5,1.5,2.5],[3.5,np.nan,2.5],[np.inf,-np.inf,np.inf],[]],
  'Float64': [[3.5,1.5,2.5],[3.5,None,2.5],[],[None,None]],
  'bool': [[True,False,True],[False,False],[]],
  'boolean': [[True,False,True],[True,None,False],[],[None,None]],
  'string': [['b','a','c'],['b',None,'c'],[]],
  'object': [[3,1,2],[1,'a',2],[None,None],[],[1.5,2.5]],
}
elementwise_ops = [
  ('diff', [1]), ('diff', [2]), ('diff', [-1]),
  ('pct_change', []), ('pct_change', [2]),
  ('cumsum', []), ('cumprod', []), ('cummin', []), ('cummax', []),
  ('abs', []), ('clip', [1.5]), ('clip', [None, 2.0]),
]
elementwise_cases = []
for dtype, value_lists in elementwise_inputs.items():
    for values in value_lists:
        try:
            source = pd.Series(values, dtype=dtype, index=[f'row-{i}' for i in range(len(values))], name='v')
        except Exception:
            continue
        frame = source.to_frame()
        for operation, args in elementwise_ops:
            entry = {'mode': 'elementwise', 'dtype': dtype,
                     'values': [encode(x) for x in values],
                     'operation': operation, 'args': args}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source, operation)(*args)
                entry['expected'] = {'dtype': str(result.dtype),
                                     'values': [encode(x) for x in result.tolist()],
                                     'index': [encode(x) for x in result.index.tolist()],
                                     'name': result.name}
            except Exception as e:
                entry['error'] = type(e).__name__
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    frame_result = getattr(frame, operation)(*args)['v']
                entry['frameExpected'] = {'dtype': str(frame_result.dtype),
                                          'values': [encode(x) for x in frame_result.tolist()],
                                          'index': [encode(x) for x in frame_result.index.tolist()],
                                          'name': frame_result.name}
            except Exception as e:
                entry['frameError'] = type(e).__name__
            elementwise_cases.append(entry)
cases.extend(elementwise_cases)
print(f'added {len(elementwise_cases)} elementwise contracts')

# ---------------------------------------------------------------------------
# Frame selection: iloc/loc (frame + row forms), drop columns, set_index,
# reset_index, describe, duplicated, isin, nlargest/nsmallest, mode,
# transpose, truncate.
# ---------------------------------------------------------------------------
frame_sel_specs = [
  {'a':('int16',[3,1,2]),'b':('string',['x','y','z']),'c':('boolean',[True,False,None])},
  {'a':('Int16',[3,None,2]),'b':('Float32',[None,2.5,0.5])},
  {'a':('string',['x','y','x']),'b':('string',['p','q','p'])},
  {'a':('bool',[True,False,True]),'b':('float64',[3.5,None,2.5])},
  {'a':('object',[1,'x',2]),'b':('int64',[4,5,6])},
  {'a':('int16',[]),'b':('string',[])},
]
def frame_expected(result):
    return {'dtypes': {str(c): str(t) for c, t in result.dtypes.items()},
            'columns': result.columns.tolist(),
            'index': [encode(x) for x in result.index.tolist()],
            'indexName': result.index.name,
            'values': [[encode(v) for v in row] for row in result.itertuples(index=False, name=None)]}
frame_selection_cases = []
for spec in frame_sel_specs:
    index = [f'row-{i}' for i in range(len(next(iter(spec.values()))[1]))]
    source = frame_spec({c: (d, v) for c, (d, v) in spec.items()}, index=index)
    n = len(index)
    ops = []
    ops += [('iloc', [p], {'positions': p}) for p in ([[n-1,0]] if n else [[]]) ]
    ops += [('iloc', [0], {'positions': 0}), ('iloc', [n+3], {'positions': n+3}), ('iloc', [-1], {'positions': -1})]
    if n:
        ops += [('loc', [['row-2','row-0'][:min(n,2)]], {'labels': ['row-2','row-0'][:min(n,2)]}),
                ('loc', ['row-1'], {'labels': 'row-1'}),
                ('loc', [['row-9']], {'labels': ['row-9']})]
    ops += [('drop', [['b']], {'columns': ['b']}),
            ('drop', [['b','missing']], {'columns': ['b','missing']})]
    if 'b' in spec and spec['b'][0] == 'string':
        ops += [('set_index', ['b'], {'keys': 'b'}),
                ('set_index', ['b', False], {'keys': 'b', 'drop': False})]
    ops += [('reset_index', [], {}), ('describe', [], {}), ('mode', [], {}), ('transpose', [], {})]
    ops += [('duplicated', [], {}), ('duplicated', [['a']], {'subset': 'a'}),
            ('duplicated', [None, 'last'], {'keep': 'last'}),
            ('duplicated', [None, False], {'keep': False})]
    ops += [('isin', [{'a': [1, 3]}], {'values': {'a': [1, 3]}}),
            ('isin', [[1, 'x']], {'values': [1, 'x']})]
    if any(d in ('int16','Int16','int64','float64','Float32') for d,_ in spec.values()):
        ops += [('nlargest', [1, 'a'], {'n': 1, 'columns': 'a'}),
                ('nsmallest', [2, 'a'], {'n': 2, 'columns': 'a'})]
    if n:
        ops += [('truncate', ['row-1','row-2'], {'before': 'row-1', 'after': 'row-2'})]
    for operation, args, kwargs in ops:
        entry = {'mode': 'frame_selection',
                 'dtypes': {c: d for c, (d, v) in spec.items()},
                 'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
                 'index': index, 'operation': operation, 'args': args}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                if operation == 'iloc': result = source.iloc[kwargs['positions']]
                elif operation == 'loc': result = source.loc[kwargs['labels']]
                elif operation == 'drop': result = source.drop(columns=kwargs['columns'])
                elif operation == 'set_index': result = source.set_index(kwargs['keys'], drop=kwargs.get('drop', True))
                elif operation == 'duplicated': result = source.duplicated(**kwargs)
                elif operation == 'isin': result = source.isin(kwargs['values'])
                elif operation == 'nlargest': result = source.nlargest(kwargs['n'], columns=kwargs['columns'])
                elif operation == 'nsmallest': result = source.nsmallest(kwargs['n'], columns=kwargs['columns'])
                elif operation == 'truncate': result = source.truncate(before=kwargs['before'], after=kwargs['after'])
                else: result = getattr(source, operation)(*args)
            if isinstance(result, pd.Series):
                entry['rowExpected'] = {'index': [encode(x) for x in result.index.tolist()],
                                        'name': result.name,
                                        'values': [encode(x) for x in result.tolist()],
                                        'dtype': str(result.dtype)}
            else:
                entry['expected'] = frame_expected(result)
        except Exception as e:
            entry['error'] = type(e).__name__
        frame_selection_cases.append(entry)
cases.extend(frame_selection_cases)
print(f'added {len(frame_selection_cases)} frame selection contracts')

# ---------------------------------------------------------------------------
# GroupBy aggregations: pandas puts keys into the index (as_index=True
# semantics); the harness adapts bun_panda's as_index=False output.
# ---------------------------------------------------------------------------
groupby_specs = [
  ({'k':('string',['a','b','a','b']),'v':('int16',[3,1,2,4])}),
  ({'k':('string',['a','b','a',None]),'v':('Int16',[3,None,2,1])}),
  ({'k':('string',['b','a','b','a']),'v':('Float32',[1.5,None,2.5,0.5]),'w':('boolean',[True,False,None,True])}),
  ({'k':('int16',[2,1,2,1]),'v':('boolean',[True,False,None,True])}),
  ({'k':('string',['b','a','b','a']),'v':('string',['x','y','z',None])}),
  ({'k':('string',['a','b','a','b']),'v':('object',[1,'x',2,3])}),
  ({'k':('string',['a','b','a','b']),'v':('float64',[1.5,np.nan,2.5,0.5])}),
]
groupby_ops = ['sum','mean','median','min','max','prod','count','nunique',
               'std','var','sem','first','last','size','skew','kurt']
groupby_cases = []
for spec in groupby_specs:
    index = [f'row-{i}' for i in range(len(next(iter(spec.values()))[1]))]
    source = frame_spec({c: (d, v) for c, (d, v) in spec.items()}, index=index)
    grouped = source.groupby('k')
    for operation in groupby_ops:
        entry = {'mode': 'groupby',
                 'dtypes': {c: d for c, (d, v) in spec.items()},
                 'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
                 'index': index, 'by': 'k', 'operation': operation, 'args': []}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = getattr(grouped, operation)()
            if isinstance(result, pd.Series):
                entry['seriesExpected'] = {'dtype': str(result.dtype),
                                           'values': [encode(x) for x in result.tolist()],
                                           'index': [encode(x) for x in result.index.tolist()]}
            else:
                entry['expected'] = frame_expected(result)
        except Exception as e:
            entry['error'] = type(e).__name__
        groupby_cases.append(entry)
cases.extend(groupby_cases)
print(f'added {len(groupby_cases)} groupby contracts')

# ---------------------------------------------------------------------------
# Frame binary ops: concat (rows/axis=1/ignore_index/join), join, merge.
# ---------------------------------------------------------------------------
binary_specs = {
  'plain': ({'x':('int16',[1,2]),'y':('string',['p','q'])},
            {'x':('int16',[3]),'y':('string',['r'])}),
  'outer': ({'x':('int16',[1,2]),'y':('string',['p','q'])},
            {'x':('int64',[3]),'z':('boolean',[True])}),
  'misaligned': ({'x':('Int16',[1,None]),'y':('Float32',[1.5,2.5])},
                 {'x':('int64',[3,4]),'w':('string',['r','s'])}),
}
binary_index = {
  'plain': (['a','b'], ['c']),
  'outer': (['a','b'], ['c']),
  'misaligned': (['a','b'], ['b','c']),
}
binary_cases = []
for label, (left_spec, right_spec) in binary_specs.items():
    li, ri = binary_index[label]
    left = frame_spec({c: (d, v) for c, (d, v) in left_spec.items()}, index=li)
    right = frame_spec({c: (d, v) for c, (d, v) in right_spec.items()}, index=ri)
    def spec_columns(s): return {c: [encode(x) for x in v] for c, (d, v) in s.items()}
    def spec_dtypes(s): return {c: d for c, (d, v) in s.items()}
    concat_variants = [
      ('concat', {}, {}),
      ('concat', {'ignore_index': True}, {'ignore_index': True}),
      ('concat', {'join': 'inner'}, {'join': 'inner'}),
      ('concat', {'axis': 1}, {'axis': 1}),
    ]
    for operation, opts, pandas_kwargs in concat_variants:
        entry = {'mode': 'frame_binary', 'left': {'dtypes': spec_dtypes(left_spec), 'columns': spec_columns(left_spec), 'index': li},
                 'right': {'dtypes': spec_dtypes(right_spec), 'columns': spec_columns(right_spec), 'index': ri},
                 'operation': operation, 'args': [opts]}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = pd.concat([left, right], **pandas_kwargs)
            entry['expected'] = frame_expected(result)
        except Exception as e:
            entry['error'] = type(e).__name__
        binary_cases.append(entry)
    for how in ['left', 'inner', 'outer', 'right']:
        for suffixes in [None, ['_l', '_r']]:
            entry = {'mode': 'frame_binary', 'left': {'dtypes': spec_dtypes(left_spec), 'columns': spec_columns(left_spec), 'index': li},
                     'right': {'dtypes': spec_dtypes(right_spec), 'columns': spec_columns(right_spec), 'index': ri},
                     'operation': 'join', 'args': [{'how': how, **({'suffixes': suffixes} if suffixes else {})}]}
            try:
                kwargs = {'how': how}
                if suffixes: kwargs.update({'lsuffix': suffixes[0], 'rsuffix': suffixes[1]})
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = left.join(right, **kwargs)
                entry['expected'] = frame_expected(result)
            except Exception as e:
                entry['error'] = type(e).__name__
            binary_cases.append(entry)
    shared = [c for c in left_spec if c in right_spec]
    if shared:
        for how in ['inner', 'left', 'outer', 'right']:
            entry = {'mode': 'frame_binary', 'left': {'dtypes': spec_dtypes(left_spec), 'columns': spec_columns(left_spec), 'index': li},
                     'right': {'dtypes': spec_dtypes(right_spec), 'columns': spec_columns(right_spec), 'index': ri},
                     'operation': 'merge', 'args': [{'on': shared[0], 'how': how}]}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = left.merge(right, on=shared[0], how=how)
                entry['expected'] = frame_expected(result)
            except Exception as e:
                entry['error'] = type(e).__name__
            binary_cases.append(entry)
cases.extend(binary_cases)
print(f'added {len(binary_cases)} frame binary contracts')

# ---------------------------------------------------------------------------
# Window contracts: Series rolling/expanding/ewm. pandas returns a Series of
# float64; bun_panda's rolling/expanding accessors yield bare value lists
# (null for under-populated windows) while ewm returns a Series, so the
# harness compares values missing-aware plus dtype/index/name when the
# result is a Series.
# ---------------------------------------------------------------------------
window_cases = []
window_inputs = [
  ('int16', [3, 1, 4, 1, 5]),
  ('Int16', [3, None, 1, None, 5]),
  ('int64', [3, 1, 4, 1, 5]),
  ('float64', [3.5, 1.5, 4.5, 0.5]),
  ('float64', [3.5, np.nan, 1.5, np.nan]),
  ('float64', [np.inf, 1.0, -np.inf, 2.0]),
  ('Float32', [3.5, None, 1.5]),
  ('bool', [True, False, True, True]),
  ('boolean', [True, None, True]),
  ('str', ['a', 'b', 'c', 'd']),
  ('object', [1, 'a', 2]),
  ('object', [1, 2, 3]),
  ('object', [None, None]),
  ('int16', []),
]
window_methods = ['mean', 'sum', 'min', 'max', 'count', 'std', 'median']
for dtype, values in window_inputs:
    try:
        source = pd.Series(values, dtype=dtype,
                           index=[f'row-{i}' for i in range(len(values))], name='v')
    except Exception:
        continue
    rolling_variants = [
        ([2], {'window': 2}),
        ([3], {'window': 3}),
        ([3, 1], {'window': 3, 'min_periods': 1}),
        ([2, 1], {'window': 2, 'min_periods': 1}),
        ([4, 2], {'window': 4, 'min_periods': 2}),
    ]
    for window_args, pandas_kwargs in rolling_variants:
        for method in window_methods:
            entry = {'mode': 'window', 'dtype': dtype,
                     'values': [encode(x) for x in values],
                     'operation': 'rolling', 'windowArgs': window_args,
                     'method': method}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source.rolling(**pandas_kwargs), method)()
                entry['expected'] = {'values': [encode(x) for x in result.tolist()],
                                     'index': [encode(x) for x in result.index.tolist()],
                                     'name': result.name, 'dtype': str(result.dtype)}
            except Exception as e:
                entry['error'] = type(e).__name__
            window_cases.append(entry)
    for window_args, pandas_kwargs in [([], {}), ([2], {'min_periods': 2})]:
        for method in window_methods:
            entry = {'mode': 'window', 'dtype': dtype,
                     'values': [encode(x) for x in values],
                     'operation': 'expanding', 'windowArgs': window_args,
                     'method': method}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source.expanding(**pandas_kwargs), method)()
                entry['expected'] = {'values': [encode(x) for x in result.tolist()],
                                     'index': [encode(x) for x in result.index.tolist()],
                                     'name': result.name, 'dtype': str(result.dtype)}
            except Exception as e:
                entry['error'] = type(e).__name__
            window_cases.append(entry)
    ewm_variants = [
        ([2], {'span': 2}),
        ([3], {'span': 3}),
        ([2, {'min_periods': 2}], {'span': 2, 'min_periods': 2}),
        ([1], {'span': 1}),
    ]
    for window_args, pandas_kwargs in ewm_variants:
        for method in ['mean', 'sum', 'std']:
            entry = {'mode': 'window', 'dtype': dtype,
                     'values': [encode(x) for x in values],
                     'operation': 'ewm', 'windowArgs': window_args,
                     'method': method}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source.ewm(**pandas_kwargs), method)()
                entry['expected'] = {'values': [encode(x) for x in result.tolist()],
                                     'index': [encode(x) for x in result.index.tolist()],
                                     'name': result.name, 'dtype': str(result.dtype)}
            except Exception as e:
                entry['error'] = type(e).__name__
            window_cases.append(entry)
cases.extend(window_cases)
print(f'added {len(window_cases)} window contracts')

# ---------------------------------------------------------------------------
# .str accessor contracts: element-wise string ops across str/string/object
# dtypes. bun_panda's StringMethods returns bare lists (null for missing or
# non-string cells), so the harness compares value lists missing-aware.
# ---------------------------------------------------------------------------
str_cases = []
str_inputs = [
  ('str', ['aBc', None, 'X Y1']),
  ('str', ['Hello World', 'abc123', '  pad  ']),
  ('str', ['a=b=c', 'x', None]),
  ('str', []),
  ('string', ['aBc', None, 'X Y1']),
  ('string', ['Hello World', 'abc123', None]),
  ('object', ['aBc', None, 'X Y1']),
  ('object', ['aBc', 1, 'X Y1', None]),
]
def encode_nested(x):
    if isinstance(x, (list, tuple)):
        return [encode_nested(v) for v in x]
    return encode(x)
str_ops = [
  ('upper', [], {}), ('lower', [], {}), ('capitalize', [], {}), ('title', [], {}),
  ('strip', [], {}), ('lstrip', [], {}), ('rstrip', [], {}),
  ('zfill', [5], {'width': 5}),
  ('slice', [1], {'start': 1}), ('slice', [0, 2], {'start': 0, 'stop': 2}),
  ('slice', [-2], {'start': -2}),
  ('replace', ['a', 'X'], {'pat': 'a', 'repl': 'X'}),
  ('replace', ['a', 'X', False], {'pat': 'a', 'repl': 'X', 'regex': False}),
  ('replace', ['\\d', '#', True], {'pat': r'\d', 'repl': '#', 'regex': True}),
  ('contains', ['a', False], {'pat': 'a', 'regex': False}),
  ('contains', ['a|y', True], {'pat': 'a|y'}),
  ('startswith', ['a'], {'pat': 'a'}), ('endswith', ['c'], {'pat': 'c'}),
  ('match', ['a.c'], {'pat': 'a.c'}), ('find', ['c'], {'sub': 'c'}),
  ('get', [1], {'i': 1}), ('get', [-1], {'i': -1}),
  ('len', [], {}), ('count', ['a|c'], {'pat': 'a|c'}),
  ('isalpha', [], {}), ('isdigit', [], {}), ('isnumeric', [], {}),
  ('isspace', [], {}), ('isalnum', [], {}), ('islower', [], {}), ('isupper', [], {}),
  ('swapcase', [], {}), ('casefold', [], {}),
  ('center', [8, '*'], {'width': 8, 'fillchar': '*'}),
  ('ljust', [6, '.'], {'width': 6, 'fillchar': '.'}),
  ('rjust', [6, '.'], {'width': 6, 'fillchar': '.'}),
  ('pad', [8, 'both', '*'], {'width': 8, 'side': 'both', 'fillchar': '*'}),
  ('removeprefix', ['a'], {'prefix': 'a'}), ('removesuffix', ['c'], {'suffix': 'c'}),
  ('repeat', [2], {'repeats': 2}),
  ('findall', ['[a-z]'], {'pat': '[a-z]'}),
  ('split', [' '], {'pat': ' '}), ('split', [], {}),
  ('rsplit', [' ', 1], {'pat': ' ', 'n': 1}),
  ('partition', [' '], {'sep': ' ', 'expand': False}),
  ('rpartition', [' '], {'sep': ' ', 'expand': False}),
  ('extract', ['(\\d+)'], {'pat': r'(\d+)', 'expand': False}),
]
for dtype, values in str_inputs:
    try:
        source = pd.Series(values, dtype=dtype, name='v')
    except Exception:
        continue
    for operation, args, kwargs in str_ops:
        entry = {'mode': 'str', 'dtype': dtype,
                 'values': [encode(x) for x in values],
                 'operation': operation, 'args': args}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = getattr(source.str, operation)(**kwargs)
            if isinstance(result, pd.Series):
                entry['expected'] = {'values': [encode_nested(x) for x in result.tolist()]}
            else:
                entry['expected'] = {'values': encode_nested(result)}
        except Exception as e:
            entry['error'] = type(e).__name__
        str_cases.append(entry)
cases.extend(str_cases)
print(f'added {len(str_cases)} str accessor contracts')

# ---------------------------------------------------------------------------
# .dt accessor contracts: datetime64 naive + fixed-offset aware dtypes.
# Naive wall clocks are stored as the UTC instant of their reading (the
# pandas convention used by this library); aware columns relabel the zone.
# ---------------------------------------------------------------------------
dt_cases = []
dt_ops = ['year', 'month', 'day', 'hour', 'minute', 'second',
          'dayofweek', 'dayofyear', 'day_name', 'date', 'isocalendar_week']
def encode_cell(x):
    if x is pd.NaT:
        return {'$missing': 'NaT'}
    if isinstance(x, (pd.Timestamp,)) or (hasattr(x, 'isoformat') and not isinstance(x, (str, bytes))):
        return x.isoformat()[:10] if type(x).__name__ == 'date' else str(x)
    return encode(x)
dt_inputs = {
  'datetime64[ns]': (
    ['2020-01-15 10:30:45', '2021-02-28 23:59:58', None, '2020-12-31 05:30:00'],
    lambda v: pd.to_datetime(pd.Series(v))),
  'datetime64[ns]2': (
    [],
    lambda v: pd.to_datetime(pd.Series(v))),
  'datetime64[ns, US/Eastern]': (
    ['2020-01-15T10:30:45Z', '2020-01-31T23:30:00Z', None],
    lambda v: pd.to_datetime(pd.Series(v), utc=True).dt.tz_convert('US/Eastern')),
  'datetime64[ns, Asia/Tokyo]': (
    ['2020-01-15T10:30:45Z', '2020-06-15T12:00:00Z', None],
    lambda v: pd.to_datetime(pd.Series(v), utc=True).dt.tz_convert('Asia/Tokyo')),
  'datetime64[ns, UTC]': (
    ['2020-01-15T10:30:45Z', '2020-01-02T00:00:00Z', None],
    lambda v: pd.to_datetime(pd.Series(v), utc=True)),
}
for dtype, (values, build) in dt_inputs.items():
    entry_dtype = 'datetime64[ns]' if dtype == 'datetime64[ns]2' else dtype
    source = build(values)
    for operation in dt_ops:
        entry = {'mode': 'dt', 'dtype': entry_dtype,
                 'values': [encode(x) if x is not None else None for x in values],
                 'operation': operation}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                if operation == 'isocalendar_week':
                    result = source.dt.isocalendar().week
                elif operation == 'day_name':
                    result = getattr(source.dt, operation)()
                else:
                    # year/month/day/hour/.../date are pandas properties, not
                    # methods — access without calling.
                    result = getattr(source.dt, operation)
            entry['expected'] = {'values': [encode_cell(x) for x in result.tolist()]}
        except Exception as e:
            entry['error'] = type(e).__name__
        dt_cases.append(entry)
cases.extend(dt_cases)
print(f'added {len(dt_cases)} dt accessor contracts')

# ---------------------------------------------------------------------------
# Frame window contracts: rolling/expanding/ewm on frames. pandas applies
# count to every column and raises DataError for the other aggregations when
# any column is non-numeric; all result columns are float64.
# ---------------------------------------------------------------------------
frame_window_cases = []
frame_window_specs = [
  {'n': ('int16', [3, 1, 4, 2]), 'f': ('float32', [1.5, None, 2.5, 0.5])},
  {'a': ('int64', [1, 2, 3]), 'b': ('float64', [np.nan, 2.0, 3.0])},
  {'a': ('boolean', [True, None, True]), 'b': ('int16', [1, 2, 3])},
  {'s': ('string', ['a', 'b', 'c']), 'n': ('int16', [1, 2, 3])},
  {'s': ('string', ['a', 'b'])},
]
for spec in frame_window_specs:
    index = [f'row-{i}' for i in range(len(next(iter(spec.values()))[1]))]
    source = frame_spec({c: (d, v) for c, (d, v) in spec.items()}, index=index)
    for window_args, pandas_kwargs in [([2], {'window': 2}),
                                       ([2, 1], {'window': 2, 'min_periods': 1}),
                                       ([3], {'window': 3})]:
        for method in window_methods:
            entry = {'mode': 'frame_window',
                     'dtypes': {c: d for c, (d, v) in spec.items()},
                     'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
                     'index': index, 'operation': 'rolling',
                     'windowArgs': window_args, 'method': method}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source.rolling(**pandas_kwargs), method)()
                entry['expected'] = frame_expected(result)
            except Exception as e:
                entry['error'] = type(e).__name__
            frame_window_cases.append(entry)
    for window_args, pandas_kwargs in [([], {}), ([2], {'min_periods': 2})]:
        for method in window_methods:
            entry = {'mode': 'frame_window',
                     'dtypes': {c: d for c, (d, v) in spec.items()},
                     'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
                     'index': index, 'operation': 'expanding',
                     'windowArgs': window_args, 'method': method}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source.expanding(**pandas_kwargs), method)()
                entry['expected'] = frame_expected(result)
            except Exception as e:
                entry['error'] = type(e).__name__
            frame_window_cases.append(entry)
    for window_args, pandas_kwargs in [([2], {'span': 2}),
                                       ([2, {'min_periods': 2}], {'span': 2, 'min_periods': 2})]:
        for method in ['mean', 'sum', 'std']:
            entry = {'mode': 'frame_window',
                     'dtypes': {c: d for c, (d, v) in spec.items()},
                     'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
                     'index': index, 'operation': 'ewm',
                     'windowArgs': window_args, 'method': method}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    result = getattr(source.ewm(**pandas_kwargs), method)()
                entry['expected'] = frame_expected(result)
            except Exception as e:
                entry['error'] = type(e).__name__
            frame_window_cases.append(entry)
cases.extend(frame_window_cases)
print(f'added {len(frame_window_cases)} frame window contracts')

# ---------------------------------------------------------------------------
# Pivot contracts: df.pivot and df.pivot_table. pandas consumes the key
# column into the index while bun_panda keeps it as a data column, so the
# harness adapts (groupby-style) before comparing.
# ---------------------------------------------------------------------------
frame_pivot_cases = []
pivot_specs = [
  {'i': ('string', ['b', 'a', 'b', 'a']), 'c': ('string', ['y', 'x', 'x', 'y']),
   'v': ('int16', [1, 2, 3, 4])},
  {'i': ('string', ['b', 'a', 'a']), 'c': ('string', ['y', 'x', 'z']),
   'v': ('float64', [1.0, 2.0, 3.0])},
  {'i': ('string', ['a', 'b', 'a']), 'c': ('string', ['x', 'y', 'x']),
   'v': ('int16', [1, 2, 3]), 'w': ('float64', [10.0, 20.0, 30.0])},
]
for spec in pivot_specs:
    index = [f'row-{i}' for i in range(len(next(iter(spec.values()))[1]))]
    source = frame_spec({c: (d, v) for c, (d, v) in spec.items()}, index=index)
    base = {'mode': 'frame_pivot',
            'dtypes': {c: d for c, (d, v) in spec.items()},
            'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
            'index': index, 'keyColumns': ['i']}
    entry = {**base, 'operation': 'pivot', 'args': ['i', 'c', 'v']}
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            result = source.pivot(index='i', columns='c', values='v')
        entry['expected'] = frame_expected(result)
    except Exception as e:
        entry['error'] = type(e).__name__
    frame_pivot_cases.append(entry)
    table_variants = [
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum'},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum'}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'mean'},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'mean'}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'count'},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'count'}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'min'},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'min'}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'max'},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'max'}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'fill_value': 0},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'fill_value': 0}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'margins': True},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'margins': True}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'margins': True, 'margins_name': 'Tot'},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'margins': True, 'margins_name': 'Tot'}),
        ({'index': 'i', 'values': 'v', 'aggfunc': 'sum'},
         {'index': 'i', 'values': 'v', 'aggfunc': 'sum'}),
        ({'index': 'i', 'values': 'v', 'aggfunc': 'mean'},
         {'index': 'i', 'values': 'v', 'aggfunc': 'mean'}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'sort': False},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'sort': False}),
        ({'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'dropna': False},
         {'index': 'i', 'columns': 'c', 'values': 'v', 'aggfunc': 'sum',
          'dropna': False}),
    ]
    for options, kwargs in table_variants:
        entry = {**base, 'operation': 'pivot_table', 'args': [options]}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = source.pivot_table(**kwargs)
            entry['expected'] = frame_expected(result)
        except Exception as e:
            entry['error'] = type(e).__name__
        frame_pivot_cases.append(entry)
cases.extend(frame_pivot_cases)
print(f'added {len(frame_pivot_cases)} pivot contracts')

# ---------------------------------------------------------------------------
# Frame value_counts / drop_duplicates / fillna / describe / mode: ride the
# existing frame_selection mode. value_counts returns a Series in pandas;
# reset_index() gives exactly the shape bun_panda returns (key columns then
# count/proportion), so it is encoded as a frame expectation.
# ---------------------------------------------------------------------------
frame_extra_specs = [
  {'a': ('string', ['x', 'y', 'x', None]), 'b': ('int16', [1, 2, 1, 3]),
   'c': ('boolean', [True, False, True, None])},
  {'a': ('string', ['p', 'p', 'q']), 'b': ('float64', [1.5, None, 1.5])},
  {'a': ('int16', [3, 1, 3]), 'b': ('string', ['u', 'v', 'u'])},
  {'a': ('object', [1, 'x', 1]), 'b': ('float64', [np.nan, 2.0, np.nan])},
]
frame_extra_ops = [
  ('drop_duplicates', [], {}),
  ('drop_duplicates', ['a'], {'subset': 'a'}),
  ('drop_duplicates', [['a', 'b'], 'last'], {'subset': ['a', 'b'], 'keep': 'last'}),
  ('drop_duplicates', [None, False], {'keep': False}),
  ('drop_duplicates', [None, 'first', True], {'keep': 'first', 'ignore_index': True}),
  ('duplicated', [], {}),
  ('duplicated', [['a', 'b'], 'last'], {'subset': ['a', 'b'], 'keep': 'last'}),
  ('value_counts', [], {}),
  ('value_counts', [{'subset': 'a'}], {'subset': 'a'}),
  ('value_counts', [{'subset': 'a', 'normalize': True}], {'subset': 'a', 'normalize': True}),
  ('value_counts', [{'subset': 'a', 'dropna': False}], {'subset': 'a', 'dropna': False}),
  ('value_counts', [{'subset': ['a', 'b'], 'sort': False}], {'subset': ['a', 'b'], 'sort': False}),
  ('value_counts', [{'ascending': True}], {'ascending': True}),
  ('fillna', [0], {'value': 0}),
  ('fillna', [{'a': 'zz'}], {'value': {'a': 'zz'}}),
  ('fillna', [{'a': 'zz', 'b': 0}], {'value': {'a': 'zz', 'b': 0}}),
  ('describe', [], {}),
  ('mode', [], {}),
]
frame_extra_cases = []
for spec in frame_extra_specs:
    index = [f'row-{i}' for i in range(len(next(iter(spec.values()))[1]))]
    source = frame_spec({c: (d, v) for c, (d, v) in spec.items()}, index=index)
    for operation, args, kwargs in frame_extra_ops:
        entry = {'mode': 'frame_selection',
                 'dtypes': {c: d for c, (d, v) in spec.items()},
                 'columns': {c: [encode(x) for x in v] for c, (d, v) in spec.items()},
                 'index': index, 'operation': operation, 'args': args}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                if operation == 'value_counts':
                    result = source.value_counts(**kwargs).reset_index()
                elif operation == 'drop_duplicates':
                    result = source.drop_duplicates(**kwargs)
                elif operation == 'duplicated':
                    result = source.duplicated(**kwargs)
                elif operation == 'fillna':
                    result = source.fillna(**kwargs)
                else:
                    result = getattr(source, operation)()
            if isinstance(result, pd.Series):
                entry['rowExpected'] = {'index': [encode(x) for x in result.index.tolist()],
                                        'name': result.name,
                                        'values': [encode(x) for x in result.tolist()],
                                        'dtype': str(result.dtype)}
            else:
                entry['expected'] = frame_expected(result)
        except Exception as e:
            entry['error'] = type(e).__name__
        frame_extra_cases.append(entry)
cases.extend(frame_extra_cases)
print(f'added {len(frame_extra_cases)} frame counts/fill contracts')

# Series fillna(dict)/list/None variants: pandas maps dict keys by index
# label, rejects non-scalar/non-dict values, and treats None as a no-op.
series_fillna_cases = []
for dtype, values in [('int16', [1, None, None]), ('Int16', [1, None, None]),
                      ('float64', [1.0, np.nan, np.nan]), ('string', ['x', None, None]),
                      ('boolean', [True, None, None]), ('object', [1, None, 'a'])]:
    try:
        source = pd.Series(values, dtype=dtype,
                           index=[f'row-{i}' for i in range(len(values))], name='v')
    except Exception:
        continue
    for arg in [{'row-1': 7}, {'row-1': 7, 'row-2': 8.5}, [0], None]:
        entry = {'mode': 'replacement', 'dtype': dtype,
                 'values': [encode(x) for x in values],
                 'operation': 'fillna', 'args': [arg]}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result = source.fillna(arg)
            entry['expected'] = {'dtype': str(result.dtype),
                                 'values': [encode(x) for x in result.tolist()],
                                 'index': result.index.tolist(), 'name': result.name}
        except Exception as e:
            entry['error'] = type(e).__name__
        series_fillna_cases.append(entry)
cases.extend(series_fillna_cases)
print(f'added {len(series_fillna_cases)} series fillna variant contracts')

# Series drop_duplicates keep variants (first already covered by selection).
series_dd_cases = []
for dtype, values in [('Int16', [1, None, 1]), ('float64', [1.5, 1.5, np.nan]),
                      ('string', ['b', 'a', 'b']), ('object', [1, 'a', 1])]:
    try:
        source = pd.Series(values, dtype=dtype,
                           index=[f'row-{i}' for i in range(len(values))], name='v')
    except Exception:
        continue
    for keep in ['last', False]:
        result = source.drop_duplicates(keep=keep)
        series_dd_cases.append({'mode': 'selection', 'dtype': dtype,
                                'values': [encode(x) for x in values],
                                'operation': 'drop_duplicates', 'args': [keep],
                                'expected': {'dtype': str(result.dtype),
                                             'values': [encode(x) for x in result.tolist()],
                                             'index': result.index.tolist(),
                                             'name': result.name}})
cases.extend(series_dd_cases)
print(f'added {len(series_dd_cases)} series drop_duplicates contracts')

Path('test/fixtures/pandas-dtype-contracts.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':cases},indent=2,allow_nan=False)+'\n')
print(f'added {len(rank_cases)} rank contracts; {len(cases)} total dtype contracts')
