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
Path('test/fixtures/pandas-dtype-contracts.json').write_text(json.dumps({'pandasVersion':pd.__version__,'cases':cases},indent=2,allow_nan=False)+'\n')
print(f'added {len(rank_cases)} rank contracts; {len(cases)} total dtype contracts')
