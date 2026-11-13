"""Independent simulations for the four lectures; no authoring functions imported."""
from pathlib import Path
from collections import defaultdict,Counter
import json,re,xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[1]
data=json.loads((ROOT/'course/data/cases.json').read_text())
manifest=json.loads((ROOT/'course/manifest.json').read_text())
report={}

# 1. Execute all twelve deliveries; identity, business key and value are distinct.
seen=set();account=defaultdict(lambda:[0,0]);trace=[];accepted=[]
for e in data['events']:
 reason='status' if e['status']!='PAID' else ('duplicate' if e['eventId'] in seen else 'new')
 if reason=='new':
  seen.add(e['eventId']);account[e['userId']][0]+=e['amountCents'];account[e['userId']][1]+=1;accepted.append(e['eventId'])
 trace.append(dict(event=e['eventId'],reason=reason,state={k:v[:] for k,v in account.items()}))
assert dict(account)=={'A':[6000,3],'B':[14000,3],'C':[12000,2]}
assert Counter(x['reason'] for x in trace)=={'new':8,'status':3,'duplicate':1}
assert len(seen)==8 and sum(v[0] for v in account.values())==32000
assert sum(e['amountCents'] for e in data['events'] if e['status']=='PAID')==34000
assert trace[3]['state']=={'A':[5000,2],'B':[5000,1],'C':[4000,1]}
net=0;ids=set();refund=[]
for kind,eid,amount in [('pay','P01',3000),('refund','R01',1000),('refund','R01',1000)]:
 if (kind,eid) not in ids:net+=amount if kind=='pay' else -amount;ids.add((kind,eid))
 refund.append(net)
assert refund==[3000,2000,2000]
report['lesson01']=dict(deliveries=trace,final=dict(account),refund=refund,designNetCents=5000+3000-1200)

# 2. Count the actual Tasks in the proposed placement; do not infer from a total.
placement=[['S0/F0','A0','O0'],['A1'],['S1/F1','A2','O1'],['A3']]
tasks=[x for slot in placement for x in slot]
assert len(tasks)==len(set(tasks))==8
assert set(tasks)=={'S0/F0','S1/F1','A0','A1','A2','A3','O0','O1'}
assert sum([2,2,4,2])==10 and len(placement)==max([2,4,2])==4
arriving=6000*.75;capacity=4*1000;queue=max(0,arriving-capacity)*20
assert (arriving,capacity,queue)==(4500,4000,10000)
assert queue/capacity==2.5 and queue/(5000-arriving)==20
loads=[3000,500,500,500];done=[min(v,1000) for v in loads]
assert sum(done)==2500 and loads[0]-done[0]==2000
exercise=(3600-3000)*15
assert exercise==9000 and exercise/3000==3 and exercise/(4000-3600)==22.5
report['lesson02']=dict(tasks=tasks,slotCount=len(placement),backlog=queue,hotCompletions=done,exercise=[exercise,3,22.5])

# 3. A window state machine with explicit watermark events after every record.
def windows(allowed):
 maximum=-10**9;w=-10**9;state=defaultdict(int);fired=set();out=[];side=[];steps=[]
 for event in data['windows']['arrivals']:
  tm=event['eventTimeMs'];start=tm//10000*10000;end=start+9999
  if w>=end+allowed:side.append(event['id'])
  else:
   state[start]+=event['amountCents']
   if end<=w:out.append([event['id'],start,state[start]])
  maximum=max(maximum,tm);w=maximum-2001
  for start in list(state):
   if start not in fired and w>=start+9999:out.append([event['id'],start,state[start]]);fired.add(start)
   if w>=start+9999+allowed:del state[start]
  steps.append(dict(event=event['id'],watermark=w,state=dict(state),outputs=[x[:] for x in out],side=side[:]))
 for start in sorted(state):
  if start not in fired:out.append(['END',start,state[start]])
 return steps,out,side
steps,out,side=windows(5000)
assert [x['watermark'] for x in steps]==[-1001,1999,6999,6999,9999,9999,15999,15999,15999,20999]
assert out==[['E',0,14000],['L',0,16500],['H',10000,15000],['END',20000,3000]]
assert side==['X'] and sum(x[2] for x in out if x[0] in ['L','H','END'])==34500
_,extended,extended_side=windows(8000)
assert extended[:3]==[['E',0,14000],['L',0,16500],['X',0,17500]] and extended_side==[]
assert sum(e['amountCents'] for e in data['windows']['arrivals'])==35500
assert min(12000,7000)==7000
progress=12000;progress=max(progress,min(16000,9000));assert progress==12000
progress=max(progress,min(16000,14000));assert progress==14000
# Boundary membership and a bridging session, independently as interval union.
assert [s for s in [0,5,10,15] if s<=10<s+10]==[5,10]
intervals=sorted([(v,v+5) for v in [1,4,12,8]])
merged=[]
for a,b in intervals:
 if merged and a<merged[-1][1]:merged[-1][1]=max(merged[-1][1],b)
 else:merged.append([a,b])
assert merged==[[1,17]]
assert sum([8000,14000,16500])==38500
assert sum([8000,-8000,14000,-14000,16500])==16500
report['lesson03']=dict(steps=steps,outputs=out,sideOutput=side,extended=extended,mergedSession=merged)

# 4. Messages and progress are separate inputs. Timers only inspect PENDING.
states={};timers={};paid=[];expired=[];audit=[];seen=set()
inputs=[('create','O1',1000,0),('create','O2',3000,0),('pay','O1',4000,3000),('create','O3',7000,0),('wm','',8001,0),('pay','O2',9000,5000),('create','O4',10000,0),('pay','O3',11000,2000),('wm','',15001,0)]
for kind,key,ts,amount in inputs:
 if kind=='create':states[key]=dict(created=ts,stage='PENDING');timers[key]=ts+5001
 elif kind=='pay':
  st=states[key]
  if st['stage']=='PENDING' and 0<=ts-st['created']<=5000:
   st['stage']='PAID';paid.append((key,amount));timers.pop(key,None)
  else:audit.append((key,amount))
 else:
  for key,deadline in list(timers.items()):
   if deadline<=ts:
    if states[key]['stage']=='PENDING':states[key]['stage']='EXPIRED';expired.append(key)
    del timers[key]
assert paid==[('O1',3000),('O3',2000)] and expired==['O2','O4'] and audit==[('O2',5000)]
assert not timers
# Interval Join has a positive match output, not an automatic timeout policy.
matches=[]
for oid,ct in data['joins']['orders']:
 for pid,pt,amount in data['joins']['payments']:
  if oid==pid and 0<=pt-ct<=5000:matches.append((oid,amount))
assert matches==paid
amounts=[3000,5000,4000,2000,6000,1000,8000,3000]
old={i:[g for g in range(8) if g*2//8==i] for i in range(2)}
new={i:[g for g in range(8) if g*4//8==i] for i in range(4)}
assert old=={0:[0,1,2,3],1:[4,5,6,7]}
assert new=={0:[0,1],1:[2,3],2:[4,5],3:[6,7]}
assert sorted(g for group in new.values() for g in group)==list(range(8))
new_values=[sum(amounts[g] for g in new[i]) for i in range(4)]
assert new_values==[8000,6000,7000,11000] and sum(new_values)==32000
# Stable key + complete value keeps one row under retried output.
rows={}
for k,v in [('A',3000),('A',5000),('A',6000),('A',6000)]:rows[k]=v
assert rows=={'A':6000}
assert 100000*64==6400000
report['lesson04']=dict(states=states,paid=paid,expired=expired,audit=audit,keyGroups=new,amounts=new_values,upsert=rows)

# Check the written evidence, local assets and classroom speech.
counts=[]
for n in range(1,5):
 source=(ROOT/f'slides/lesson-{n:02}/slides.md').read_text()
 notes=re.findall(r'<!--(.*?)-->',source,re.S);pages=[p for p in manifest if p['lesson']==n]
 assert len(notes)==len(pages) and 45<=len(notes)<=55
 assert all(len(note.split('[Sources]')[0].strip())>=90 and '[Sources]' in note for note in notes)
 assert source.count('class="story-figure"')>=len(pages)*.75
 assert sum(p['kind']=='exercise' for p in pages)>=3 and sum(p['kind']=='answer' for p in pages)>=3
 assert '方案' in source and '独立' in source
 for asset in re.findall(r'src="(\./assets/[^\"]+)"',source):
  file=ROOT/f'slides/lesson-{n:02}'/asset;assert file.exists(),file
  svg=ET.fromstring(file.read_text());assert 'Syntax Error' not in file.read_text()
 specs=json.loads((ROOT/f'course/story/lesson-{n:02}.json').read_text())
 assert all((ROOT/f'course/diagrams/lesson-{n:02}'/f'{name}.svg').exists() for name in specs)
 counts.append(len(notes))
# Diagram scene parameters must agree with independently computed progress.
spec=json.loads((ROOT/'course/story/lesson-03.json').read_text())
for name,ix in [('wm-a',0),('wm-b',1),('wm-dc',3),('wm-e',4),('wm-l',5),('wm-g',6),('wm-x',7)]:
 assert spec[name]['wm']==steps[ix]['watermark']
assert json.loads((ROOT/'course/story/lesson-02.json').read_text())['backpressure']['reverse']
report['counts']=counts
outdir=ROOT/'tmp/lessons-01-04-review';outdir.mkdir(exist_ok=True,parents=True)
(outdir/'independent-recalculation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print(json.dumps(dict(slides=counts,localSceneCount=158,paidCents=32000,windowOutputs=out,orderPaidCents=5000,rescaleCents=32000),ensure_ascii=False,indent=2))
