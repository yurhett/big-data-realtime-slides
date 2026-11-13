"""Independently recompute teaching examples and inspect the authored decks."""
from pathlib import Path
from collections import defaultdict
import json, re, sys
ROOT=Path(__file__).resolve().parents[1]
d=json.loads((ROOT/'course/data/cases.json').read_text())
manifest=json.loads((ROOT/'course/manifest.json').read_text())
paid=[e for e in d['events'] if e['status']=='PAID']
unique={e['eventId']:e for e in paid}
assert (len(d['events']),len(paid),len(unique))==(12,9,8)
assert sum(e['amountCents'] for e in paid)==34000
assert sum(e['amountCents'] for e in unique.values())==32000
users=defaultdict(int)
for e in unique.values():users[e['userId']]+=e['amountCents']
assert dict(users)=={'A':6000,'B':14000,'C':12000}
for p in (0,1):
    offsets=[e['offset'] for e in d['events'] if e['partition']==p]
    assert offsets==sorted(set(offsets))
params=d['windows']
def simulate(lateness):
    wm=-10**18;maximum=-10**18;state={};fired=set();out=[];side=[];trace=[]
    for e in params['arrivals']:
        start=e['eventTimeMs']//params['sizeMs']*params['sizeMs']
        endmax=start+params['sizeMs']-1
        if wm>=endmax+lateness:side.append(e['id'])
        else:
            state[start]=state.get(start,0)+e['amountCents']
            if wm>=endmax:out.append((e['id'],start,state[start]))
        maximum=max(maximum,e['eventTimeMs']);wm=maximum-params['outOfOrderMs']-1;trace.append(wm)
        for s in list(state):
            if s not in fired and wm>=s+params['sizeMs']-1:
                out.append((e['id'],s,state[s]));fired.add(s)
            if wm>=s+params['sizeMs']-1+lateness:del state[s]
    for s in sorted(state):
        if s not in fired:out.append(('END',s,state[s]))
    return out,side,trace
out,side,trace=simulate(5000)
assert out==[('E',0,14000),('L',0,16500),('H',10000,15000),('END',20000,3000)]
assert side==['X']
assert trace==[-1001,1999,6999,6999,9999,9999,15999,15999,15999,20999]
assert simulate(0)[1]==['L','X']
assert simulate(8000)[0][:3]==[('E',0,14000),('L',0,16500),('X',0,17500)]
base=[e for e in params['arrivals'] if e['id'] not in ('L','X')]
window_sums=[sum(e['amountCents'] for e in base if s<=e['eventTimeMs']<s+10000) for s in (0,10000,20000)]
assert window_sums==[14000,15000,3000]
sliding=[sum(e['amountCents'] for e in base if s<=e['eventTimeMs']<s+10000) for s in range(0,25000,5000)]
assert sliding==[14000,12000,15000,12000,3000]
intervals=[]
for t in sorted([1000,4000,8000,12000,16000]):
    if intervals and t<intervals[-1][1]:intervals[-1][1]=max(intervals[-1][1],t+5000)
    else:intervals.append([t,t+5000])
assert intervals==[[1000,21000]]
assert sum([8000,6000,2500])==16500
assert sum([8000,14000,16500])==38500
assert sum([8000,-8000,14000,-14000,16500])==16500
r=d['runtime'];arrival=r['sourceRate']*r['paidFraction'];capacity=r['parallelism']*r['perTaskCapacity']
assert arrival==4500 and capacity==4000
assert (arrival-capacity)*r['seconds']==10000 and 10000/capacity==2.5
assert (3600-3000)*15==9000 and 9000/3000==3
assert r['hotKeyRate']-r['perTaskCapacity']==2000
assert sum(min(x,r['perTaskCapacity']) for x in (3000,500,500,500))==2500
assert 2+2+4+2==10 and 2+4+2==8
j=d['joins'];matches=[]
for oid,t in j['orders']:
    for pid,pt,amount in j['payments']:
        if oid==pid and 0<=pt-t<=j['boundMs']:matches.append((oid,amount))
assert matches==[('O1',3000),('O3',2000)]
assert sum(v for _,v in matches)==5000
assert {oid:t+j['boundMs']+1 for oid,t in j['orders']}=={'O1':6001,'O2':8001,'O3':12001,'O4':15001}
assert [[g for g in range(8) if g*p//8==i] for p,i in [(2,0),(2,1),(4,0),(4,1),(4,2),(4,3)]]==[[0,1,2,3],[4,5,6,7],[0,1],[2,3],[4,5],[6,7]]
assert 100000*64==6400000
c=d['checkpoint'];before=sum(unique[x]['amountCents'] for x in c['beforeFailure']);replay=sum(unique[x]['amountCents'] for x in c['replay'])
assert (c['sumCents'],before,replay)==(14000,7000,18000)
assert c['sumCents']+replay==32000 and c['sumCents']+before+replay==39000
assert 32000-unique['E05']['amountCents']==26000
assert c['nextOffsets']=={'0':102,'1':202}
counts=[]
for n in range(1,6):
    source=(ROOT/f'slides/lesson-{n:02}/slides.md').read_text()
    notes=re.findall(r'<!--(.*?)-->',source,re.S)
    pages=[x for x in manifest if x['lesson']==n]
    assert len(notes)==len(pages)
    assert 45<=len(notes)<=55,(n,len(notes))
    assert all('[Sources]' in note and len(note.split('[Sources]')[0].strip()) >= (25 if page['kind']=='section' else 50) for note,page in zip(notes,pages))
    assert sum(p['kind']=='exercise' for p in pages)>=2
    assert sum(p['kind']=='answer' for p in pages)>=2
    visible=re.sub(r'<!--.*?-->','',source,flags=re.S)
    for diagram in re.findall(r'```mermaid\n(.*?)\n```',visible,re.S):
        assert '&#91;' not in diagram,(n,'Escaped Mermaid node bracket')
        assert not re.search(r'^- ',diagram,re.M),(n,'List swallowed by diagram fence')
    assert '```-' not in visible,(n,'Missing newline after code fence')
    for phrase in ['不要求编程','暂时不用理解','只是一个简单示意','小本子','不是特定公司的']:
        assert phrase not in visible,(n,phrase)
    for asset in re.findall(r'\./assets/figures/([\w-]+\.svg)',source):
        p=ROOT/f'slides/lesson-{n:02}/assets/figures'/asset
        assert p.exists(),p
        assert 'Syntax Error' not in p.read_text(),p
    counts.append(len(notes))
assert all(not (ROOT/f'slides/lesson-{n:02}').exists() for n in (6,7,8))
assert not (ROOT/'assessment').exists()
assert not (ROOT/'_ref/lesson-drafts-20261030').exists()
assert len(list((ROOT/'course/diagrams').glob('*.puml')))>=6
if '--built' in sys.argv:
    index=(ROOT/'dist/index.html').read_text()
    assert all(f'lesson-{n:02}/' in index for n in range(1,6))
    assert all(f'lesson-{n:02}/' not in index for n in (6,7,8))
    assert '/chapter-' not in index
    assert (ROOT/'dist/reference/index.html').exists()
    assert len(list((ROOT/'dist').glob('lesson-*/index.html')))==5
    assert not list((ROOT/'dist').rglob('*student-exam*'))
print(json.dumps(dict(slides=counts,totalSlides=sum(counts),uniquePaid=8,totalCents=32000,users=dict(users),watermarks=trace,windowOutputs=out,lateSideOutput=side,windowSums=window_sums,slidingSums=sliding,backlog=10000,joinMatches=matches,recoveredCents=32000,blindSinkCents=39000),ensure_ascii=False,indent=2))
