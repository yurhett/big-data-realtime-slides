"""Replay the lesson's fault cases independently of its figure authoring code."""
from pathlib import Path
import json, re, xml.etree.ElementTree as ET

root=Path(__file__).resolve().parents[1]
data=json.loads((root/'course/data/cases.json').read_text())
amount={e['eventId']:e['amountCents'] for e in data['events'] if e['status']=='PAID'}
logs={'A':[(100,'E01'),(101,'E03'),(102,'E05'),(103,'E07')],
      'B':[(200,'E02'),(201,'E04'),(202,'E06'),(203,'E08')]}
snapshot_ids={'E01','E02','E03','E04'}
snapshot_sum=sum(amount[e] for e in snapshot_ids)
assert snapshot_sum==14000

def replay(next_positions):
    ids=[e for lane in logs for offset,e in logs[lane] if offset>=next_positions[lane]]
    return ids,snapshot_sum+sum(amount[e] for e in ids)

correct,value=replay({'A':102,'B':202})
assert set(correct)=={'E05','E06','E07','E08'} and value==32000
assert replay({'A':103,'B':202})[1]==26000
assert replay({'A':101,'B':202})[1]==34000
before=['E01','E02','E03','E04','E05','E06']
appended=before+correct
assert len(appended)==10 and sum(amount[e] for e in appended)==39000
idempotent={e:amount[e] for e in appended}
assert len(idempotent)==8 and sum(idempotent.values())==32000
assert sorted(e for e in set(appended) if appended.count(e)==2)==['E05','E06']
assert 39000-32000==amount['E05']+amount['E06']==7000

# Gate a channel when its barrier reaches the operator. Other channels continue.
state=0;seen=[];blocked=set()
for lane,e in [('A','E01'),('B','E02'),('A','E03'),('A','barrier'),('B','E04'),('B','barrier')]:
    if e=='barrier':blocked.add(lane)
    else:
        assert lane not in blocked
        seen.append(e);state+=amount[e]
assert state==14000 and blocked=={'A','B'}
operator_ids={'E01','E02','E03'};channel_ids={'E04'}
assert operator_ids.isdisjoint(channel_ids)
assert operator_ids|channel_ids==snapshot_ids
assert sum(amount[e] for e in operator_ids)==10000
assert sum(amount[e] for e in channel_ids)==4000
assert 10000+4000+sum(amount[e] for e in correct)==32000
visible=snapshot_sum
uncommitted=amount['E05']+amount['E06']
assert (visible,uncommitted)==(14000,7000)
uncommitted=0 # abort before the associated checkpoint completes
assert visible==14000
visible+=sum(amount[e] for e in correct) # recovery transaction successfully commits
assert visible==32000

scenes=json.loads((root/'course/diagrams/lesson-05/scenes.json').read_text())
for name,s in scenes.items():
    assert sum(amount[e] for e in s['consumed'])==s['sum'],name
    if s['blocked']:assert 'E05' not in s['consumed']
    if s.get('snapshot'):assert set(s['consumed'])==snapshot_ids
    svg=ET.fromstring((root/f'slides/lesson-05/assets/story/{name}.svg').read_text())
    assert int(svg.attrib['data-sum'])==s['sum']
    assert set(svg.attrib['data-consumed'].split(','))-{''}==set(s['consumed'])
for name,expected in [('replay-correct',32000),('replay-skipped',26000),('replay-early',34000)]:
    svg=ET.fromstring((root/f'slides/lesson-05/assets/story/{name}.svg').read_text())
    assert int(svg.attrib['data-final'])==expected

source=(root/'slides/lesson-05/slides.md').read_text()
notes=re.findall(r'<!--(.*?)-->',source,re.S)
manifest=[p for p in json.loads((root/'course/manifest.json').read_text()) if p['lesson']==5]
assert len(notes)==len(manifest)==54
for page,note in zip(manifest,notes):
    spoken=note.split('[Sources]')[0]
    assert len(spoken.strip())>=100,(page['page'],'Spoken explanation missing')
    assert not any(s in spoken for s in ['按行读每个分区','不把下一条位置误解','不要求编程','暂时不用理解'])
for asset in re.findall(r'src="(\./assets/[^\"]+)"',source):
    assert (root/'slides/lesson-05'/asset).exists(),asset
assert len(re.findall('class="story-figure"',source))>=40
assert all(s in source for s in ['幂等','源位置','共享持久','独立设计练习','7000','26000','39000'])
print(json.dumps({'slides':54,'illustratedPages':source.count('class="story-figure"'),
                  'alignedSnapshot':state,'unalignedOperatorState':10000,'channelState':4000,
                  'correct':32000,'latePosition':26000,'earlyPosition':34000,
                  'appendRows':10,'appendCents':39000,'idempotentRows':8,'idempotentCents':32000,
                  'transactionVisibleCents':visible},ensure_ascii=False,indent=2))
