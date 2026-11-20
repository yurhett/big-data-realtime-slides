"""Local editable evidence diagrams. Scene data and SVG are both versioned."""
from pathlib import Path
from html import escape
import json, shutil, unicodedata, re
ROOT=Path(__file__).resolve().parents[1]
BLUE,RED,GREEN,ORANGE,GRAY='#1d4ed8','#dc2626','#15803d','#b45309','#64748b'
def width(s,size):
 return sum(1 if unicodedata.east_asian_width(ch) in 'WF' else .56 for ch in str(s))*size
def wrap(s,maxwidth,size):
 out=[];current=''
 # Keep event IDs, English terms and numbers intact across lines.
 tokens=re.findall(r'[A-Za-z0-9_]+|[^A-Za-z0-9_]',str(s))
 for ch in tokens:
  if current and width(current+ch,size)>maxwidth:
   out.append(current);current=ch
  else:current+=ch
 if current:out.append(current)
 return out or ['']
def t(x,y,s,size=27,c='#20242a',a='start',weight=400):
 rows=wrap(s,930,size) if x==500 and a=='middle' else [str(s)]
 return ''.join(f'<text x="{x}" y="{y-(len(rows)-1-i)*(size+8)}" font-size="{size}" fill="{c}" text-anchor="{a}" font-weight="{weight}">{escape(row)}</text>' for i,row in enumerate(rows))
def line(x,y,xx,yy,c=BLUE,w=3,arrow=False,dash=False):
 return f'<path d="M{x} {y}L{xx} {yy}" fill="none" stroke="{c}" stroke-width="{w}"'+(' stroke-dasharray="7 6"' if dash else '')+(f' marker-end="url(#{c[1:]})"' if arrow else '')+'/>'
def box(x,y,w,h,labels,c=BLUE,size=27,fill=None):
 if isinstance(labels,str):labels=[labels]
 labels=[row for label in labels for row in wrap(label,w-20,size)]
 if len(labels)*size+(len(labels)-1)*16>h: print('TEXT FIT:',w,h,labels)
 fill=fill or {BLUE:'#eff6ff',RED:'#fef2f2',GREEN:'#f0fdf4',ORANGE:'#fff7ed',GRAY:'#f1f5f9'}.get(c,'white')
 b=f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" fill="{fill}" stroke="{c}" stroke-width="2"/>'
 for i,s in enumerate(labels):b+=t(x+w/2,y+h/2+(i-(len(labels)-1)/2)*(size+16)+size*.34,s,size,c,'middle')
 return b
def dot(x,y,label,c=BLUE,size=24):
 return f'<circle cx="{x}" cy="{y}" r="7" fill="{c}"/>'+t(x,y-15,label,size,c,'middle')
def footer(s):return t(500,397,s,27,BLUE,'middle')
def draw(s):
 k=s['type'];b=''
 if k=='flow':
  ls=s['nodes'];n=len(ls);w=270 if n==3 else (360 if n==2 else 205);gap=(930-n*w)/(n-1);xs=[35+i*(w+gap) for i in range(n)]
  for i,(x,labels) in enumerate(zip(xs,ls)):
   c=s.get('colors',[BLUE]*n)[i];b+=box(x,90,w,140,labels,c,27)
   if i<n-1 and s.get('connections',True):
    reverse=s.get('reverse') or (s.get('converge') and i==n-2)
    b+=(line(x+w+gap-6,160,x+w+4,160,arrow=True) if reverse else line(x+w+4,160,x+w+gap-6,160,arrow=True))
    if s.get('arrows'):b+=t(x+w+gap/2,65,s['arrows'][i],23,BLUE,'middle')
  if s.get('detail'):b+=t(500,310,s['detail'],29,s.get('detailColor',BLUE),'middle')
 elif k=='schema':
  b=t(30,40,s.get('label','一条支付事件'),31,BLUE)
  for i,(name,val) in enumerate(s['fields']):
   x=30+(i%3)*325;y=75+(i//3)*132
   b+=t(x,y,name,24,GRAY)+line(x,y+13,x+285,y+13,GRAY,1)+t(x,y+61,val,32,BLUE)
 elif k=='lanes':
  lanes=s['lanes'];n=len(lanes);dy=290/max(n,2)
  for i,row in enumerate(lanes):
   y=48+i*dy
   b+=t(15,y+44,row['name'],27,BLUE)+line(140,y+36,815,y+36,GRAY,2,True)
   vals=row.get('items',[]);w=min(140,620/max(len(vals),1)-12)
   for j,label in enumerate(vals):
    labels=label if isinstance(label,list) else [label]
    b+=box(155+j*(w+12),y,w,83,labels,row.get('color',BLUE),24)
   if 'result' in row:b+=box(835,y,150,83,row['result'],row.get('color',GREEN),26)
  if s.get('detail'):b+=t(500,375,s['detail'],27,BLUE,'middle')
 elif k=='trace':
  b=t(25,30,s.get('heading','输入 → 更新前 → 更新后'),28,BLUE)
  for i,row in enumerate(s['rows']):
   y=62+i*69
   b+=box(25,y,270,52,row[0],BLUE,24)+line(305,y+26,355,y+26,arrow=True)+t(480,y+35,row[1],29,GRAY,'middle')+line(595,y+26,655,y+26,arrow=True)+box(670,y,305,52,row[2],GREEN,28)
 elif k=='route':
  left=s['left'];right=s['right'];n=max(len(left),len(right));dy=260/max(n-1,1)
  for i,v in enumerate(left):b+=box(25,35+i*dy,275,78,v,size=25)
  for j,v in enumerate(right):b+=box(675,35+j*260/max(len(right)-1,1),300,78,v,GREEN,26)
  for i,j,label in s['edges']:
   y=74+i*dy;yy=74+j*260/max(len(right)-1,1)
   b+=line(308,y,665,yy,BLUE,2,True)
   if label:b+=t(488,(y+yy)/2-8,label,23,BLUE,'middle')
 elif k=='parallel':
  cols=s.get('columns',[('S/F',2),('A',4),('O',2)]);xs=[50,445,840] if len(cols)==3 else [30,320,610,840]
  centers=[]
  for j,(name,count) in enumerate(cols):
   b+=t(xs[j]+60,30,name,29,BLUE,'middle');ys=[]
   for i in range(count):
    y=55+i*300/max(count,2);ys.append(y+30);b+=box(xs[j],y,125,60,f'{name}{i}',size=26)
   centers.append(ys)
  for j in range(len(cols)-1):
   for i,y in enumerate(centers[j]):
    mode=s.get('modes',['all']*(len(cols)-1))[j]
    targets=range(len(centers[j+1])) if mode=='all' else [i%len(centers[j+1])]
    for z in targets:b+=line(xs[j]+130,y,xs[j+1]-8,centers[j+1][z],GRAY,1.5,True)
  b+=t(500,365,s.get('detail','每个框是一个并行子任务；连线表示允许的数据路由。'),25,BLUE,'middle')
 elif k=='deploy':
  machines=s['machines'];w=440
  for i,m in enumerate(machines):
   x=35+i*490;b+=f'<rect x="{x}" y="35" width="{w}" height="300" fill="white" stroke="{BLUE}" stroke-width="3"/>'+t(x+20,75,m['name'],29,BLUE)
   b+=t(x+20,112,m.get('process','TaskManager 进程'),25,GRAY)
   for j,slot in enumerate(m['slots']):b+=box(x+20+j*207,145,193,155,[f'Slot {j}']+slot,GREEN,24)
  if s.get('detail'):b+=t(500,375,s['detail'],26,BLUE,'middle')
 elif k=='capacity':
  rates=s['stages'];mx=max(r[1] for r in rates)
  for i,(name,rate) in enumerate(rates):
   y=35+i*82;b+=t(10,y+37,name,26,BLUE)+f'<rect x="260" y="{y}" width="{rate/mx*530}" height="45" fill="'+('#fecaca' if i==s.get('bottleneck',-1) else '#bfdbfe')+'"/>'+t(820,y+34,f'{rate} 条/s',27,RED if i==s.get('bottleneck',-1) else BLUE)
 elif k=='queue':
  b=t(30,35,s['rates'],30,BLUE)
  for i,(time,amount) in enumerate(s['points']):
   x=40+i*325;b+=t(x+135,95,time,28,BLUE,'middle')+box(x,125,270,120,['等待记录',f'{amount} 条'],RED if amount else GREEN,31)
   if i<2:b+=line(x+278,185,x+310,185,arrow=True)
  b+=t(500,325,s['detail'],30,RED,'middle')
 elif k=='times':
  # horizontal event time; vertical processing / arrival order
  b=line(120,45,940,45,arrow=True)+line(120,45,120,335,arrow=True)+t(800,82,'事件时间 / s',24,BLUE)+t(8,374,'到达顺序',24,BLUE)
  for z in range(0,13,2):b+=t(120+z*62,28,z,23,BLUE,'middle')
  pts=s['points']
  for i,(name,et) in enumerate(pts):
   x=120+et*62;y=100+i*66;b+=dot(x,y,f'{name}：{et}s')+t(98,y+8,i+1,24,GRAY,'end')
   if i:
    p=pts[i-1];b+=line(120+p[1]*62,100+(i-1)*66,x,y,GRAY,2,dash=True)
 elif k=='windows':
  scale=s.get('scale',30);x0=180
  b=line(x0,42,950,42,arrow=True)+t(835,25,'事件时间 / s',23,BLUE)
  for z in range(0,26,5):b+=t(x0+z*scale,76,z,23,BLUE,'middle')
  for i,(label,start,end) in enumerate(s['windows']):
   y=112+i*78;b+=t(10,y+28,label,25,BLUE)+f'<rect x="{x0+start*scale}" y="{y}" width="{(end-start)*scale}" height="42" fill="#dbeafe" stroke="{BLUE}"/>'+t(x0+start*scale+6,y+28,f'[{start}, {end})',22,BLUE)
  for i,(label,tm) in enumerate(s.get('events',[])):
   x=x0+tm*scale;b+=line(x,88,x,335,ORANGE,2,dash=True)+t(x,360-i%2*24,label,23,ORANGE,'middle')
 elif k=='watermark':
  b=t(30,32,s.get('input','按本页顺序处理记录，再发出水位线'),27,BLUE)
  for i in range(3):
   x0=70+i*280
   b+=f'<rect x="{x0}" y="100" width="280" height="44" fill="{["#dbeafe","#f1f5f9"][i%2]}"/>'
   b+=t(x0+140,203,f'W{i}：[{i*10},{(i+1)*10})s',23,GRAY,'middle')
  b+=line(70,122,935,122,arrow=True)
  for z in range(0,31,5):b+=t(70+z*28,169,z,25,BLUE,'middle')
  w=s['wm']/1000;x=70+max(0,w)*28
  b+=line(x,87,x,180,RED,5)+t(max(145,x),80,f'W={s["wm"]}ms',27,RED,'middle')
  if s.get('event'):
   name,et=s['event'];ex=70+et*28
   b+=f'<circle cx="{ex}" cy="122" r="8" fill="{ORANGE}"/>'+t(ex+12,130,f'{name} / {et}s',24,ORANGE)
  b+=box(50,225,410,112,s['state'],GREEN,29)+box(540,225,410,112,s['output'],s.get('color',BLUE),29)
 elif k=='multiwm':
  vals=s['inputs']
  for i,(name,v) in enumerate(vals):
   y=35+i*145;b+=box(25,y,300,100,[name,v],size=30)+line(335,y+50,630,180,arrow=True)
  b+=box(650,120,320,120,['算子水位线',s['result']],GREEN,32)
  if s.get('detail'):b+=t(500,345,s['detail'],27,BLUE,'middle')
 elif k=='state':
  labels=s['states'];n=len(labels)
  for i,v in enumerate(labels):
   x=30+i*(940/n);b+=box(x,90,850/n,105,v,s.get('colors',[BLUE]*n)[i],29)
   if i<n-1:
    b+=line(x+850/n+5,145,x+940/n-8,145,arrow=True)
    b+=t(x+895/n,65,s.get('actions',['']*n)[i],23,BLUE,'middle')
  if s.get('event'):b+=box(155,260,690,75,s['event'],ORANGE,28)
 elif k=='join':
  b=line(170,335,950,335,arrow=True)+t(830,380,'事件时间 / s',23,BLUE)
  for i,(oid,ct,pt) in enumerate(s['rows']):
   y=60+i*75;xx=170+ct*43;b+=t(10,y+9,oid,27,BLUE)+line(170,y,950,y,GRAY,1)+line(xx,y,xx+5*43,y,BLUE,12)+dot(xx,y,f'创建{ct}s',BLUE,21)
   if pt is not None:
    col=GREEN if s.get('valid',[0<=v[2]-v[1]<=5 if v[2] is not None else False for v in s['rows']])[i] else RED
    b+=f'<circle cx="{170+pt*43}" cy="{y}" r="8" fill="{col}"/>'+t(180+pt*43,y+30,f'支付{pt}s',22,col)
 elif k=='changes':
  b=t(30,32,s.get('heading','每条消息改变哪一行？'),29,BLUE)
  for i,(msg,old,new) in enumerate(s['rows']):
   y=65+i*85;b+=t(25,y+35,msg,27,BLUE)+line(315,y+25,365,y+25,arrow=True)+box(380,y,245,58,old,GRAY,27)+line(635,y+25,685,y+25,arrow=True)+box(700,y,270,58,new,GREEN,28)
 elif k=='groups':
  for i,(label,vals) in enumerate(s['old']):
   y=45+i*150;b+=box(25,y,290,105,[label,' '.join(map(str,vals))],size=28)
  for i,(label,vals) in enumerate(s['new']):
   y=25+i*80;b+=box(690,y,285,67,[label,' '.join(map(str,vals))],GREEN,23)
  for i,j in s['edges']:b+=line(325,97+i*150,680,59+j*80,GRAY,2,True)
 else:raise ValueError(k)
 if s.get('footer'):b+=footer(s['footer'])
 return b

def render_all():
 for n in range(1,5):
  folder=ROOT/f'course/diagrams/lesson-{n:02}';folder.mkdir(exist_ok=True)
  dest=ROOT/f'slides/lesson-{n:02}/assets/story';dest.mkdir(parents=True,exist_ok=True)
  specs=json.loads((ROOT/f'course/story/lesson-{n:02}.json').read_text())
  for name,spec in specs.items():
   markers=''.join(f'<marker id="{c[1:]}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L10 5L0 10Z" fill="{c}"/></marker>' for c in [BLUE,RED,GREEN,ORANGE,GRAY])
   svg=f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 425" role="img"><defs>{markers}</defs><rect width="1000" height="425" fill="white"/><g font-family="PingFang SC,Microsoft YaHei,sans-serif">{draw(spec)}</g></svg>'
   (folder/f'{name}.svg').write_text(svg);(dest/f'{name}.svg').write_text(svg)
  print(f'Lesson {n:02}: {len(specs)} editable evidence scenes.')
if __name__=='__main__':render_all()
