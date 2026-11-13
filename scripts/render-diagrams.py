"""Render the authored UML locally and copy deterministic technical figures into decks."""
from pathlib import Path
import subprocess, shutil, hashlib, urllib.request, json, re
import xml.etree.ElementTree as ET
from html import escape

ROOT=Path(__file__).resolve().parents[1]
FIG=ROOT/'course/diagrams'
VERSION='1.2025.0'
JAR=ROOT/f'tmp/tools/plantuml-{VERSION}.jar'

def technical_figures():
    def text(x,y,s,size=24,color='#222',anchor='start'):
        return f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" text-anchor="{anchor}">{escape(str(s))}</text>'
    def line(x1,y1,x2,y2,color='#1d4ed8',width=3,dash=False,arrow=False):
        return f'<path d="M{x1} {y1}L{x2} {y2}" fill="none" stroke="{color}" stroke-width="{width}"'+(' stroke-dasharray="8 6"' if dash else '')+(' marker-end="url(#arrow)"' if arrow else '')+'/>'
    def box(x,y,w,h,label,fill='#eff6ff'):
        return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="3" fill="{fill}" stroke="#1d4ed8" stroke-width="2"/>'+text(x+w/2,y+h/2+8,label,24,anchor='middle')
    def save(name,body,height=360):
        (FIG/f'{name}.svg').write_text(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 {height}" role="img"><defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10Z" fill="#1d4ed8"/></marker></defs><rect width="1000" height="{height}" fill="white"/><g font-family="PingFang SC,Microsoft YaHei,sans-serif">{body}</g></svg>')
    b=line(100,45,920,45,arrow=True)+line(100,45,100,335,arrow=True)
    b+=text(800,80,'事件时间 / s',21)+text(15,360,'处理时间 / s',21)
    for t in range(0,13,2):b+=line(100+t*60,40,100+t*60,50,width=1)+text(100+t*60,28,t,20,anchor='middle')
    points=[('A',1,2),('B',4,5),('D',9,10),('C',7,11)]
    for (i,et,pt),(j,et2,pt2) in zip(points,points[1:]):b+=line(100+et*60,65+pt*20,100+et2*60,65+pt2*20,'#94a3b8',2,dash=True)
    for i,et,pt in points:
        x=100+et*60;y=65+pt*20
        b+=f'<circle cx="{x}" cy="{y}" r="8" fill="#1d4ed8"/>'+text(x+15,y-10,f'{i} ({et}s, {pt}s)',24)+text(80,y+7,pt,20,anchor='end')
    save('event-time',b,390)
    b=''
    for row,(oid,created,pay) in enumerate([('O1',1,4),('O2',3,9),('O3',7,11),('O4',10,None)]):
        y=60+row*70;x=180+created*40
        b+=text(35,y+8,oid)+line(170,y,900,y,'#d5dbe4',2)+line(x,y,x+200,y,'#bfdbfe',14)
        b+=f'<circle cx="{x}" cy="{y}" r="7" fill="#1d4ed8"/>'+text(x,y-17,f'创建{created}s',20)
        if pay is not None:
            px=180+pay*40;color='#b45309' if pay-created>5 else '#1d4ed8'
            b+=f'<circle cx="{px}" cy="{y}" r="9" fill="{color}"/>'+text(px+10,y+29,f'支付{pay}s',20,color)
    b+=line(180,320,900,320,arrow=True)
    for t in range(0,17,2):b+=text(180+t*40,350,t,20,anchor='middle')
    b+=text(790,382,'事件时间 / s',21)
    save('join-time',b,400)
    b=text(25,100,'P1')+text(25,260,'P2')+line(90,90,950,90,arrow=True)+line(90,250,950,250,arrow=True)
    b+=line(250,90,450,250,arrow=True)+text(200,175,'m1：已接收',24)
    b+=line(500,90,740,250,arrow=True)+text(650,155,'m2：在途',24)
    b+=line(570,35,570,305,'#b45309',3,dash=True)+text(570,338,'一致性切面',24,'#b45309',anchor='middle')
    b+=text(100,380,'切面内保存局部状态，并记录m2对应的通道状态。',24)
    save('consistent-cut',b,405)
    for name,aligned in [('barrier-start',False),('barrier-aligned',True)]:
        b=text(15,55,'分区A')+text(15,205,'分区B')
        for y,events in [(70,['E01:3000','E03:2000','E05:6000']),(220,['E02:5000','E04:4000','E06:1000'])]:
            for x,label in [(105,events[0]),(280,events[1]),(585,events[2])]:b+=box(x,y,150,55,label,'#f1f5f9' if aligned and x<500 else '#eff6ff')
            b+=line(255,y+27,277,y+27,arrow=True)+line(430,y+27,450,y+27,arrow=True)
            b+=box(455,y,100,55,'n', '#dbeafe')+line(555,y+27,582,y+27,arrow=True)
            b+=text(105,y+90,'已纳入n' if aligned else '屏障前的记录',22,'#666')+text(585,y+90,'n之后继续处理',22,'#666')
        b+=box(785,110,195,115,'Sum = 14000' if aligned else 'Sum', '#eff6ff')
        b+=text(785,260,'A.next = 102',22)+text(785,295,'B.next = 202',22)
        b+=text(105,350,'每行从左到右表示分区记录顺序；n是检查点标识。',22,'#666')
        save(name,b,380)

def render():
    technical_figures()
    JAR.parent.mkdir(parents=True,exist_ok=True)
    if not JAR.exists():
        urllib.request.urlretrieve(f'https://repo.maven.apache.org/maven2/net/sourceforge/plantuml/plantuml/{VERSION}/plantuml-{VERSION}.jar',JAR)
    expected='f37f171fe61ba4522ea469ec5732b8b64defd6b2bf548b75bb94881b5001c1da'
    if hashlib.sha256(JAR.read_bytes()).hexdigest()!=expected:
        raise RuntimeError('PlantUML dependency checksum mismatch')
    subprocess.run(['java','-Djava.awt.headless=true','-jar',str(JAR),'-charset','UTF-8','-tsvg',str(FIG/'*.puml')],check=True)
    # Smetana can place self-transition labels above its reported viewport.
    # Expand the viewport to include the actual text bounds, retaining vectors.
    for source in FIG.glob('*.puml'):
        svg=source.with_suffix('.svg')
        content=svg.read_text()
        root=ET.fromstring(content)
        x,y,w,h=map(float,root.attrib['viewBox'].split())
        left,top,right,bottom=x,y,x+w,y+h
        for node in root.iter('{http://www.w3.org/2000/svg}text'):
            tx=float(node.attrib.get('x',0));ty=float(node.attrib.get('y',0))
            size=float(node.attrib.get('font-size',22))
            width=float(node.attrib.get('textLength',0))
            left=min(left,tx-8);top=min(top,ty-size-8)
            right=max(right,tx+width+8);bottom=max(bottom,ty+8)
        if (left,top,right,bottom)!=(x,y,x+w,y+h):
            content=re.sub(r'viewBox="[^"]+"',f'viewBox="{left:g} {top:g} {right-left:g} {bottom-top:g}"',content,count=1)
            content=re.sub(r'height="[\d.]+px"',f'height="{bottom-top:g}px"',content,count=1)
            content=re.sub(r'width="[\d.]+px"',f'width="{right-left:g}px"',content,count=1)
            content=re.sub(r'style="width:[^"]*"',f'style="background:#FFFFFF;"',content,count=1)
            svg.write_text(content)
    for n in range(1,6):
        folder=ROOT/f'slides/lesson-{n:02}'
        dest=folder/'assets/figures';dest.mkdir(parents=True,exist_ok=True)
        source=(folder/'slides.md').read_text()
        for name in re.findall(r'\./assets/figures/([\w-]+\.svg)',source):
            if name=='cover.svg':continue
            shutil.copyfile(FIG/name,dest/name)
        title=['数据流与计算模型','主从架构与并行执行','时间、窗口与结果修正','状态与流式查询','分布式快照与故障恢复'][n-1]
        (dest/'cover.svg').write_text(f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 720"><rect width="1280" height="720" fill="#fff"/><path d="M80 150H1200" stroke="#1d4ed8" stroke-width="5"/><g font-family="PingFang SC,Microsoft YaHei,sans-serif"><text x="80" y="110" fill="#1d4ed8" font-size="30">分布式实时计算 · 第{n}讲</text><text x="80" y="330" fill="#222" font-size="64">{title}</text><text x="80" y="420" fill="#666" font-size="30">Dataflow 模型 · Flink 2.3</text><text x="80" y="620" fill="#666" font-size="26">135 分钟</text></g></svg>''')
    print('Rendered UML and copied technical SVGs into five decks.')

if __name__=='__main__':render()
