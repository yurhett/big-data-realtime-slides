"""Editable technical scenes for the fault-recovery lesson (SVG, no remote renderer)."""
from pathlib import Path
from html import escape
import json, shutil

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'course/diagrams/lesson-05'
BLUE, RED, GREEN, ORANGE, GRAY = '#1d4ed8', '#dc2626', '#15803d', '#b45309', '#64748b'
AMOUNTS = {e['eventId']: e['amountCents'] for e in json.loads((ROOT/'course/data/cases.json').read_text())['events'] if e['status']=='PAID'}

def text(x,y,s,size=27,color='#20242a',anchor='start',weight=400):
    return f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" text-anchor="{anchor}" font-weight="{weight}">{escape(str(s))}</text>'
def line(x,y,xx,yy,color=BLUE,width=3,arrow=False,dash=False):
    return f'<path d="M{x} {y}L{xx} {yy}" fill="none" stroke="{color}" stroke-width="{width}"'+(' stroke-dasharray="8 6"' if dash else '')+(f' marker-end="url(#{color[1:]})"' if arrow else '')+'/>'
def box(x,y,w,h,lines,color=BLUE,fill='#eff6ff',size=27):
    if isinstance(lines,str): lines=[lines]
    b=f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="5" fill="{fill}" stroke="{color}" stroke-width="2"/>'
    for i,s in enumerate(lines):b+=text(x+w/2,y+h/2+(i-(len(lines)-1)/2)*(size+10)+size*.34,s,size,color,anchor='middle')
    return b
def arrow(x,y,xx,yy,label='',color=BLUE):
    return line(x,y,xx,yy,color,arrow=True)+(text((x+xx)/2,(y+yy)/2-13,label,24,color,'middle') if label else '')
def cross(x,y,w=92,h=67):
    return line(x,y,x+w,y+h,RED,5)+line(x+w,y,x,y+h,RED,5)
def save(name,body,height=420,attrs=''):
    OUT.mkdir(parents=True,exist_ok=True)
    markers=''.join(f'<marker id="{c[1:]}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L10 5L0 10Z" fill="{c}"/></marker>' for c in [BLUE,RED,GREEN,ORANGE,GRAY])
    svg=f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 {height}" role="img" {attrs}><defs>{markers}</defs><rect width="1000" height="{height}" fill="white"/><g font-family="PingFang SC,Microsoft YaHei,sans-serif">{body}</g></svg>'
    (OUT/f'{name}.svg').write_text(svg)

def flow(name,labels,arrows=None,extra='',height=380):
    n=len(labels);w=250 if n==3 else 190;gap=(930-n*w)/(n-1);x0=35
    b=''
    for i,ls in enumerate(labels):
        x=x0+i*(w+gap);b+=box(x,85,w,125,ls)
        if i<n-1:
            label=(arrows or ['']*n)[i]
            b+=arrow(x+w+5,148,x+w+gap-6,148)
            if label:b+=text(x+w+gap/2,66,label,24,BLUE,'middle')
    save(name,b+extra,height)

def cuts(name,case):
    c1,c2=(300,750) if case=='orphan' else (510,540)
    b=text(25,105,'发送方',26)+text(25,260,'接收方',26)
    for y,c in [(95,c1),(250,c2)]:
        b+=f'<rect x="140" y="{y-30}" width="{c-140}" height="60" fill="#dbeafe"/>'
        b+=arrow(140,y,950,y)+text(150,y-48,'已纳入快照',23,BLUE)
    b+=line(380,95,660,250,BLUE,3,True)+text(560,163,'m：4000分',27,BLUE)
    b+='<path d="M%d 30V170H%dV315" fill="none" stroke="%s" stroke-width="4" stroke-dasharray="9 6"/>'%(c1,c2,ORANGE)
    b+=text(c1+12,38,'快照边界',23,ORANGE)
    for x,y,l in [(380,95,'发送m'),(660,250,'接收m')]:
        b+=f'<circle cx="{x}" cy="{y}" r="9" fill="{BLUE}"/>'+text(x+13,y+37,l,24)
    if case=='orphan':
        b+=text(80,365,'快照中已有接收，却没有对应发送：因果关系断裂。',29,RED)
    elif case=='lost':
        b+=box(100,330,800,65,'局部状态：已发送、未接收；通道内容：未保存',RED,'#fef2f2',26)
    else:
        b+=box(100,330,800,65,'局部状态：已发送、未接收；通道内容：保存 m',GREEN,'#f0fdf4',26)
    save(name,b)

SCENES={
 'barrier-inject':dict(consumed=[],sum=0,barA=360,barB=360,blocked=False),
 'barrier-a-first':dict(consumed=['E01','E02','E03'],sum=10000,barA=650,barB=360,blocked=True),
 'barrier-wait':dict(consumed=['E01','E02','E03'],sum=10000,barA=650,barB=360,blocked=True,focus='E04'),
 'barrier-b-record':dict(consumed=['E01','E02','E03','E04'],sum=14000,barA=650,barB=360,blocked=True,focus='E04'),
 'barrier-aligned':dict(consumed=['E01','E02','E03','E04'],sum=14000,barA=650,barB=650,blocked=True,snapshot=True),
 'barrier-resume':dict(consumed=['E01','E02','E03','E04'],sum=14000,barA=980,barB=980,blocked=False,snapshot=True),
}
def pipeline(name,scene):
    b=text(120,25,'记录沿箭头向右流动；右侧的旧记录先到达算子',25,GRAY)
    records=[('A',108,[('E07',103,120),('E05',102,240),('E03',101,420),('E01',100,540)]),('B',254,[('E08',203,120),('E06',202,240),('E04',201,420),('E02',200,540)])]
    for lane,y,items in records:
        b+=text(5,y+10,f'分区{lane}',25)+line(100,y+13,712,y+13,GRAY,3,True)
        for eid,offset,x in items:
            consumed=eid in scene['consumed'];color=GRAY if consumed else BLUE;fill='#f1f5f9' if consumed else '#eff6ff'
            if scene.get('focus')==eid:color=ORANGE;fill='#fff7ed'
            b+=f'<g data-event="{eid}" data-consumed="{str(consumed).lower()}">'+box(x,y-38,99,93,[eid,str(AMOUNTS[eid])+'分',str(offset)],color,fill,22)+'</g>'
            if consumed:b+=text(x+50,y+82,'已计入',22,GRAY,'middle')
        bx=scene['bar'+lane]
        if bx<900:
            b+=line(bx,y-48,bx,y+62,RED,6)+text(bx,y-61,'n',30,RED,'middle',600)
        if lane=='A' and scene['blocked']:
            b+=text(240,y+92,'A 暂停消费',24,RED,'middle')
    b+=box(735,115,250,150,['Sum 工作状态',str(scene['sum'])+' 分'],BLUE,'#eff6ff',32)
    if scene.get('focus')=='E04':
        b+=f'<path d="M470 310V350H700V254H732" fill="none" stroke="{ORANGE}" stroke-width="3" marker-end="url(#{ORANGE[1:]})"/>'
        b+=text(590,383,'E04：+4000' if 'E04' in scene['consumed'] else '下一条 E04',24,ORANGE,'middle')
    if scene.get('snapshot'):
        b+=box(520,365,460,56,'快照 n：14000；A.next=102，B.next=202',GREEN,'#f0fdf4',23)
        b+=arrow(860,270,860,358,color=GREEN)+text(840,322,'保存 n',24,GREEN,'end')
    else:
        b+=text(120,412,'事件编号 / 金额 / Offset；灰色表示已经计入工作状态。',22,GRAY)
    if name=='barrier-resume':b+=text(710,53,'Barrier n 已向下游传播',24,RED)
    save(name,b,440,f'data-sum="{scene["sum"]}" data-consumed="{",".join(scene["consumed"])}"')

def replay(name,skip=None,early=False):
    seq=['E05','E06','E07','E08'];start=14000
    b=box(20,28,250,80,['恢复快照 n','14000 分'],GREEN,'#f0fdf4',27)
    b+=text(330,53,'A.next = '+('103' if skip else ('101' if early else '102')),28,RED if skip or early else BLUE)+text(660,53,'B.next = 202',28,BLUE)
    b+=arrow(270,70,305,70)
    if early:seq=['E03']+seq
    w=170 if len(seq)==4 else 150;gap=55 if len(seq)==4 else 40;x0=35
    vals=[]
    for i,eid in enumerate(seq):
        x=x0+i*(w+gap);color=RED if eid==skip or (early and eid=='E03') else BLUE
        b+=box(x,150,w,100,[eid,f'+{AMOUNTS[eid]} 分'],color,'#fef2f2' if color==RED else '#eff6ff',29)
        if eid==skip:b+=cross(x,150,w,100)
        else:start+=AMOUNTS[eid]
        vals.append(start)
        b+=text(x+w/2,300,f'{start} 分',30,color,'middle',600)
        if i<len(seq)-1:b+=arrow(x+w+5,200,x+w+gap-5,200)
    b+=text(500,380,'跳过 E05 → 少计 6000 分' if skip else ('重读 E03 → 多计 2000 分' if early else '每个箭头：读入记录 → 更新同一份累计状态'),29,RED if skip or early else BLUE,'middle')
    save(name,b,420,f'data-final="{start}"')

def ledgers(name,mode):
    b=text(30,30,'Flink 算子',30,BLUE)+text(625,30,'外部 MySQL',30,BLUE)
    if mode=='before':
        b+=box(30,80,290,145,['工作状态','21000 分'],size=33)
        b+=box(30,290,330,90,['已完成快照 n：14000','下一条：A102 / B202'],GREEN,'#f0fdf4',25)
        b+=box(600,80,360,115,['前四条：14000','E05 + E06：7000'],size=29)+text(780,243,'6 行；合计 21000 分',29,BLUE,'middle')
        b+=arrow(330,150,580,150,'故障前已写出')
    elif mode=='crash':
        b+=box(30,80,290,145,['进程故障','工作状态丢失'],GRAY,'#f1f5f9',30)+cross(30,80,290,145)
        b+=box(30,290,330,90,['共享存储中的快照 n','14000；A102 / B202'],GREEN,'#f0fdf4',25)
        b+=box(600,80,360,145,['数据库仍保留','6 行；21000 分'],BLUE,'#eff6ff',30)
        b+=text(480,260,'内存丢失 ≠ 数据库回滚',31,RED,'middle',600)
    else:
        b+=box(30,90,300,125,['恢复后重新计算','32000 分'],size=34)
        rows=[('前四条',14000,BLUE),('故障前 E05、E06',7000,ORANGE),('重放 E05—E08',18000,RED)]
        for i,(l,v,c) in enumerate(rows):b+=box(600,65+i*87,360,70,[f'{l}：{v} 分'],c,'#fff7ed' if c==ORANGE else '#eff6ff',26)
        b+=arrow(340,150,570,150,'再次追加')+text(780,365,'10 行；39000 分',34,RED,'middle',600)
        b+=text(30,330,'内部回滚到 14000',27,BLUE)+text(30,375,'外部保留已写的 7000',27,ORANGE)
    save(name,b)

def render():
    flow('shop',[['前台收款','产生支付小票'],['后厨累计','记住已处理小票'],['营业额账本','保存对外结果']],['小票','金额'],extra=text(500,290,'突然停电：手上的累计、小票位置、账本能否对上？',30,RED,'middle'))
    flow('system',[['输入日志 Source','保留支付记录与位置'],['计算任务 Flink','内存中累计金额'],['结果库 Sink','持久保存业务结果']],['记录','结果'],extra=arrow(500,215,500,262,color=GREEN)+box(260,270,480,95,['共享持久存储','程序退出后，仍可读取快照'],GREEN,'#f0fdf4',26))
    b=''
    for y,lane,rows in [(75,'A',[('E01',100),('E03',101),('E05',102),('E07',103)]),(245,'B',[('E02',200),('E04',201),('E06',202),('E08',203)])]:
        b+=text(15,y+40,lane,32,BLUE)
        for i,(eid,offset) in enumerate(rows):
            x=100+i*225;b+=box(x,y,190,100,[eid+' / '+str(offset),str(AMOUNTS[eid])+' 分'],size=28)
            if i<3:b+=arrow(x+195,y+50,x+220,y+50)
    b+=text(500,400,'本页从左向右列出日志顺序；每个分区的 Offset 独立递增。',25,GRAY,'middle')
    save('input-log',b,430)
    b=box(30,50,285,115,['过去完成 n','工作状态：14000'],size=29)+box(660,50,310,115,['继续计算后','工作状态：21000'],size=29)
    b+=arrow(325,108,650,108,'继续处理 E05 / E06')
    b+=arrow(210,175,420,255,'保存 n',GREEN)
    b+=box(330,265,440,90,['持久快照 n','14000；A102 / B202'],GREEN,'#f0fdf4',28)
    b+=text(500,410,'故障后，从 n 恢复并重放快照后的记录。',29,BLUE,'middle')
    save('memory',b,440)
    cuts('cut-orphan','orphan');cuts('cut-lost','lost');cuts('cut-complete','complete')
    flow('marker',[['先保存','本进程的局部状态'],['再传播 Marker','给输出通道划边界'],['记录其他输入','直到该通道标记到达']],['随后','同时'],extra=text(500,300,'可靠 FIFO 通道：同一通道中，标记不会越过先发出的消息。',27,BLUE,'middle'))
    for name,s in SCENES.items():pipeline(name,s)
    flow('snapshot-pair',[['已处理前四条','Sum = 14000'],['快照 n','状态与位置成套'],['尚未计入的起点','A102 / B202']],['保存','恢复读取'],extra=text(500,300,'Offset 是日志位置；nextOffset 是恢复后要读的下一条。',29,BLUE,'middle'))
    flow('async',[['短暂准备','固定状态视图'],['恢复正常处理','后台写快照'],['写入成功','向协调器确认']],['继续计算','持久化完成'],extra=text(500,300,'工作状态可以继续变化；后台保存的是 n 的一致视图。',29,BLUE,'middle'))
    b=box(50,50,300,95,['非对齐快照','算子状态：10000'],size=30)+box(650,50,300,95,['通道状态','尚未处理的 E04：4000'],ORANGE,'#fff7ed',27)
    b+=arrow(360,100,630,100,'一并保存')+box(180,250,640,105,['恢复：10000 + 通道中的 4000 = 14000','再从 A102 / B202 读取屏障后的记录'],GREEN,'#f0fdf4',28)
    save('unaligned',b)
    b=box(25,65,280,100,['算子恢复','10000 分'],size=30)+box(360,65,280,100,['先恢复通道记录','E04：+4000'],ORANGE,'#fff7ed',29)+box(695,65,280,100,['再重放源记录','E05—E08：+18000'],size=29)
    b+=arrow(310,115,350,115)+arrow(645,115,685,115)+text(500,290,'最终：10000 + 4000 + 18000 = 32000 分',36,GREEN,'middle',600)+text(500,355,'B 从 202 继续，E04 的 201 由保存的通道状态提供。',27,BLUE,'middle')
    save('unaligned-restore',b)
    for m in ['before','crash','repeat']:ledgers('ledger-'+m,m)
    replay('replay-correct');replay('replay-skipped',skip='E05');replay('replay-early',early=True)
    b=box(35,45,410,105,['先重读 E05 / E06','工作状态回到 21000'],size=32)+box(555,45,410,105,['再读 E07 / E08','最终累计 32000'],size=32)+arrow(455,98,545,98)
    b+=text(60,260,'14000 → 20000 → 21000 → 29000 → 32000',36,BLUE)+text(500,350,'源记录可重放 + 工作状态回滚 → 内部重建同一计算结果',29,GREEN,'middle')
    save('replay-two-stages',b)
    flow('design-template',[['输入端','从哪里重读？'],['引擎内部','恢复哪份状态？'],['输出端','重试如何不多算？']],['配套边界','业务效果'],extra=text(500,300,'每一段都写：系统能力、保存内容、恢复动作、验证办法。',29,BLUE,'middle'))
    b=text(25,35,'两次尝试：同一事件 E05，金额均为 6000 分',29,BLUE)
    b+=box(30,90,280,125,['第一次写 E05','第二次仍写 E05'],size=29)+arrow(320,150,540,150,'唯一键 eventId')
    b+=box(560,85,405,140,['MySQL：E05 只有一行','金额：6000 分'],GREEN,'#f0fdf4',31)
    b+=text(500,315,'主键约束 + 相同事件写相同内容 → 重试保持同一业务结果',29,GREEN,'middle')+text(500,378,'8 个唯一事件各一行；对账合计 32000 分。',30,BLUE,'middle')
    save('idempotent',b)
    b=text(30,35,'同一事件 E05 被重试',30,BLUE)
    b+=box(30,95,400,130,['每次执行 total += 6000','第一次 6000；第二次 12000'],RED,'#fef2f2',29)
    b+=box(565,95,400,130,['按 eventId 保存事件行','两次写入后仍是 E05：6000'],GREEN,'#f0fdf4',29)
    b+=text(230,315,'重试再次增加金额',31,RED,'middle')+text(765,315,'重试复用同一业务键',31,GREEN,'middle')
    save('increment',b)
    flow('transaction',[['写入事务 T','结果尚未对外可见'],['预提交并确认','等待检查点完成'],['检查点完成','提交 T，使结果可见']],['准备完成','完成通知'],extra=text(500,320,'完成前失败：中止未提交事务；完成后失败：恢复并完成提交。',27,BLUE,'middle'))
    b=box(30,40,290,105,['已完成 n','可见金额 14000'],GREEN,'#f0fdf4',30)+box(355,40,290,105,['新事务写入 7000','未提交，发生故障'],ORANGE,'#fff7ed',29)+box(680,40,290,105,['中止未完成事务','仍可见 14000'],size=29)
    b+=arrow(325,90,345,90)+arrow(650,90,670,90)+box(180,240,640,100,['恢复后重放 E05—E08：事务写入 18000','下一次检查点完成并提交：可见 32000'],GREEN,'#f0fdf4',29)
    save('transaction-recovery',b)
    flow('solution',[['Kafka','保留 7 天支付日志'],['Flink','每 30 秒保存检查点'],['MySQL','eventId 设唯一主键']],['位点重放','幂等写入'],extra=arrow(500,215,500,283,color=GREEN)+box(250,290,500,65,'共享持久存储：金额状态 + 源位置',GREEN,'#f0fdf4',28))
    flow('test-plan',[['注入故障','E05 / E06 写出后'],['观察恢复','状态 n；A102 / B202'],['核对业务结果','8 行；32000 分']],['停止任务','再次写出'],extra=text(500,310,'同时记录：输入读取位置、恢复的检查点、数据库唯一事件数。',27,BLUE,'middle'))
    flow('exercise-answer',[['输入','保留 2h','按快照位点重放'],['内部','每 30s','写共享持久快照'],['输出','按 eventId','幂等写入']],['配套恢复','允许重试'],extra=text(500,300,'故障 20min 在保留范围内；复核 8 个唯一事件、总金额 32000。',28,GREEN,'middle'))
    b=box(20,60,445,140,['甲：A 从 103 开始','E05 的 6000 被跳过','内部 26000；少 6000'],RED,'#fef2f2',30)
    b+=box(535,60,445,140,['乙：从 102 / 202 重放','E05 / E06 在外部重复','外部 39000；多 7000'],RED,'#fef2f2',30)
    b+=text(500,315,'修复：状态与源位置成套恢复；出口使用幂等键或事务协议。',29,GREEN,'middle')
    save('exercise-differences',b)
    flow('summary',[['输入边界','定位需要重读的记录'],['一致快照','恢复并继续处理'],['输出协议','重试保持业务结果']],['状态对应','端到端配合'],extra=text(500,310,'快照状态、重放位置与输出协议共同确定恢复结果。',32,BLUE,'middle'))
    b = ''
    for x, labels in [(35,['Checkpoint','定期生成故障恢复点']),(375,['Savepoint','人工控制的迁移快照']),(715,['HA','协调服务故障后接替'])]:
        b += box(x,85,250,125,labels)
    b += text(500,310,'协调器元数据与检查点需要可恢复；备用协调器仍依赖这些数据。',27,BLUE,'middle')
    save('ha',b,380)
    b=text(25,40,'每次写 200 MB；存储可持续写入 50 MB/s',30,BLUE)+arrow(80,130,920,130,'每次至少需要 4 秒')
    for x in [80,280,480,680]:b+=line(x,110,x,150,BLUE,3)
    b+=text(80,195,'若每 2 秒发起一次：',28)+text(80,250,'写入需求 = 200 MB ÷ 2 s = 100 MB/s',34,RED)+text(80,320,'100 MB/s > 50 MB/s：写入任务持续积压',31,RED)+text(80,385,'优先减少快照量、降低频率，或提高存储吞吐。',28,BLUE)
    save('cost',b)
    flow('commit-uncertain',[['检查点已完成','事务 T 应当提交'],['数据库已提交','确认消息丢失'],['恢复时查询 / 重试','确认同一事务的结果']],['提交 T','重新协调'],extra=text(500,310,'完成后的事务不能当作未完成事务丢弃；协议需支持提交恢复。',28,BLUE,'middle'))
    (ROOT/'course/diagrams/lesson-05/scenes.json').write_text(json.dumps(SCENES,ensure_ascii=False,indent=2))
    dest=ROOT/'slides/lesson-05/assets/story';dest.mkdir(parents=True,exist_ok=True)
    for p in OUT.glob('*.svg'):shutil.copyfile(p,dest/p.name)
    print(f'Lesson 05: {len(list(OUT.glob("*.svg")))} editable SVG scenes rendered.')

if __name__=='__main__':render()
