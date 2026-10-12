#!/usr/bin/env python3
import os, re, json, sqlite3, hashlib, secrets, threading, time, base64, struct
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
DB='/app/huddle.db'
LOCK=threading.RLock()
WS_CLIENTS=[]
def now(): return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
def db():
    c=sqlite3.connect(DB,check_same_thread=False); c.row_factory=sqlite3.Row
    c.executescript('''
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,username TEXT UNIQUE,password TEXT,display_name TEXT,timezone TEXT DEFAULT 'UTC',avatar_url TEXT DEFAULT '',status_text TEXT DEFAULT '',status_emoji TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS tokens(token TEXT PRIMARY KEY,user_id INTEGER);
    CREATE TABLE IF NOT EXISTS workspaces(id INTEGER PRIMARY KEY,slug TEXT UNIQUE,name TEXT,owner_id INTEGER,join_mode TEXT DEFAULT 'open');
    CREATE TABLE IF NOT EXISTS members(workspace_id INTEGER,user_id INTEGER,role TEXT,PRIMARY KEY(workspace_id,user_id));
    CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY,workspace_id INTEGER,name TEXT,is_private INTEGER DEFAULT 0,is_dm INTEGER DEFAULT 0,topic TEXT DEFAULT '',is_archived INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS channel_members(channel_id INTEGER,user_id INTEGER,PRIMARY KEY(channel_id,user_id));
    CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY,channel_id INTEGER,author_id INTEGER,body TEXT,parent_id INTEGER,created_at TEXT,edited_at TEXT,deleted INTEGER DEFAULT 0,event_id INTEGER);
    CREATE TABLE IF NOT EXISTS reactions(message_id INTEGER,user_id INTEGER,emoji TEXT,PRIMARY KEY(message_id,user_id,emoji));
    CREATE TABLE IF NOT EXISTS pins(message_id INTEGER,user_id INTEGER,pinned_at TEXT,PRIMARY KEY(message_id));
    CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,uploader_id INTEGER,filename TEXT,content_type TEXT,size INTEGER,created_at TEXT,data BLOB);
    '''); return c
def userobj(r):
    return {k:r[k] for k in ('id','username','display_name','timezone','avatar_url','status_text','status_emoji')}
def channelobj(r):
    d=dict(r); d['is_private']=bool(d['is_private']);d['is_dm']=bool(d['is_dm']);d['is_archived']=bool(d['is_archived']);return d
def msgobj(c,r):
    d=dict(r); d['author']=userobj(c.execute('select * from users where id=?',(r['author_id'],)).fetchone())
    d['parent_id']=r['parent_id']; d['files']=[]; d['mentions']=[]
    rr=c.execute('select emoji,count(*) n,group_concat(user_id) ids from reactions where message_id=? group by emoji',(r['id'],)).fetchall()
    d['reactions']=[{'emoji':x['emoji'],'count':x['n'],'user_ids':[int(i) for i in x['ids'].split(',')]} for x in rr]
    return d
def broadcast(ev):
    raw=json.dumps(ev).encode()
    for s in list(WS_CLIENTS):
        try:
            frame=b'\x81'+(bytes([len(raw)]) if len(raw)<126 else b'\x7e'+struct.pack('>H',len(raw)))+raw;s.sendall(frame)
        except: 
            try: WS_CLIENTS.remove(s)
            except: pass
class H(BaseHTTPRequestHandler):
    server_version='Huddle/1'
    def log_message(self,*a): pass
    def sendj(self,code,obj,headers=None):
        b=json.dumps(obj).encode();self.send_response(code);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(b)))
        for k,v in (headers or {}).items(): self.send_header(k,v)
        self.end_headers();self.wfile.write(b)
    def readj(self):
        n=int(self.headers.get('Content-Length','0'));return json.loads(self.rfile.read(n) or b'{}')
    def auth(self):
        t=self.headers.get('Authorization','').replace('Bearer ','')
        if not t:
            t=parse_qs(urlparse(self.path).query).get('token',[''])[0].replace('Bearer ','')
        c=db();r=c.execute('select u.* from tokens t join users u on u.id=t.user_id where t.token=?',(t,)).fetchone();c.close();return r
    def path(self): return urlparse(self.path)
    def do_GET(self):
        p=urlparse(self.path); path=p.path
        if path=='/api/health': return self.sendj(200,{'status':'ok','node_id':int(os.getenv('NODE_ID','0'))})
        if path=='/' or path.startswith('/static'): return self.html()
        u=self.auth()
        if path=='/api/auth/me': return self.sendj(200,{'user':userobj(u)}) if u else self.sendj(401,{'error':'unauthorized'})
        if path=='/api/ws': return self.websocket(u)
        if not u:return self.sendj(401,{'error':'unauthorized'})
        c=db()
        m=re.match(r'/api/users/(\d+)$',path)
        if m:
            r=c.execute('select * from users where id=?',(m.group(1),)).fetchone();return self.sendj(200,{'user':userobj(r)}) if r else self.sendj(404,{'error':'not found'})
        if path=='/api/workspaces':
            rs=c.execute('select w.* from workspaces w join members m on m.workspace_id=w.id where m.user_id=?',(u['id'],)).fetchall();return self.sendj(200,{'workspaces':[dict(x) for x in rs]})
        m=re.match(r'/api/workspaces/([^/]+)$',path)
        if m:
            w=c.execute('select * from workspaces where slug=?',(m.group(1),)).fetchone()
            if not w:return self.sendj(404,{'error':'not found'})
            cs=c.execute('select * from channels where workspace_id=? and (is_archived=0)',(w['id'],)).fetchall();return self.sendj(200,{'workspace':dict(w),'channels':[channelobj(x) for x in cs],'read_state':{}})
        m=re.match(r'/api/channels/(\d+)/messages$',path)
        if m:
            rs=c.execute('select * from messages where channel_id=? and deleted=0 order by id desc limit 200',(m.group(1),)).fetchall();return self.sendj(200,{'messages':[msgobj(c,x) for x in rs],'next_cursor':None})
        m=re.match(r'/api/messages/(\d+)/replies$',path)
        if m:
            rs=c.execute('select * from messages where parent_id=? and deleted=0 order by id',(m.group(1),)).fetchall();return self.sendj(200,{'replies':[msgobj(c,x) for x in rs],'next_cursor':None})
        m=re.match(r'/api/channels/(\d+)$',path)
        if m:
            r=c.execute('select * from channels where id=?',(m.group(1),)).fetchone();return self.sendj(200,{'channel':channelobj(r)}) if r else self.sendj(404,{'error':'not found'})
        if path=='/api/search':
            q=parse_qs(p.query).get('q',[''])[0];rs=c.execute("select * from messages where body like ? and deleted=0 order by id desc limit 50",('%'+q+'%',)).fetchall();return self.sendj(200,{'results':[msgobj(c,x) for x in rs],'next_cursor':None})
        m=re.match(r'/api/workspaces/([^/]+)/members$',path)
        if m:
            w=c.execute('select id from workspaces where slug=?',(m.group(1),)).fetchone()
            if not w:return self.sendj(404,{'error':'not found'})
            rs=c.execute('select m.user_id,m.role,u.username,u.display_name from members m join users u on u.id=m.user_id where m.workspace_id=?',(w['id'],)).fetchall()
            return self.sendj(200,{'members':[dict(x) for x in rs]})
        m=re.match(r'/api/channels/(\d+)/members$',path)
        if m:
            rs=c.execute('select u.* from channel_members x join users u on u.id=x.user_id where x.channel_id=?',(m.group(1),)).fetchall()
            return self.sendj(200,{'members':[userobj(x) for x in rs]})
        m=re.match(r'/api/channels/(\d+)/pins$',path)
        if m:
            rs=c.execute('select p.*,m.* from pins p join messages m on m.id=p.message_id where m.channel_id=? order by p.pinned_at desc',(m.group(1),)).fetchall()
            return self.sendj(200,{'pins':[{'message':msgobj(c,x),'pinned_by':x['user_id'],'pinned_at':x['pinned_at']} for x in rs],'next_cursor':None})
        m=re.match(r'/api/files/([0-9]+)/download$',path)
        if m:
            r=c.execute('select * from files where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            self.send_response(200);self.send_header('Content-Type',r['content_type']);self.send_header('Content-Disposition','attachment; filename="'+r['filename']+'"');self.send_header('Content-Length',str(len(r['data'])));self.end_headers();self.wfile.write(r['data']);return
        m=re.match(r'/api/files/([0-9]+)$',path)
        if m:
            r=c.execute('select id,uploader_id,filename,content_type,size,created_at from files where id=?',(m.group(1),)).fetchone()
            return self.sendj(200,{'file':dict(r)}) if r else self.sendj(404,{'error':'not found'})
        return self.sendj(404,{'error':'not found'})
    def do_POST(self):
        path=urlparse(self.path).path; b={} if self.headers.get('Content-Type','').startswith('multipart/form-data') else self.readj()
        if path=='/api/auth/register':
            if not re.fullmatch(r'[A-Za-z0-9_]+',b.get('username','')) or len(b.get('password',''))<8:return self.sendj(400,{'error':'invalid registration'})
            c=db()
            try:c.execute('insert into users(username,password,display_name) values(?,?,?)',(b['username'],hashlib.sha256(b['password'].encode()).hexdigest(),b.get('display_name') or b['username']));c.commit()
            except sqlite3.IntegrityError:return self.sendj(409,{'error':'duplicate username'})
            r=c.execute('select * from users where username=?',(b['username'],)).fetchone();t=secrets.token_urlsafe(24);c.execute('insert into tokens values(?,?)',(t,r['id']));c.commit();return self.sendj(201,{'user':userobj(r),'token':t})
        if path=='/api/auth/login':
            c=db();r=c.execute('select * from users where username=? and password=?',(b.get('username'),hashlib.sha256(b.get('password','').encode()).hexdigest())).fetchone()
            if not r:return self.sendj(401,{'error':'invalid credentials'})
            t=secrets.token_urlsafe(24);c.execute('insert into tokens values(?,?)',(t,r['id']));c.commit();return self.sendj(200,{'user':userobj(r),'token':t})
        u=self.auth()
        if not u:return self.sendj(401,{'error':'unauthorized'})
        c=db()
        if path=='/api/files':
            ct=self.headers.get('Content-Type','')
            raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            if len(raw)>10*1024*1024:return self.sendj(413,{'error':'file too large'})
            if 'boundary=' not in ct:return self.sendj(400,{'error':'multipart required'})
            boundary=ct.split('boundary=',1)[1].encode()
            crlf=bytes((13,10))
            data=None;filename='upload';ctype='application/octet-stream'
            for part in raw.split(b'--'+boundary):
                if b'filename="' in part and crlf+crlf in part:
                    head,data=part.split(crlf+crlf,1)
                    data=data.rsplit(crlf,1)[0]
                    mm=re.search(b'filename="([^"]+)"',head)
                    if mm: filename=mm.group(1).decode(errors='replace')
                    mm=re.search(b'Content-Type:[ ]*([^\\r\
]+)',head,re.I)
                    if mm: ctype=mm.group(1).decode()
                    break
            if data is None:return self.sendj(400,{'error':'file required'})
            cur=c.execute('insert into files(uploader_id,filename,content_type,size,created_at,data) values(?,?,?,?,?,?)',(u['id'],filename,ctype,len(data),now(),data));c.commit()
            r=c.execute('select id,uploader_id,filename,content_type,size,created_at from files where id=?',(cur.lastrowid,)).fetchone()
            return self.sendj(201,{'file':dict(r)})
        if path=='/api/workspaces':
            slug=b.get('slug','');name=b.get('name','')
            if not re.fullmatch(r'[a-z0-9-]{2,32}',slug) or not name:return self.sendj(400,{'error':'invalid workspace'})
            try:c.execute('insert into workspaces(slug,name,owner_id) values(?,?,?)',(slug,name,u['id']))
            except sqlite3.IntegrityError:return self.sendj(409,{'error':'duplicate'})
            w=c.execute('select * from workspaces where slug=?',(slug,)).fetchone();c.execute('insert into members values(?,?,?)',(w['id'],u['id'],'owner'));cur=c.execute('insert into channels(workspace_id,name) values(?,?)',(w['id'],'general'));cid=cur.lastrowid;c.execute('insert into channel_members values(?,?)',(cid,u['id']));c.commit();return self.sendj(201,{'workspace':dict(w),'general_channel':channelobj(c.execute('select * from channels where id=?',(cid,)).fetchone())})
        m=re.match(r'/api/workspaces/([^/]+)/channels$',path)
        if m:
            w=c.execute('select * from workspaces where slug=?',(m.group(1),)).fetchone();mem=c.execute('select role from members where workspace_id=? and user_id=?',(w['id'],u['id'])).fetchone()
            if not mem:return self.sendj(403,{'error':'forbidden'})
            if b.get('is_private') and mem['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            if not re.fullmatch(r'[a-z0-9-]{1,32}',b.get('name','')):return self.sendj(400,{'error':'invalid name'})
            try:cur=c.execute('insert into channels(workspace_id,name,is_private,topic) values(?,?,?,?)',(w['id'],b['name'],int(bool(b.get('is_private'))),b.get('topic','')));cid=cur.lastrowid;c.execute('insert into channel_members values(?,?)',(cid,u['id']));c.commit()
            except sqlite3.IntegrityError:return self.sendj(409,{'error':'duplicate'})
            return self.sendj(201,{'channel':channelobj(c.execute('select * from channels where id=?',(cid,)).fetchone())})
        m=re.match(r'/api/channels/(\d+)/join$',path)
        if m:
            ch=c.execute('select * from channels where id=?',(m.group(1),)).fetchone()
            if not ch:return self.sendj(404,{'error':'not found'})
            c.execute('insert or ignore into channel_members values(?,?)',(ch['id'],u['id']));c.execute('insert or ignore into members select workspace_id,?,\'member\' from channels where id=?',(u['id'],ch['id']));c.commit();return self.sendj(200,{'channel':channelobj(ch)})
        m=re.match(r'/api/channels/(\d+)/messages$',path)
        if m:
            ch=c.execute('select * from channels where id=?',(m.group(1),)).fetchone();body=b.get('body','')
            if not ch:return self.sendj(404,{'error':'not found'})
            if not body.strip():return self.sendj(400,{'error':'empty body'})
            if body.startswith('/shrug'):body=(body[6:].strip()+' ¯\\_(ツ)_/¯').strip()
            eid=(c.execute('select coalesce(max(event_id),0) from messages where channel_id=?',(ch['id'],)).fetchone()[0]+1)
            cur=c.execute('insert into messages(channel_id,author_id,body,parent_id,created_at,event_id) values(?,?,?,?,?,?)',(ch['id'],u['id'],body,b.get('parent_id'),now(),eid));mid=cur.lastrowid;c.commit();r=c.execute('select * from messages where id=?',(mid,)).fetchone();ev={'type':'message.reply' if r['parent_id'] else 'message.created','event_id':eid,'channel_id':ch['id'],'message':msgobj(c,r)};broadcast(ev);return self.sendj(201,{'message':msgobj(c,r)})
        m=re.match(r'/api/messages/(\d+)/reactions$',path)
        if m:
            try:c.execute('insert or ignore into reactions values(?,?,?)',(m.group(1),u['id'],b.get('emoji','')));c.commit()
            except:pass
            rs=c.execute('select emoji,count(*) n,group_concat(user_id) ids from reactions where message_id=? group by emoji',(m.group(1),)).fetchall();return self.sendj(200,{'reactions':[{'emoji':x['emoji'],'count':x['n'],'user_ids':[int(i) for i in x['ids'].split(',')]} for x in rs]})
        m=re.match(r'/api/messages/(\d+)/pin$',path)
        if m:
            r=c.execute('select * from messages where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            c.execute('insert or ignore into pins values(?,?,?)',(r['id'],u['id'],now()));c.commit()
            x=c.execute('select * from pins where message_id=?',(r['id'],)).fetchone()
            return self.sendj(200,{'pin':{'message_id':x['message_id'],'pinned_by':x['user_id'],'pinned_at':x['pinned_at']}})
        m=re.match(r'/api/channels/(\d+)/(archive|unarchive)$',path)
        if m:
            c.execute('update channels set is_archived=? where id=?',(m.group(2)=='archive',m.group(1)));c.commit()
            return self.sendj(200,{'channel':channelobj(c.execute('select * from channels where id=?',(m.group(1),)).fetchone())})
        m=re.match(r'/api/dms$',path)
        if m:
            other=c.execute('select id from users where id=?',(b.get('user_id'),)).fetchone()
            if not other:return self.sendj(404,{'error':'not found'})
            a,z=sorted((u['id'],other['id']));ch=c.execute('select c.id from channels c join channel_members x on x.channel_id=c.id and x.user_id=? join channel_members y on y.channel_id=c.id and y.user_id=? where c.is_dm=1',(a,z)).fetchone()
            if ch:return self.sendj(200,{'channel_id':ch['id']})
            cur=c.execute('insert into channels(workspace_id,name,is_dm) values(0,?,1)',('dm-'+str(a)+'-'+str(z),));cid=cur.lastrowid;c.execute('insert into channel_members values(?,?)',(cid,a));c.execute('insert into channel_members values(?,?)',(cid,z));c.commit()
            return self.sendj(200,{'channel_id':cid})
        return self.sendj(404,{'error':'not found'})
    def do_PATCH(self):
        u=self.auth()
        if not u:return self.sendj(401,{'error':'unauthorized'})
        p=urlparse(self.path).path;b=self.readj();c=db()
        m=re.match(r'/api/messages/(\d+)$',p)
        if m:
            r=c.execute('select * from messages where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            if r['author_id']!=u['id']:return self.sendj(403,{'error':'forbidden'})
            c.execute('update messages set body=?,edited_at=? where id=?',(b.get('body',''),now(),r['id']));c.commit();return self.sendj(200,{'message':msgobj(c,c.execute('select * from messages where id=?',(r['id'],)).fetchone())})
        m=re.match(r'/api/users/me$',p)
        if m:
            allowed=['display_name','timezone','avatar_url','status_text','status_emoji'];vals={k:b[k] for k in allowed if k in b}
            if vals:c.execute('update users set '+','.join(k+'=?' for k in vals)+' where id=?',tuple(vals.values())+(u['id'],));c.commit()
            return self.sendj(200,{'user':userobj(c.execute('select * from users where id=?',(u['id'],)).fetchone())})
        return self.sendj(404,{'error':'not found'})
    def do_DELETE(self):
        u=self.auth()
        if not u:return self.sendj(401,{'error':'unauthorized'})
        m=re.match(r'/api/messages/(\d+)$',urlparse(self.path).path)
        if m:
            c=db();c.execute('update messages set deleted=1 where id=? and author_id=?',(m.group(1),u['id']));c.commit();return self.sendj(200,{'deleted':True})
        return self.sendj(404,{'error':'not found'})
    def websocket(self,u):
        if not u:
            self.send_response(403); self.end_headers(); return
        key=self.headers.get('Sec-WebSocket-Key','')
        acc=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        self.send_response(101);self.send_header('Upgrade','websocket');self.send_header('Connection','Upgrade');self.send_header('Sec-WebSocket-Accept',acc);self.end_headers()
        sock=self.connection; WS_CLIENTS.append(sock)
        def frame(obj):
            raw=json.dumps(obj).encode(); n=len(raw)
            if n<126: return b'\\x81'+bytes([n])+raw
            return b'\\x81\\x7e'+struct.pack('>H',n)+raw
        try:
            while True:
                h=sock.recv(2)
                if len(h)<2: break
                n=h[1]&127; masked=h[1]&128
                if n==126: n=struct.unpack('>H',sock.recv(2))[0]
                if n==127: n=struct.unpack('>Q',sock.recv(8))[0]
                mask=sock.recv(4) if masked else b''
                data=sock.recv(n)
                if masked: data=bytes(data[i]^mask[i%4] for i in range(n))
                if h[0]&15==8: break
                try: req=json.loads(data)
                except: continue
                cid=req.get('channel_id')
                if req.get('type')=='subscribe':
                    c=db(); head=c.execute('select coalesce(max(event_id),0) from messages where channel_id=?',(cid,)).fetchone()[0];sock.sendall(frame({'type':'subscribed','channel_id':cid,'head_event_id':head}))
                elif req.get('type')=='resume':
                    c=db(); since=int(req.get('since_event_id',0)); rows=c.execute('select * from messages where channel_id=? and event_id>? and deleted=0 order by event_id',(cid,since)).fetchall()
                    for r in rows: sock.sendall(frame({'type':'message.reply' if r['parent_id'] else 'message.created','event_id':r['event_id'],'channel_id':cid,'message':msgobj(c,r)}))
                    head=c.execute('select coalesce(max(event_id),0) from messages where channel_id=?',(cid,)).fetchone()[0];sock.sendall(frame({'type':'resumed','channel_id':cid,'head_event_id':head}))
        except Exception: pass
        try: WS_CLIENTS.remove(sock)
        except ValueError: pass
    def html(self):
        h='''<!doctype html><html><head><meta charset="utf-8"><title>Huddle</title><style>body{margin:0;font:15px Arial;color:#222}button{border:0;padding:9px 14px;border-radius:4px;color:#fff;cursor:pointer}.primary{background:#007a5a}.danger{background:#e01e5a}.secondary{background:#1264a3}.app{display:flex;height:100vh}.side{width:250px;background:#3f0e40;color:white;padding:20px}.main{flex:1;padding:28px}.modal{max-width:420px;margin:80px auto}input,textarea{padding:10px;margin:6px 0;width:100%;box-sizing:border-box}.msg{padding:12px;border-bottom:1px solid #ddd}</style></head><body><div id="app"></div><script>
const A='/api';let token=localStorage.getItem('huddle.token'),user,ws,cur;
async function api(x,o={}){o.headers={...(o.headers||{}),'Content-Type':'application/json',...(token?{Authorization:'Bearer '+token}:{})};let r=await fetch(A+x,o);let j=await r.json();if(!r.ok)throw Error(j.error||'Error');return j}
function auth(){document.getElementById('app').innerHTML=`<div class=modal><h1>Huddle</h1><form data-testid=auth-form><input name=username placeholder=username required><input name=password type=password placeholder=password required><input name=display_name placeholder='display name'><button data-testid=auth-submit data-button-role=primary class=primary>Sign in</button></form><p id=err></p><button data-testid=auth-toggle class=secondary onclick="reg=true">Create account</button></div>`;document.querySelector('form').onsubmit=async e=>{e.preventDefault();let f=new FormData(e.target);try{let j=await api(reg?'/auth/register':'/auth/login',{method:'POST',body:JSON.stringify(Object.fromEntries(f))});token=j.token;localStorage.setItem('huddle.token',token);user=j.user;home()}catch(x){err.textContent=x.message}}}
async function home(){let j=await api('/workspaces');document.getElementById('app').innerHTML=`<div class=app><aside class=side><h2 data-testid=workspace-header>Huddle</h2><div data-testid=current-user>${user?.display_name||''}</div><div data-testid=channel-list id=channels></div><button data-testid=new-channel-btn data-button-role=secondary class=secondary onclick=newchan()>New channel</button><button data-testid=logout-btn data-button-role=danger class=danger onclick="localStorage.clear();token=null;auth()">Logout</button></aside><main class=main><h1>Welcome to Huddle</h1><div data-testid=message-list id=messages></div></main></div>`;let w=j.workspaces[0];if(w){let d=await api('/workspaces/'+w.slug);channels.innerHTML=d.channels.map(x=>`<div data-testid=channel-entry data-channel-id=${x.id} data-channel-name=${x.name} onclick="channel(${x.id})"># ${x.name}</div>`).join('');}}
async function channel(id){cur=id;let j=await api('/channels/'+id+'/messages');messages.innerHTML=`<h2 data-testid=channel-title># channel</h2>`+j.messages.reverse().map(m=>`<div class=msg data-testid=message><b>${m.author.display_name}</b> <small>${m.created_at}</small><div data-testid=message-body>${m.body}</div><button data-testid=open-thread-btn onclick="thread(${m.id})">Reply (${m.reply_count||0})</button><button data-testid=reaction-button onclick="react(${m.id})">😀</button></div>`).join('')+`<form onsubmit="send(event)" style="display:flex"><input data-testid=message-input id=mi><button data-testid=send-btn data-button-role=primary class=primary>Send</button></form>`}
async function send(e){e.preventDefault();await api('/channels/'+cur+'/messages',{method:'POST',body:JSON.stringify({body:mi.value})});mi.value='';channel(cur)}
async function react(id){await api('/messages/'+id+'/reactions',{method:'POST',body:JSON.stringify({emoji:'😀'})});channel(cur)}
function newchan(){let n=prompt('Channel name');if(n)api('/workspaces/'+j.workspaces[0].slug+'/channels',{method:'POST',body:JSON.stringify({name:n})}).then(home)}
let reg=false;if(token)api('/auth/me').then(x=>{user=x.user;home()}).catch(auth);else auth();
</script></body></html>''';b=h.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
def run():
    port=int(os.getenv('PORT','8000'));ThreadingHTTPServer(('127.0.0.1',port),H).serve_forever()
if __name__=='__main__':run()
