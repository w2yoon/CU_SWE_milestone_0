import os,signal,subprocess,time
nodes={}
def launch(i,port):
    env=os.environ.copy();env['NODE_ID']=str(i);env['PORT']=str(port)
    f=open(f'/tmp/huddle-{i}.log','ab')
    p=subprocess.Popen(['python3','/app/server.py'],env=env,stdout=f,stderr=f,start_new_session=True)
    nodes[i]=(p,f)
def stop(*_):
    for p,f in list(nodes.values()):
        try:p.kill()
        except:pass
        f.close()
    raise SystemExit
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
for i,port in enumerate((8000,8001,8002)): launch(i,port)
while True:
    for i,port in enumerate((8000,8001,8002)):
        p,f=nodes[i]
        if p.poll() is not None:
            f.close();launch(i,port)
    time.sleep(1)
