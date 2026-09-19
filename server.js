const http=require("http"),fs=require("fs"),path=require("path"),crypto=require("crypto"),WebSocket=require("ws");
const PORT=process.env.PORT||8787;
// V10 CLOUD FREE: intentionally in-memory only. Persistent history/database will be added later.
let db={devices:{},messages:[],pairs:{}};
const save=()=>{};
const send=(w,o)=>w&&w.readyState===WebSocket.OPEN&&w.send(JSON.stringify(o));
const online=d=>!!(d&&d.ws&&d.ws.readyState===WebSocket.OPEN);
const uid=()=>crypto.randomUUID?crypto.randomUUID():crypto.randomBytes(16).toString("hex");
const code=()=>String(Math.floor(100000+Math.random()*900000));
const peer=id=>db.devices[id]?.pair?db.devices[db.devices[id].pair]:null;
const server=http.createServer((req,res)=>{const u=req.url.split("?")[0],f=path.resolve(__dirname,"public",u==="/"?"index.html":u.slice(1)),b=path.resolve(__dirname,"public");if(!f.startsWith(b))return res.writeHead(403).end();fs.readFile(f,(e,d)=>{if(e)return res.writeHead(404).end();res.writeHead(200,{"Content-Type":f.endsWith(".js")?"text/javascript":"text/html; charset=utf-8","Cache-Control":"no-store"}).end(d)})});
const wss=new WebSocket.Server({server});
function history(id){const d=db.devices[id];if(online(d))send(d.ws,{type:"history",messages:db.messages.filter(m=>m.from===id||m.to===id).slice(-500).map(m=>({...m,fromName:db.devices[m.from]?.name||"Device"}))})}
function pending(id){const d=db.devices[id];if(!online(d))return;for(const m of db.messages.filter(x=>x.to===id&&x.status==="sent")){send(d.ws,{type:"message",...m,fromName:db.devices[m.from]?.name||"Device",offline:true});m.status="delivered";const s=db.devices[m.from];if(online(s))send(s.ws,{type:"delivered",messageId:m.id})}save()}
function read(id,mid){const m=db.messages.find(x=>x.id===String(mid)&&x.to===id);if(!m)return;m.status="read";save();const s=db.devices[m.from];if(online(s))send(s.ws,{type:"read",messageId:m.id})}
wss.on("connection",ws=>{let id=null;ws.on("message",raw=>{let m;try{m=JSON.parse(raw)}catch{return}
 if(m.type==="register"){id=String(m.deviceId||uid());db.devices[id]=db.devices[id]||{name:"Device",pair:null};db.devices[id].name=String(m.name||db.devices[id].name);db.devices[id].ws=ws;send(ws,{type:"registered",deviceId:id,paired:!!db.devices[id].pair,peerName:db.devices[id].pair&&db.devices[db.devices[id].pair]?.name});history(id);pending(id);return}
 const me=db.devices[id];if(!me)return;
 if(m.type==="createPair"){const c=code();db.pairs[c]={deviceId:id,expires:Date.now()+600000};save();send(ws,{type:"pairCode",code:c});return}
 if(m.type==="joinPair"){const p=db.pairs[String(m.code||"")],a=p&&db.devices[p.deviceId];if(!p||p.expires<Date.now()||!a)return send(ws,{type:"pairError",message:"Invalid or expired code."});if(a.pair||me.pair)return send(ws,{type:"pairError",message:"A device is already paired."});a.pair=id;me.pair=p.deviceId;delete db.pairs[String(m.code)];save();send(a.ws,{type:"paired",peerName:me.name});send(me.ws,{type:"paired",peerName:a.name});return}
 if(m.type==="chat"){if(!me.pair)return send(ws,{type:"sendError",message:"Not paired."});const mid=String(m.messageId||uid()),msg={id:mid,text:String(m.text||"").trim().slice(0,4000),from:id,to:me.pair,time:Date.now(),status:"sent"};if(!msg.text)return;db.messages.push(msg);save();send(ws,{type:"accepted",messageId:mid});const p=db.devices[me.pair];if(online(p)){send(p.ws,{type:"message",...msg,fromName:me.name});msg.status="delivered";save();send(ws,{type:"delivered",messageId:mid})}return}
 if(m.type==="read")return read(id,m.messageId);
 if(m.type==="markAllRead"){db.messages.filter(x=>x.to===id&&x.from===me.pair&&x.status!=="read").forEach(x=>read(id,x.id));return}
 if(m.type==="typing"){const p=peer(id);if(p&&online(p))send(p.ws,{type:"typing",name:me.name,on:!!m.on})}
 if(m.type==="diagnostic"){const p=peer(id);send(ws,{type:"diagnosticResult",paired:!!p,peerOnline:online(p),storedMessages:db.messages.filter(x=>x.from===id||x.to===id).length})}
});ws.on("close",()=>{if(id&&db.devices[id]?.ws===ws){delete db.devices[id].ws;const p=peer(id);if(p)send(p.ws,{type:"peerOffline"})}})});
server.listen(PORT,()=>console.log(`CADVORA Private Chat V10 running on port ${PORT}`));