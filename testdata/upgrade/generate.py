from pathlib import Path
import shutil,sys,tempfile,subprocess,json,os
src=Path(sys.argv[1]).resolve()
if subprocess.check_output(['git','-C',str(src),'rev-parse','HEAD'],text=True).strip() != '6c20e087383ac7c333f562aa373c873bd5423db6': raise SystemExit('Expected pinned Pulse source')
dst=Path(tempfile.mkdtemp(prefix='original-docker-fixtures-'))
c2j=json.loads(subprocess.check_output(['go','mod','download','-json','github.com/colony-2/c2j@v0.0.129-0.20261115173350-cd211e6aead8'],text=True))['Dir']
(dst/'go.mod').write_text('module github.com/colony-2/pulse\ngo 1.26.0\nrequire (\ngithub.com/colony-2/pulse-provider-api v0.0.0-20261105011626-401f11e595a8\ngithub.com/distribution/reference v0.6.0\ngithub.com/opencontainers/go-digest v1.0.0\n)\n')
for pattern in ('pkg/compute/*.go','internal/providers/docker/*.go'):
 for f in src.glob(pattern):
  if f.name.endswith('_test.go'):continue
  p=dst/f.relative_to(src);p.parent.mkdir(parents=True,exist_ok=True);shutil.copy(f,p)
p=dst/'internal/quantity';p.mkdir(parents=True,exist_ok=True)
s=(src/'internal/quantity/quantity.go').read_text();(p/'quantity.go').write_text('package quantity\nimport("fmt";"strings")\n'+s[s.index('func CPU'):])
p=dst/'internal/c2j';p.mkdir(parents=True,exist_ok=True)
s=(src/'internal/c2j/jobs.go').read_text();(p/'process.go').write_text('package c2j\nimport("fmt";"strings";"github.com/colony-2/pulse/pkg/compute";"github.com/colony-2/pulse/internal/quantity")\n'+s[s.index('func Process'):])
p=dst/'internal/bases';p.mkdir(parents=True,exist_ok=True)
s=(src/'internal/bases/bases.go').read_text().replace('"github.com/colony-2/c2j/pkg/execution"','"github.com/colony-2/pulse/internal/selection"').replace('execution.MatchBase','selection.MatchBase');(p/'bases.go').write_text(s)
p=dst/'internal/selection';p.mkdir(parents=True,exist_ok=True);s=(Path(c2j)/'pkg/execution/base.go').read_text();(p/'match.go').write_text('package selection\nimport "strings"\n'+s[s.index('// MatchBase'):])
p=dst/'internal/providers/adapter';p.mkdir(parents=True,exist_ok=True)
s=(src/'internal/providers/adapter/adapter.go').read_text();m=s[s.index('func Metadata'):s.index('func (p *Provider) Submit(')]
a=s.index('\tfor _, l := range launches {');b=s.index('\tif len(native) == 0',a)
loop=s[a:b]
(p/'translate.go').write_text('''package adapter
import("maps";provider "github.com/colony-2/pulse-provider-api";"github.com/colony-2/pulse/pkg/compute";"github.com/colony-2/pulse/internal/bases";"github.com/colony-2/pulse/internal/c2j")
type Provider struct{Platforms []string;Env map[string]string}
'''+m+'''func Translate(launches []provider.Launch, env map[string]string) []compute.Launch {
 p:=Provider{Platforms:[]string{"linux/arm64","linux/amd64"},Env:env}
 out:=[]provider.Submission{}
 native:=[]compute.Launch{}
'''+loop+''' _=out;return native
}
''')
(dst/'internal/providers/docker/generate_test.go').write_text('''package docker
import("testing";"encoding/json";"os";"time";"fmt";"crypto/sha256";"encoding/hex";provider "github.com/colony-2/pulse-provider-api";"github.com/colony-2/pulse/internal/providers/adapter";"github.com/colony-2/pulse/pkg/compute")
type fixture struct { Launch provider.Launch; Defaults map[string]string; NativeJSON string; Fingerprint string; Name string; Accounting string; CacheKey,CacheOwner string; CacheLabels map[string]string; Cursor string }
func TestGenerateOriginal(t *testing.T){
 deadline:=time.Date(2026,11,30,12,30,0,123,time.UTC)
 cases:=[]provider.Launch{
 {LaunchID:"golden-minimal",Allocation:provider.Allocation{Base:"ubuntu-26*"},Job:provider.JobRef{JobDB:"http://jobdb/tenant",InstanceID:"db",TenantID:"tenant",JobID:"job"},Lease:" synthetic lease\\nwith whitespace \\n"},
 {LaunchID:"golden-present",Allocation:provider.Allocation{Base:"ubuntu-26.04-amd64",CPUMillis:101,MemoryBytes:7000001,ScratchBytes:4101},Job:provider.JobRef{JobDB:"https://jobdb.example/tenant",InstanceID:"db",TenantID:"tenant",JobID:"other"},Lease:"synthetic-present",TimeoutSeconds:42,StartBefore:&deadline},
 {LaunchID:"golden-oci",Allocation:provider.Allocation{Base:"oci:linux/arm64:docker.io/library/alpine:3.21",CPUMillis:500,MemoryBytes:128<<20},Job:provider.JobRef{JobDB:"http://jobdb/other",InstanceID:"db2",TenantID:"other",JobID:"legacy"},Lease:"synthetic-oci"},
 }
 out:=[]fixture{}
 for _,l:=range cases {
  env:=map[string]string{"Z_LAST":"last","A_FIRST":"first"}
  n:=adapter.Translate([]provider.Launch{l},env)[0]
  b,err:=json.Marshal(n);if err!=nil{t.Fatal(err)}
  sum:=sha256.Sum256([]byte(l.LaunchID))
  p:=&Provider{cfg:Config{DependencyCache:CacheConfig{Namespace:"default",Generation:"1"}}}
  cache,_,err:=p.cachePlan(nativePlan{Request:n.Request,ImageID:"sha256:synthetic"},n.Process.Env);if err!=nil{t.Fatal(err)}
  f:=fixture{Launch:l,Defaults:env,NativeJSON:string(b),Fingerprint:fingerprint(n),Name:"pulse-"+hex.EncodeToString(sum[:16])}
  accounting,_:=json.Marshal(p.cost(n.Allocation));f.Accounting=string(accounting)
  if cache!=nil{f.CacheKey=cache.Key;f.CacheOwner=cache.Owner;f.CacheLabels=cache.labels("tools")}
  page,err:=compute.Page([]compute.Instance{{ID:"a",LaunchID:l.LaunchID,State:"running"},{ID:"b",LaunchID:l.LaunchID,State:"running"}},compute.ListRequest{PageSize:1,LaunchID:l.LaunchID});if err!=nil{t.Fatal(err)};f.Cursor=page.NextPageToken
  out=append(out,f)
 }
 b,err:=json.MarshalIndent(out,"","  ");if err!=nil{t.Fatal(err)}
 if err=os.WriteFile(os.Getenv("GOLDEN_OUTPUT"),append(b,'\\n'),0644);err!=nil{t.Fatal(err)}
 fmt.Println("recorded",len(out),"original-source fixtures")
}
''')

subprocess.run(['go','-C',str(dst),'mod','tidy'],check=True)
env=dict(os.environ,GOLDEN_OUTPUT=str(Path(__file__).resolve().parent/'original.json'))
subprocess.run(['go','-C',str(dst),'test','-count=1','-run','^TestGenerateOriginal$','-v','./internal/providers/docker'],env=env,check=True)
shutil.rmtree(dst)
