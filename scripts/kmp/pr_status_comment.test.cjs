const {test} = require('node:test');
const assert = require('node:assert/strict');
const statusComment = require('./pr_status_comment.cjs');
function harness(comments) {
  const calls = [];
  const record = kind => async args => { calls.push([kind,args]); return {data:{id:42}}; };
  return {calls, core:{setOutput:(...args)=>calls.push(['output',args])}, github:{
    paginate:async()=>comments,
    rest:{issues:{listComments:()=>{},deleteComment:record('delete'),updateComment:record('update'),createComment:record('create')}}
  }};
}
const env = {STATUS_REPOSITORY:'example/app',STATUS_PR:'123',STATUS_MARKER:'<!-- central-ios-testflight -->',
  STATUS_BODY:'Build 410.57 uploaded',STATUS_MODE:'upsert',STATUS_APP_SLUG:'our-ci'};
function comment(id, app='our-ci', body=env.STATUS_MARKER+'\nold') {
  return {id,body,performed_via_github_app:app ? {slug:app} : null};
}
test('upsert preserves foreign and human comments and removes owned duplicates',async()=>{
  const h = harness([comment(1),comment(2),comment(3,'other-ci'),comment(4,null),comment(5,'our-ci','body contains '+env.STATUS_MARKER)]);
  await statusComment(h,env);
  assert.deepEqual(h.calls.filter(c=>c[0]==='delete').map(c=>c[1].comment_id),[2]);
  assert.equal(h.calls.find(c=>c[0]==='update')[1].comment_id,1);
  assert.equal(h.calls.find(c=>c[0]==='update')[1].body,env.STATUS_MARKER+'\n'+env.STATUS_BODY);
});
test('replace purges prior owned comments and creates new',async()=>{
  const h = harness([comment(1),comment(2)]);
  await statusComment(h,{...env,STATUS_MODE:'replace'});
  assert.deepEqual(h.calls.map(c=>c[0]),['delete','delete','create','output']);
});
test('purge only deletes owned marker comments',async()=>{
  const h = harness([comment(1),comment(2,null)]);
  await statusComment(h,{...env,STATUS_MODE:'purge'});
  assert.deepEqual(h.calls.map(c=>c[0]),['delete']);
});
test('manual run avoids GitHub requests',async()=>{
  const h = harness([]);
  h.github.paginate = async()=>{throw Error('should not call')};
  await statusComment(h,{...env,STATUS_PR:''});
  assert.deepEqual(h.calls,[]);
});
test('rejects malformed inputs before mutation',async()=>{
  for (const override of [{STATUS_PR:'1;echo danger'},{STATUS_MARKER:'visible marker'},{STATUS_APP_SLUG:''}]) {
    const h = harness([]);
    await assert.rejects(statusComment(h,{...env,...override}));
    assert.deepEqual(h.calls,[]);
  }
});
