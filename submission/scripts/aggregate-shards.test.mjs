import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
const root=fileURLToPath(new URL('../',import.meta.url));
const artifactRoot=path.join(root,'.artifacts');
fs.mkdirSync(artifactRoot,{recursive:true});
function fixture(users,offset,latency) {
  return {passed:true,config:{connections:users,user_offset:offset,distinct_user_per_recipient:true,measurement_ns:1e9,logical_messages_per_second:1},
    measurement_started_at:'2026-01-01T00:00:00Z',measurement_ends_at:'2026-01-01T00:00:01Z',measurement_and_drain_seconds:1,
    min_resumed_during_measurement:users,connected_at_measurement_start:users,recipients_with_measured_delivery:users,
    expected_measured_deliveries:users,received_measured_deliveries:users,measured_acknowledged_messages:1,
    missing_measured_deliveries:0,missing_warmup_deliveries:0,unacknowledged_logical_messages:0,scheduler_queue_drops:0,
    unscheduled_rate_slots:0,duplicate_callbacks:0,client_terminal_errors:0,never_connected:0,
    http_attempt_errors:0,http_recovery_retries:0,websocket_establishment_failures:0,connection_retries:0,suppressed_duplicates:0,
    history_audit:{acknowledged_checked:1,missing_or_changed_acknowledged:0},
    delivery_latency_ms_upper_bound_histogram:{[latency]:users},ack_latency_ms_upper_bound_histogram:{2:1}};
}
function run(reports,mode='healthy') {
  const temporary=fs.mkdtempSync(path.join(artifactRoot,'aggregate-test-'));
  try {
    fs.mkdirSync(path.join(temporary,'docs/results'),{recursive:true});
    reports.forEach((report,i)=>fs.writeFileSync(path.join(temporary,`docs/results/fixture-shard${i}.json`),JSON.stringify(report)));
    const output=path.join(temporary,'summary.json');
    const child=spawnSync(process.execPath,[path.join(root,'scripts/aggregate-shards.mjs'),'fixture',String(reports.length),output,mode],{cwd:temporary,encoding:'utf8'});
    return {code:child.status,result:fs.existsSync(output)?JSON.parse(fs.readFileSync(output)):null};
  } finally {
    const relative=path.relative(artifactRoot,temporary);
    if(relative.startsWith('..')||path.isAbsolute(relative)) throw new Error('Unsafe temporary cleanup path');
    fs.rmSync(temporary,{recursive:true,force:true});
  }
}
test('global latency uses all samples rather than averaging percentiles',()=>{
  const {code,result}=run([fixture(99,0,1),fixture(1,99,100)]);
  assert.equal(code,0);assert.equal(result.delivery_latency.samples,100);
  assert.equal(result.delivery_latency.p99_ms_upper_bound,1);
  assert.equal(result.delivery_latency.max_ms_upper_bound,100);
});
test('overlapping user identities cannot count as distinct users',()=>{
  const {code}=run([fixture(99,0,1),fixture(1,98,100)]);assert.notEqual(code,0);
});
test('missing deliveries fail even if a component pass flag is wrong',()=>{
  const bad=fixture(99,0,1);bad.received_measured_deliveries=98;bad.missing_measured_deliveries=1;bad.delivery_latency_ms_upper_bound_histogram={1:98};
  const {code,result}=run([bad,fixture(1,99,100)]);assert.notEqual(code,0);assert.equal(result.passed,false);
});
test('healthy reports cannot be relabeled as a node-failure proof',()=>{
  const {code,result}=run([fixture(99,0,1),fixture(1,99,100)],'fault');assert.notEqual(code,0);assert.equal(result.passed,false);
});

test('clock discontinuity invalidates an otherwise successful run',()=>{
  const bad=fixture(99,0,1);bad.clock_discontinuity_detected=true;
  const {code,result}=run([bad,fixture(1,99,100)]);
  assert.notEqual(code,0);assert.equal(result.passed,false);
  assert.ok(result.failures.some(reason=>reason.includes('Clock discontinuity')));
});
