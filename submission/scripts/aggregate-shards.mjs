import fs from 'node:fs';
const read = file => JSON.parse(fs.readFileSync(file,'utf8').replace(/^\uFEFF/,''));
const [prefix, countText, output, faultText] = process.argv.slice(2);
if (!prefix || !output) throw new Error('Usage: aggregate-shards.mjs PREFIX SHARDS OUTPUT [fault]');
const reports = Array.from({length:Number(countText)},(_,i)=>read(`docs/results/${prefix}-shard${i}.json`));
function mergeHistogram(field, expected) {
  const bins = new Map();
  for (const report of reports) for (const [key,value] of Object.entries(report[field] ?? {})) bins.set(Number(key),(bins.get(Number(key)) ?? 0)+value);
  const rows = [...bins].sort((a,b)=>a[0]-b[0]);
  const total = rows.reduce((n,[,value])=>n+value,0);
  if (total !== expected) throw new Error(`Histogram sample mismatch: ${field}`);
  const percentile = fraction => { let n=0; for(const [bound,value] of rows) { n+=value; if(n>=Math.ceil(total*fraction)) return bound; } return null; };
  return { samples:total,p50_ms_upper_bound:percentile(.5),p95_ms_upper_bound:percentile(.95),p99_ms_upper_bound:percentile(.99),max_ms_upper_bound:percentile(1),resolution_ms:1 };
}
const sum = field => reports.reduce((n,r)=>n+r[field],0);
const starts = reports.map(r=>Date.parse(r.measurement_started_at));
const ends = reports.map(r=>Date.parse(r.measurement_ends_at));
const overlap = (Math.min(...ends)-Math.max(...starts))/1000;
const elapsed = (Math.max(...reports.map((r,i)=>starts[i]+r.measurement_and_drain_seconds*1000))-Math.min(...starts))/1000;
const ranges = reports.map(r=>[r.config.user_offset,r.config.user_offset+r.config.connections]).sort((a,b)=>a[0]-b[0]);
let covered=0;
for(const [start,end] of ranges) { if(start!==covered) throw new Error('User ranges overlap or have a gap'); covered=end; }
if(reports.some(r=>!r.config.distinct_user_per_recipient)) throw new Error('Shard does not simulate distinct users');
const fault = faultText==='fault';
const errors=[];
if(reports.some(r=>r.clock_discontinuity_detected===true)) errors.push('Clock discontinuity detected during measurement');
if(reports.some(r=>r.passed!==true)) errors.push('At least one shard failed its connection/delivery/durability assertions');
if(reports.some(r=>r.config.measurement_ns!==reports[0].config.measurement_ns)) errors.push('Shard measurement durations differ');
for(const field of ['missing_measured_deliveries','missing_warmup_deliveries','unacknowledged_logical_messages','scheduler_queue_drops','unscheduled_rate_slots','duplicate_callbacks','client_terminal_errors','never_connected']) {
  if(reports.some(r=>!Number.isInteger(r[field])||r[field]!==0)) errors.push(`Nonzero or unavailable failure counter: ${field}`);
}
if(reports.some(r=>r.expected_measured_deliveries!==r.received_measured_deliveries||r.recipients_with_measured_delivery!==r.config.connections)) errors.push('Not every intended recipient delivery was observed');
if(reports.some(r=>r.history_audit.error||r.history_audit.missing_or_changed_acknowledged!==0||r.history_audit.acknowledged_checked<r.measured_acknowledged_messages)) errors.push('Durability audit did not verify all acknowledgements');
if(fault&&!(reports[0].config.kill_after_ns>0&&reports[0].kill_to_affected_reconnect_ms>0)) errors.push('Fault scenario has no verified affected-client recovery');
if(!Number.isFinite(overlap) || overlap<reports[0].config.measurement_ns/1e9-1) errors.push('Measurement windows did not overlap sufficiently');
if(!fault && reports.some(r=>r.min_resumed_during_measurement!==r.config.connections)) errors.push('Requested user count was not maintained throughout the measurement');
const result={
  clock_checks:reports.map(r=>({discontinuity:r.clock_discontinuity_detected??null,wall_seconds:r.wall_measurement_and_drain_seconds??null,monotonic_seconds:r.measurement_and_drain_seconds})),
  passed:errors.length===0,failures:errors,distinct_recipient_users:covered,shards:reports.length,
  recipients_with_measured_delivery:sum('recipients_with_measured_delivery'),connected_at_measurement_start:sum('connected_at_measurement_start'),
  global_aggregate_sends_per_second:reports.reduce((n,r)=>n+r.config.logical_messages_per_second,0),
  rate_scope:'sum of generator rates across workload; never per user',
  concurrent_measurement_overlap_seconds:overlap,measurement_and_drain_seconds:elapsed,
  sum_of_per_shard_minimum_connections:sum('min_resumed_during_measurement'),
  expected_deliveries:sum('expected_measured_deliveries'),received_deliveries:sum('received_measured_deliveries'),
  missing_deliveries:sum('missing_measured_deliveries'),missing_warmup_deliveries:sum('missing_warmup_deliveries'),
  measured_acknowledged_messages:sum('measured_acknowledged_messages'),
  http_attempt_errors:sum('http_attempt_errors'),http_recovery_retries:sum('http_recovery_retries'),
  connection_failures:sum('websocket_establishment_failures'),connection_retries:sum('connection_retries'),
  unacknowledged_messages:sum('unacknowledged_logical_messages'),scheduler_drops:sum('scheduler_queue_drops'),
  duplicate_callbacks:sum('duplicate_callbacks'),suppressed_duplicates:sum('suppressed_duplicates'),
  recipient_deliveries_per_second:sum('received_measured_deliveries')/elapsed,
  acknowledged_messages_per_second:sum('measured_acknowledged_messages')/elapsed,
  delivery_latency:mergeHistogram('delivery_latency_ms_upper_bound_histogram',sum('received_measured_deliveries')),
  ack_latency:mergeHistogram('ack_latency_ms_upper_bound_histogram',sum('measured_acknowledged_messages')),
  durable_acknowledgements_checked:reports.reduce((n,r)=>n+r.history_audit.acknowledged_checked,0),
  durable_missing_or_changed:reports.reduce((n,r)=>n+r.history_audit.missing_or_changed_acknowledged,0),
  audit_errors:reports.map(r=>r.history_audit.error).filter(Boolean),
  fault_requested:fault,pinned_client_recovery_ms:reports[0].kill_to_affected_reconnect_ms ?? null,
  sources:reports.map((_,i)=>`docs/results/${prefix}-shard${i}.json`),
  latency_basis:'Local monotonic durations in each process; merged 1ms histogram counts, not averaged percentiles. Missing deliveries remain errors.',
  overlap_basis:'Shared filesystem barrier and wall timestamps within the same Docker VM; wall timestamps are used only to verify concurrent windows, never to compute delivery latency.',
};
fs.writeFileSync(output,JSON.stringify(result,null,2)+'\n',{flag:'wx'});
console.log(`${result.passed?'PASS':'FAIL'}: ${output}`);
if(!result.passed) process.exitCode=1;
