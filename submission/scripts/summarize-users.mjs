import fs from 'node:fs';
import path from 'node:path';

const read = file => JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''));
const [statusFile, output] = process.argv.slice(2);
if (!statusFile || !output) throw new Error('Usage: node scripts/summarize-users.mjs STATUS.json OUTPUT.json');
const status = read(statusFile);
const prefix = statusFile.replace(/-status\.json$/, '');
const memoryMiB = value => {
  const match = value.split('/')[0].trim().match(/^([\d.]+)([A-Za-z]+)$/);
  if (!match) return null;
  const multiplier = { B: 1/1048576, kB: 1000/1048576, KiB: 1/1024, MB: 1000000/1048576, MiB: 1, GB: 1000000000/1048576, GiB: 1024 }[match[2]];
  return multiplier === undefined ? null : Number(match[1])*multiplier;
};
const stages = status.stages.map(stage => {
  const result = { ...stage };
  if (!stage.report || !fs.existsSync(stage.report)) return result;
  const report = read(stage.report);
  Object.assign(result, {
    config: report.config, distinct_recipient_users: report.distinct_recipient_users,
    conversations: report.conversation_count,
    connected_at_start: report.connected_at_measurement_start,
    min_connected: report.min_resumed_during_measurement, peak_connected: report.max_simultaneously_resumed,
    recipients_with_measured_delivery: report.recipients_with_measured_delivery,
    expected: report.expected_measured_deliveries, delivered: report.received_measured_deliveries,
    missing: report.missing_measured_deliveries, missing_warmup: report.missing_warmup_deliveries,
    connection_failures: report.websocket_establishment_failures, retries: report.connection_retries,
    http_attempt_errors: report.http_attempt_errors, unacknowledged: report.unacknowledged_logical_messages,
    duplicate_callbacks: report.duplicate_callbacks, suppressed_duplicates: report.suppressed_duplicates,
    delivery_latency: report.recipient_delivery_latency, ack_latency: report.sender_ack_latency,
    sender_throughput: report.acknowledged_messages_per_second,
    recipient_throughput: report.recipient_deliveries_per_second_including_drain,
    scheduler_lateness: report.scheduler_lateness, audit: report.history_audit,
    passed: report.passed, failure: report.failure,
  });
  result.resources = {};
  const resourceFile = `${prefix}-${stage.users}-resources.jsonl`;
  if (fs.existsSync(resourceFile)) {
    for (const line of fs.readFileSync(resourceFile, 'utf8').replace(/^\uFEFF/, '').split(/\r?\n/).filter(Boolean)) {
      const sample = JSON.parse(line);
      if (sample.exit_code !== 0) { result.resource_sample_errors = (result.resource_sample_errors ?? 0)+1; continue; }
      for (const row of sample.stats.trim().split(/\r?\n/).filter(Boolean)) {
        const metric = JSON.parse(row);
        const bucket = result.resources[metric.Name] ??= { samples: 0, max_cpu_percent: 0, max_memory_mib: 0 };
        bucket.samples++;
        bucket.max_cpu_percent = Math.max(bucket.max_cpu_percent, parseFloat(metric.CPUPerc));
        const memory = memoryMiB(metric.MemUsage);
        if (memory !== null) bucket.max_memory_mib = Math.max(bucket.max_memory_mib, memory);
      }
    }
  }
  return result;
});
fs.writeFileSync(output, JSON.stringify({
  source_status: path.normalize(statusFile), target_users: status.target_distinct_recipient_users,
  status: status.status, stages,
  latency_basis: 'Monotonic elapsed time in one Linux generator, first send HTTP attempt to recipient callback; percentiles cover received messages only. Missing messages and errors are reported separately and fail assertions.',
  resource_basis: 'Sampled Docker CPU (100% equals one logical CPU) and memory maxima per container; not continuous peaks. Older collectors sampled only the six service containers; newer collectors include the generator and other running containers explicitly by name.',
}, null, 2)+'\n', { flag: 'wx' });
console.log(`Saved ${output}`);
