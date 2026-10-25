import fs from 'node:fs';
const [input,output] = process.argv.slice(2);
if(!input||!output) throw new Error('Usage: resource-summary.mjs INPUT.jsonl OUTPUT.json');
const containers={};
let samples=0,errors=0;
const maxima={app_cpu_percent:0,app_memory_mib:0,driver_cpu_percent:0,driver_memory_mib:0};
function mib(text) {
  const match=text.split('/')[0].trim().match(/^([\d.]+)([A-Za-z]+)$/);
  const units={B:1/1048576,kB:1000/1048576,KiB:1/1024,MB:1000000/1048576,MiB:1,GB:1000000000/1048576,GiB:1024};
  return match&&units[match[2]]!==undefined?Number(match[1])*units[match[2]]:NaN;
}
for(const line of fs.readFileSync(input,'utf8').replace(/^\uFEFF/,'').split(/\r?\n/).filter(Boolean)) {
  const sample=JSON.parse(line);
  if(sample.exit_code!==0) {errors++;continue;}
  samples++;
  const totals={app_cpu_percent:0,app_memory_mib:0,driver_cpu_percent:0,driver_memory_mib:0};
  for(const raw of sample.stats.trim().split(/\r?\n/).filter(Boolean)) {
    let row; try {row=JSON.parse(raw);} catch {errors++;continue;}
    const cpu=parseFloat(row.CPUPerc),memory=mib(row.MemUsage);
    if(!Number.isFinite(cpu)||!Number.isFinite(memory)) {errors++;continue;}
    const bucket=containers[row.Name]??={samples:0,max_cpu_percent:0,max_memory_mib:0};
    bucket.samples++;
    bucket.max_cpu_percent=Math.max(bucket.max_cpu_percent,cpu);
    bucket.max_memory_mib=Math.max(bucket.max_memory_mib,memory);
    const type=/^phi-messaging-app[123]-1$/.test(row.Name)?'app':/^phi-messaging-(users-|loadgen-run)/.test(row.Name)?'driver':null;
    if(type) {totals[`${type}_cpu_percent`]+=cpu;totals[`${type}_memory_mib`]+=memory;}
  }
  for(const key of Object.keys(maxima)) maxima[key]=Math.max(maxima[key],totals[key]);
}
fs.writeFileSync(output,JSON.stringify({source:input,samples,errors,containers,same_sample_group_maxima:maxima,
  basis:'Sampled whole-run CPU/memory; 100% CPU equals one logical CPU. Group maxima sum simultaneous rows, not independent per-container peaks. Includes setup and teardown.'},null,2)+'\n',{flag:'wx'});
console.log(`Saved ${output}`);
