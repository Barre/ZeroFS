'use client'

import React from 'react'
import {
  BenchmarkTabs,
  TabPanel,
  ComparisonBar,
  SimpleTable,
  PerformanceIndicator
} from '@/components/BenchmarkComponents'

const benchmarkData = {
  throughput: [
    { test: 'Sequential Writes', zerofs: 230.71, efs: 108.13, ratio: 2.1 },
    { test: 'Data Modifications', zerofs: 308.60, efs: 181.52, ratio: 1.7 },
    { test: 'File Append', zerofs: 300.91, efs: 91.11, ratio: 3.3 },
    { test: 'Empty Files', zerofs: 294.87, efs: 195.61, ratio: 1.5 },
    { test: 'Empty Directories', zerofs: 446.61, efs: 219.01, ratio: 2.0 },
    { test: 'Random Reads', zerofs: 415.68, efs: 1242.61, ratio: 0.33 },
  ],
  latency: [
    { test: 'Sequential Writes', zerofs: 4.32, efs: 9.22, ratio: 2.1 },
    { test: 'Data Modifications', zerofs: 3.23, efs: 5.50, ratio: 1.7 },
    { test: 'File Append', zerofs: 3.31, efs: 10.95, ratio: 3.3 },
    { test: 'Empty Files', zerofs: 2.74, efs: 4.62, ratio: 1.7 },
    { test: 'Empty Directories', zerofs: 2.23, efs: 4.55, ratio: 2.0 },
    { test: 'Random Reads', zerofs: 2.40, efs: 0.80, ratio: 0.33 },
  ],
  realworld: [
    { test: 'Git Clone', zerofs: '3.9s', efs: '5.0s', ratio: 1.3 },
    { test: 'Cargo Build', zerofs: '5m 45s', efs: '4m 56s', ratio: 0.86 },
    { test: 'TAR Extract', zerofs: '40.0s', efs: '88.1s', ratio: 2.2 },
  ],
  storage: [
    { metric: '100 GB/month', zerofs: '$2.30', efs: '$30.00', ratio: 13 },
    { metric: '1 TB/month', zerofs: '$23.00', efs: '$300.00', ratio: 13 },
    { metric: '10 TB/month', zerofs: '$230.00', efs: '$3,000.00', ratio: 13 },
  ],
}

export function BenchmarkCharts() {
  const tabs = [
    { name: 'Overview', key: 'overview' },
    { name: 'Throughput', key: 'throughput' },
    { name: 'Latency', key: 'latency' },
    { name: 'Real-World', key: 'realworld' },
    { name: 'Cost', key: 'cost' },
  ]

  return (
    <BenchmarkTabs tabs={tabs}>
      <TabPanel className="space-y-6">
        <div className="prose prose-zinc dark:prose-invert max-w-none">
          <h3>Performance at a Glance</h3>
        </div>

        <div className="space-y-4 rounded-lg border border-zinc-200 bg-zinc-50 p-6 dark:border-zinc-700 dark:bg-zinc-800/50">
          <h4 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Key Performance Differences</h4>
          <ComparisonBar label="Sequential Writes (higher is better)" zerofsValue="231 ops/s" comparisonValue="108 ops/s" comparisonLabel="EFS" ratio={2.1} type="performance" />
          <ComparisonBar label="Data Modifications (higher is better)" zerofsValue="309 ops/s" comparisonValue="182 ops/s" comparisonLabel="EFS" ratio={1.7} type="performance" />
          <ComparisonBar label="File Append (higher is better)" zerofsValue="301 ops/s" comparisonValue="91 ops/s" comparisonLabel="EFS" ratio={3.3} type="performance" />
          <ComparisonBar label="Empty Files (higher is better)" zerofsValue="295 ops/s" comparisonValue="196 ops/s" comparisonLabel="EFS" ratio={1.5} type="performance" />
          <ComparisonBar label="Empty Directories (higher is better)" zerofsValue="447 ops/s" comparisonValue="219 ops/s" comparisonLabel="EFS" ratio={2.0} type="performance" />
          <ComparisonBar label="Random Reads (higher is better)" zerofsValue="416 ops/s" comparisonValue="1243 ops/s" comparisonLabel="EFS" ratio={0.33} type="performance" />
          <ComparisonBar label="Git Clone (lower is better)" zerofsValue="3.9s" comparisonValue="5.0s" comparisonLabel="EFS" ratio={1.3} type="performance" />
          <ComparisonBar label="Cargo Build (lower is better)" zerofsValue="5m 45s" comparisonValue="4m 56s" comparisonLabel="EFS" ratio={0.86} type="performance" />
          <ComparisonBar label="TAR Extract (lower is better)" zerofsValue="40s" comparisonValue="88s" comparisonLabel="EFS" ratio={2.2} type="performance" />
          <ComparisonBar label="Storage Cost per GB (lower is better)" zerofsValue="2.3¢" comparisonValue="30¢" comparisonLabel="EFS" ratio={13} type="cost" />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Operations Per Second</h3>
            <p>Higher values indicate better performance.</p>
          </div>

          <PerformanceIndicator type="higher" />

          <SimpleTable
            data={benchmarkData.throughput}
            columns={[
              { key: 'test', label: 'Test' },
              { key: 'zerofs', label: 'ZeroFS (ops/sec)' },
              { key: 'efs', label: 'AWS EFS (ops/sec)' },
              {
                key: 'ratio',
                label: 'Factor',
                className: (val: number) => {
                  if (val > 1.5) return 'font-bold text-green-600 dark:text-green-400'
                  if (val < 0.7) return 'font-bold text-amber-600 dark:text-amber-400'
                  return 'text-zinc-700 dark:text-zinc-300'
                }
              },
            ]}
          />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Operation Latency</h3>
            <p>Time per operation in milliseconds. Lower is better.</p>
          </div>

          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.latency}
            columns={[
              { key: 'test', label: 'Test' },
              { key: 'zerofs', label: 'ZeroFS (ms)' },
              { key: 'efs', label: 'AWS EFS (ms)' },
              {
                key: 'ratio',
                label: 'Factor',
                className: (val: number) => {
                  if (val > 1.5) return 'font-bold text-green-600 dark:text-green-400'
                  if (val < 0.7) return 'font-bold text-amber-600 dark:text-amber-400'
                  return 'text-zinc-700 dark:text-zinc-300'
                }
              },
            ]}
          />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Real-World Operations</h3>
            <p>Common development tasks.</p>
          </div>

          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.realworld}
            columns={[
              { key: 'test', label: 'Operation' },
              { key: 'zerofs', label: 'ZeroFS' },
              { key: 'efs', label: 'AWS EFS' },
              {
                key: 'ratio',
                label: 'Factor',
                className: (val: number) => {
                  if (val > 1.2) return 'font-bold text-green-600 dark:text-green-400'
                  if (val < 0.9) return 'font-bold text-amber-600 dark:text-amber-400'
                  return 'text-zinc-700 dark:text-zinc-300'
                }
              },
            ]}
          />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Storage Cost Comparison</h3>
            <p>Monthly storage costs. Does not include data transfer fees for EFS.</p>
          </div>

          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.storage}
            columns={[
              { key: 'metric', label: 'Storage Size' },
              { key: 'zerofs', label: 'ZeroFS (S3)' },
              { key: 'efs', label: 'AWS EFS' },
              {
                key: 'ratio',
                label: 'EFS is X times more',
                className: () => 'font-bold text-red-600 dark:text-red-400'
              },
            ]}
          />
        </div>
      </TabPanel>
    </BenchmarkTabs>
  )
}