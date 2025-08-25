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
    { test: 'Sequential Writes', zerofs: 988.18, azure: 28.32, ratio: 34.9 },
    { test: 'Data Modifications', zerofs: 1063.00, azure: 67.64, ratio: 15.7 },
    { test: 'File Append', zerofs: 1118.04, azure: 67.31, ratio: 16.6 },
    { test: 'Empty Files', zerofs: 1374.44, azure: 33.12, ratio: 41.5 },
    { test: 'Empty Directories', zerofs: 1597.71, azure: 41.08, ratio: 38.9 },
    { test: 'Random Reads', zerofs: 1389.42, azure: 327.18, ratio: 4.2 },
  ],
  latency: [
    { test: 'Sequential Writes', zerofs: 1.01, azure: 35.28, ratio: 34.9 },
    { test: 'Data Modifications', zerofs: 0.94, azure: 14.78, ratio: 15.7 },
    { test: 'File Append', zerofs: 0.89, azure: 14.85, ratio: 16.7 },
    { test: 'Empty Files', zerofs: 0.53, azure: 30.17, ratio: 56.9 },
    { test: 'Empty Directories', zerofs: 0.62, azure: 24.33, ratio: 39.2 },
    { test: 'Random Reads', zerofs: 0.72, azure: 3.05, ratio: 4.2 },
  ],
  realworld: [
    { test: 'Git Clone', zerofs: '2.2s', azure: '18.3s', ratio: 8.3 },
    { test: 'Cargo Build', zerofs: '2m 03s', azure: 'Failed', ratio: 'N/A' },
    { test: 'TAR Extract', zerofs: '9.1s', azure: '2m 41.6s', ratio: 17.8 },
  ],
  storage: [
    { metric: '100 GB/month', zerofs: '$1.96', azure: '$2.71', ratio: 1.4 },
    { metric: '1 TB/month', zerofs: '$19.60', azure: '$27.10', ratio: 1.4 },
    { metric: '10 TB/month', zerofs: '$196.00', azure: '$271.00', ratio: 1.4 },
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
          <ComparisonBar label="Sequential Writes (higher is better)" zerofsValue="988 ops/s" comparisonValue="28 ops/s" comparisonLabel="Azure" ratio={34.9} type="performance" />
          <ComparisonBar label="Data Modifications (higher is better)" zerofsValue="1063 ops/s" comparisonValue="68 ops/s" comparisonLabel="Azure" ratio={15.7} type="performance" />
          <ComparisonBar label="File Append (higher is better)" zerofsValue="1118 ops/s" comparisonValue="67 ops/s" comparisonLabel="Azure" ratio={16.6} type="performance" />
          <ComparisonBar label="Empty Files (higher is better)" zerofsValue="1374 ops/s" comparisonValue="33 ops/s" comparisonLabel="Azure" ratio={41.5} type="performance" />
          <ComparisonBar label="Empty Directories (higher is better)" zerofsValue="1598 ops/s" comparisonValue="41 ops/s" comparisonLabel="Azure" ratio={38.9} type="performance" />
          <ComparisonBar label="Random Reads (higher is better)" zerofsValue="1389 ops/s" comparisonValue="327 ops/s" comparisonLabel="Azure" ratio={4.2} type="performance" />
          <ComparisonBar label="Git Clone (lower is better)" zerofsValue="2.2s" comparisonValue="18.3s" comparisonLabel="Azure" ratio={8.3} type="performance" />
          <ComparisonBar label="Cargo Build" zerofsValue="2m 03s" comparisonValue="Failed" comparisonLabel="Azure" ratio="N/A" type="performance" comparisonColor="red" />
          <ComparisonBar label="TAR Extract (lower is better)" zerofsValue="9.1s" comparisonValue="2m 41.6s" comparisonLabel="Azure" ratio={17.8} type="performance" />
          <ComparisonBar label="Storage Cost per GB (lower is better)" zerofsValue="2.0¢" comparisonValue="2.7¢" comparisonLabel="Azure" ratio={1.4} type="cost" />
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
              { key: 'azure', label: 'Azure Files (ops/sec)' },
              {
                key: 'ratio',
                label: 'Factor',
                className: (val: number) => {
                  if (val > 10) return 'font-bold text-green-600 dark:text-green-400'
                  if (val > 1.5) return 'text-green-600 dark:text-green-400'
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
              { key: 'azure', label: 'Azure Files (ms)' },
              {
                key: 'ratio',
                label: 'Factor',
                className: (val: number) => {
                  if (val > 10) return 'font-bold text-green-600 dark:text-green-400'
                  if (val > 1.5) return 'text-green-600 dark:text-green-400'
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
              {
                key: 'azure',
                label: 'Azure Files',
                className: (val: string) => val === 'Failed' ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
              },
              {
                key: 'ratio',
                label: 'Factor',
                className: (val: number | string) => {
                  if (val === 'N/A') return 'text-zinc-500 dark:text-zinc-400'
                  const numVal = Number(val)
                  if (numVal > 10) return 'font-bold text-green-600 dark:text-green-400'
                  if (numVal > 1.2) return 'text-green-600 dark:text-green-400'
                  if (numVal < 0.9) return 'font-bold text-amber-600 dark:text-amber-400'
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
            <p>Monthly storage costs based on Azure pricing (West Europe region).</p>
          </div>

          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.storage}
            columns={[
              { key: 'metric', label: 'Storage Size' },
              { key: 'zerofs', label: 'ZeroFS (Blob Storage)' },
              { key: 'azure', label: 'Azure Files' },
              {
                key: 'ratio',
                label: 'Azure Files is X times more',
                className: () => 'font-bold text-amber-600 dark:text-amber-400'
              },
            ]}
          />
        </div>
      </TabPanel>
    </BenchmarkTabs>
  )
}
