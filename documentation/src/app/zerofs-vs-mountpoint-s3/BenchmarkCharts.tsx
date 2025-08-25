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
    { test: 'Sequential Writes', zerofs: 663.87, mountpoint: 0.70, ratio: 948 },
    { test: 'Data Modifications', zerofs: 695.53, mountpoint: 0, ratio: 'N/A' },
    { test: 'File Append', zerofs: 769.50, mountpoint: 0, ratio: 'N/A' },
    { test: 'Empty Files', zerofs: 888.66, mountpoint: 0.09, ratio: 9874 },
    { test: 'Empty Directories', zerofs: 985.98, mountpoint: 2.08, ratio: 474 },
    { test: 'Random Reads', zerofs: 1000.84, mountpoint: 3.20, ratio: 313 },
  ],
  latency: [
    { test: 'Sequential Writes', zerofs: 1.42, mountpoint: 1435.81, ratio: 1011 },
    { test: 'Data Modifications', zerofs: 1.30, mountpoint: 'N/A', ratio: 'N/A' },
    { test: 'File Append', zerofs: 1.22, mountpoint: 'N/A', ratio: 'N/A' },
    { test: 'Empty Files', zerofs: 0.86, mountpoint: 605.61, ratio: 704 },
    { test: 'Empty Directories', zerofs: 0.98, mountpoint: 479.80, ratio: 490 },
    { test: 'Random Reads', zerofs: 0.90, mountpoint: 312.13, ratio: 347 },
  ],
  reliability: [
    { test: 'Sequential Writes', zerofs: 100, mountpoint: 100 },
    { test: 'Data Modifications', zerofs: 100, mountpoint: 0 },
    { test: 'File Append', zerofs: 100, mountpoint: 0 },
    { test: 'Empty Files', zerofs: 100, mountpoint: 2 },
    { test: 'Empty Directories', zerofs: 100, mountpoint: 100 },
    { test: 'Random Reads', zerofs: 100, mountpoint: 100 },
  ],
  realworld: [
    { test: 'Git Clone', zerofs: '3.1s', mountpoint: 'Failed', ratio: 'N/A' },
    { test: 'TAR Extract (full)', zerofs: '13.5s', mountpoint: '~2h (est.)', ratio: 533 },
    { test: 'TAR Extract (10%)', zerofs: '1.35s', mountpoint: '12m 27s', ratio: 554 },
  ],
  storage: [
    { metric: 'Class A Ops', zerofs: '578', mountpoint: '8.77k', ratio: 15.2 },
    { metric: 'Class B Ops', zerofs: '61', mountpoint: '5.87k', ratio: 96.2 },
  ],
}

export function BenchmarkCharts() {
  const tabs = [
    { name: 'Overview', key: 'overview' },
    { name: 'Throughput', key: 'throughput' },
    { name: 'Latency', key: 'latency' },
    { name: 'Reliability', key: 'reliability' },
    { name: 'Real-World', key: 'realworld' },
    { name: 'Storage', key: 'storage' },
  ]

  return (
    <BenchmarkTabs tabs={tabs}>
      <TabPanel className="space-y-6">
        <div className="prose prose-zinc dark:prose-invert max-w-none">
          <h3>Performance at a Glance</h3>
        </div>

        <div className="space-y-4 rounded-lg border border-zinc-200 bg-zinc-50 p-6 dark:border-zinc-700 dark:bg-zinc-800/50">
          <h4 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Key Performance Differences</h4>
          <ComparisonBar label="Sequential Writes (higher is better)" zerofsValue="664 ops/s" comparisonValue="0.7 ops/s" comparisonLabel="Mountpoint" ratio={948} type="performance" />
          <ComparisonBar label="Empty Files (higher is better)" zerofsValue="889 ops/s" comparisonValue="0.09 ops/s" comparisonLabel="Mountpoint" ratio={9874} type="performance" />
          <ComparisonBar label="Random Reads (higher is better)" zerofsValue="1001 ops/s" comparisonValue="3.2 ops/s" comparisonLabel="Mountpoint" ratio={313} type="performance" />
          <ComparisonBar label="TAR Extract Time (lower is better)" zerofsValue="13.5s" comparisonValue="~2h" comparisonLabel="Mountpoint" ratio={533} type="resource" />
          <ComparisonBar label="S3 API Operations (lower is better)" zerofsValue="0.6k" comparisonValue="14.6k" comparisonLabel="Mountpoint" ratio={23} type="resource" />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Operations Per Second</h3>
            <p>Tests measure sustainable operation rates. Note: Some operations are not supported by Mountpoint-s3.</p>
          </div>

          <PerformanceIndicator type="higher" />

          <SimpleTable
            data={benchmarkData.throughput}
            columns={[
              { key: 'test', label: 'Test' },
              { key: 'zerofs', label: 'ZeroFS (ops/sec)' },
              {
                key: 'mountpoint',
                label: 'Mountpoint-s3 (ops/sec)',
                className: (val: number) => val === 0 ? 'text-zinc-400 dark:text-zinc-500 italic' : 'text-zinc-700 dark:text-zinc-300'
              },
              {
                key: 'ratio',
                label: 'Multiplier',
                className: (val: any) => {
                  if (val === 'N/A') return 'text-zinc-400 dark:text-zinc-500'
                  return val > 100 ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
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
            <p>Time per individual operation in milliseconds.</p>
          </div>

          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.latency}
            columns={[
              { key: 'test', label: 'Test' },
              { key: 'zerofs', label: 'ZeroFS (ms)' },
              {
                key: 'mountpoint',
                label: 'Mountpoint-s3 (ms)',
                className: (val: any) => val === 'N/A' ? 'text-zinc-400 dark:text-zinc-500 italic' : 'text-zinc-700 dark:text-zinc-300'
              },
              {
                key: 'ratio',
                label: 'Multiplier',
                className: (val: any) => {
                  if (val === 'N/A') return 'text-zinc-400 dark:text-zinc-500'
                  return val > 100 ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
                }
              },
            ]}
          />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Operation Success Rate</h3>
            <p>Percentage of operations that completed successfully.</p>
          </div>

          <PerformanceIndicator type="higher" />

          <SimpleTable
            data={benchmarkData.reliability}
            columns={[
              { key: 'test', label: 'Test' },
              {
                key: 'zerofs',
                label: 'ZeroFS (%)',
                className: () => 'font-mono text-green-600 dark:text-green-400'
              },
              {
                key: 'mountpoint',
                label: 'Mountpoint-s3 (%)',
                className: (val: number) => {
                  if (val === 0) return 'font-mono text-red-600 dark:text-red-400'
                  if (val < 50) return 'font-mono text-amber-600 dark:text-amber-400'
                  return 'font-mono text-green-600 dark:text-green-400'
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
            <p>Common development and deployment tasks.</p>
          </div>

          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.realworld}
            columns={[
              { key: 'test', label: 'Operation' },
              { key: 'zerofs', label: 'ZeroFS' },
              {
                key: 'mountpoint',
                label: 'Mountpoint-s3',
                className: (val: string) => val === 'Failed' ? 'text-red-600 dark:text-red-400 font-medium' : 'text-zinc-700 dark:text-zinc-300'
              },
              {
                key: 'ratio',
                label: 'Multiplier',
                className: (val: any) => {
                  if (val === 'N/A') return 'text-zinc-400 dark:text-zinc-500'
                  return val > 100 ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
                }
              },
            ]}
          />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Storage & API Efficiency</h3>
            <p>S3 API operations consumed for benchmark completion.</p>
          </div>

          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.storage}
            columns={[
              { key: 'metric', label: 'Metric' },
              { key: 'zerofs', label: 'ZeroFS' },
              { key: 'mountpoint', label: 'Mountpoint-s3' },
              {
                key: 'ratio',
                label: 'Multiplier',
                className: (val: number) => val > 10 ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
              },
            ]}
          />
        </div>
      </TabPanel>
    </BenchmarkTabs>
  )
}