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
    { test: 'Sequential Writes', zerofs: 984.29, juicefs: 5.62, ratio: 175 },
    { test: 'Data Modifications', zerofs: 1098.62, juicefs: 5.98, ratio: 183 },
    { test: 'File Append', zerofs: 1203.56, juicefs: 5.29, ratio: 227 },
    { test: 'Empty Files', zerofs: 1350.66, juicefs: 1150.57, ratio: 1.17 },
  ],
  latency: [
    { test: 'Sequential Writes', zerofs: 1.01, juicefs: 177.76, ratio: 176 },
    { test: 'Data Modifications', zerofs: 0.91, juicefs: 166.25, ratio: 183 },
    { test: 'File Append', zerofs: 0.83, juicefs: 186.16, ratio: 224 },
    { test: 'Empty Files', zerofs: 0.59, juicefs: 0.83, ratio: 1.4 },
  ],
  reliability: [
    { test: 'Sequential Writes', zerofs: 100, juicefs: 100 },
    { test: 'Data Modifications', zerofs: 100, juicefs: 7.94 },
    { test: 'File Append', zerofs: 100, juicefs: 2.57 },
    { test: 'Empty Files', zerofs: 100, juicefs: 100 },
  ],
  realworld: [
    { test: 'Git Clone', zerofs: '2.6s', juicefs: '34.4s', ratio: 13 },
    { test: 'Cargo Build', zerofs: '3m 4s', juicefs: '>69m', ratio: 22 },
    { test: 'TAR Extract', zerofs: '8.2s', juicefs: '10m 26s', ratio: 76 },
  ],
  storage: [
    { metric: 'Bucket Size', zerofs: '7.57 GB', juicefs: '238.99 GB', ratio: 31.6 },
    { metric: 'Class A Ops', zerofs: '6.15k', juicefs: '359.21k', ratio: 58.4 },
    { metric: 'Class B Ops', zerofs: '1.84k', juicefs: '539.3k', ratio: 293 },
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
          <ComparisonBar label="Sequential Writes (higher is better)" zerofsValue="984 ops/s" comparisonValue="5.6 ops/s" comparisonLabel="JuiceFS" ratio={175} type="performance" comparisonColor="red" />
          <ComparisonBar label="Data Modifications (higher is better)" zerofsValue="1099 ops/s" comparisonValue="6 ops/s" comparisonLabel="JuiceFS" ratio={183} type="performance" comparisonColor="red" />
          <ComparisonBar label="File Append (higher is better)" zerofsValue="1204 ops/s" comparisonValue="5.3 ops/s" comparisonLabel="JuiceFS" ratio={227} type="performance" comparisonColor="red" />
          <ComparisonBar label="Empty Files (higher is better)" zerofsValue="1351 ops/s" comparisonValue="1151 ops/s" comparisonLabel="JuiceFS" ratio={1.17} type="performance" comparisonColor="red" />
          <ComparisonBar label="Git Clone (lower is better)" zerofsValue="2.6s" comparisonValue="34.4s" comparisonLabel="JuiceFS" ratio={13} type="performance" comparisonColor="red" />
          <ComparisonBar label="Cargo Build (lower is better)" zerofsValue="3m 4s" comparisonValue=">69m" comparisonLabel="JuiceFS" ratio={22} type="performance" comparisonColor="red" />
          <ComparisonBar label="TAR Extract (lower is better)" zerofsValue="8.2s" comparisonValue="10m 26s" comparisonLabel="JuiceFS" ratio={76} type="performance" comparisonColor="red" />
          <ComparisonBar label="Storage Used (lower is better)" zerofsValue="7.6 GB" comparisonValue="239 GB" comparisonLabel="JuiceFS" ratio={31.6} type="resource" comparisonColor="red" />
          <ComparisonBar label="API Operations (lower is better)" zerofsValue="8k" comparisonValue="898k" comparisonLabel="JuiceFS" ratio={112} type="resource" comparisonColor="red" />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Operations Per Second</h3>
            <p>Tests measure sustainable operation rates.</p>
          </div>
          
          <PerformanceIndicator type="higher" />

          <SimpleTable
            data={benchmarkData.throughput}
            columns={[
              { key: 'test', label: 'Test' },
              { key: 'zerofs', label: 'ZeroFS (ops/sec)' },
              { key: 'juicefs', label: 'JuiceFS (ops/sec)' },
              {
                key: 'ratio',
                label: 'Multiplier',
                className: (val: number) => val > 10 ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
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
              { key: 'juicefs', label: 'JuiceFS (ms)' },
              {
                key: 'ratio',
                label: 'Multiplier',
                className: (val: number) => val > 10 ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
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
                key: 'juicefs',
                label: 'JuiceFS (%)',
                className: (val: number) => val < 50 ? 'font-mono text-red-600 dark:text-red-400' : 'font-mono text-zinc-700 dark:text-zinc-300'
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
              { key: 'juicefs', label: 'JuiceFS' },
              {
                key: 'ratio',
                label: 'Multiplier',
                className: (val: number) => val > 10 ? 'font-bold text-red-600 dark:text-red-400' : 'text-zinc-700 dark:text-zinc-300'
              },
            ]}
          />
        </div>
      </TabPanel>

      <TabPanel>
        <div className="space-y-6">
          <div className="prose prose-zinc dark:prose-invert max-w-none">
            <h3>Storage & API Efficiency</h3>
            <p>Resource consumption for identical workloads.</p>
          </div>
          
          <PerformanceIndicator type="lower" />

          <SimpleTable
            data={benchmarkData.storage}
            columns={[
              { key: 'metric', label: 'Metric' },
              { key: 'zerofs', label: 'ZeroFS' },
              { key: 'juicefs', label: 'JuiceFS' },
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