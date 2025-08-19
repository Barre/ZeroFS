'use client'

import React from 'react'
import { Tab, TabGroup, TabList, TabPanel, TabPanels } from '@headlessui/react'
import clsx from 'clsx'

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

function MetricCard({ label, value, unit, color = 'blue' }: {
  label: string
  value: string | number
  unit?: string
  color?: 'blue' | 'red' | 'amber'
}) {
  const colorClasses = {
    blue: 'text-blue-600 dark:text-blue-400',
    red: 'text-red-600 dark:text-red-400',
    amber: 'text-amber-600 dark:text-amber-400',
  }

  return (
    <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-800">
      <div className="text-xs font-medium text-zinc-500 dark:text-zinc-400">{label}</div>
      <div className={`mt-1 text-2xl font-bold ${colorClasses[color]}`}>{value}</div>
      {unit && <div className="text-xs text-zinc-600 dark:text-zinc-400">{unit}</div>}
    </div>
  )
}

function ComparisonBar({ label, zerofs, juicefs, ratio, type = 'performance' }: {
  label: string
  zerofs: string
  juicefs: string
  ratio: number
  type?: 'performance' | 'resource'
}) {
  // Parse numeric values from strings like "984 ops/s" -> 984
  const parseValue = (val: string) => {
    const num = parseFloat(val.replace(/[^0-9.]/g, ''))
    return isNaN(num) ? 0 : num
  }

  const zerofsVal = parseValue(zerofs)
  const juicefsVal = parseValue(juicefs)

  // For performance metrics (ops/s), higher is better
  // For resource metrics (GB, operations), lower is better
  const isHigherBetter = type === 'performance'
  const maxVal = Math.max(zerofsVal, juicefsVal)

  // Calculate bar widths proportionally
  const zerofsWidth = Math.max((zerofsVal / maxVal) * 300, 5)
  const juicefsWidth = Math.max((juicefsVal / maxVal) * 300, 5)

  return (
    <div className="space-y-2">
      <div className="flex justify-between text-sm">
        <span className="font-medium text-zinc-700 dark:text-zinc-300">{label}</span>
        {ratio > 1.5 && (
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            JuiceFS: {ratio}x {isHigherBetter ? 'slower' : 'more'}
          </span>
        )}
      </div>
      <div className="space-y-1 text-xs">
        <div className="flex items-center gap-2">
          <span className="w-16 text-right text-zinc-600 dark:text-zinc-400">ZeroFS</span>
          <div
            className="h-6 bg-blue-500 dark:bg-blue-600 rounded"
            style={{ width: `${zerofsWidth}px` }}
          />
          <span className="font-mono text-zinc-700 dark:text-zinc-300">{zerofs}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="w-16 text-right text-zinc-600 dark:text-zinc-400">JuiceFS</span>
          <div
            className="h-6 bg-red-500 dark:bg-red-600 rounded"
            style={{ width: `${juicefsWidth}px` }}
          />
          <span className="font-mono text-zinc-700 dark:text-zinc-300">{juicefs}</span>
        </div>
      </div>
    </div>
  )
}

interface TableColumn {
  key: string
  label: string
  className?: (value: any) => string
}

function SimpleTable({ data, columns }: {
  data: any[]
  columns: TableColumn[]
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-zinc-200 dark:border-zinc-700">
            {columns.map((col: any) => (
              <th key={col.key} className="px-3 py-2 text-left font-medium text-zinc-700 dark:text-zinc-300">
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row: any, i: number) => (
            <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800">
              {columns.map((col: any) => (
                <td key={col.key} className="px-3 py-2">
                  <span className={col.className?.(row[col.key]) || 'text-zinc-700 dark:text-zinc-300'}>
                    {row[col.key]}
                  </span>
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
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
    <div className="my-8 space-y-6">
      <TabGroup>
        <TabList className="not-prose flex gap-4 rounded-full bg-zinc-100 p-1 text-xs font-semibold dark:bg-zinc-800">
          {tabs.map((tab) => (
            <Tab
              key={tab.key}
              className={({ selected }) =>
                clsx(
                  'rounded-full px-3 py-1.5 transition focus:outline-none',
                  selected
                    ? 'bg-white text-zinc-900 shadow-sm dark:bg-zinc-700 dark:text-white'
                    : 'text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-200'
                )
              }
            >
              {tab.name}
            </Tab>
          ))}
        </TabList>

        <TabPanels className="mt-6">
          <TabPanel className="space-y-6">
            <div className="prose prose-zinc dark:prose-invert max-w-none">
              <h3>Performance at a Glance</h3>
            </div>

            <div className="space-y-4 rounded-lg border border-zinc-200 bg-zinc-50 p-6 dark:border-zinc-700 dark:bg-zinc-800/50">
              <h4 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Key Performance Differences</h4>
              <ComparisonBar label="Sequential Writes" zerofs="984 ops/s" juicefs="5.6 ops/s" ratio={175} type="performance" />
              <ComparisonBar label="Data Modifications" zerofs="1099 ops/s" juicefs="6 ops/s" ratio={183} type="performance" />
              <ComparisonBar label="File Append" zerofs="1204 ops/s" juicefs="5.3 ops/s" ratio={227} type="performance" />
              <ComparisonBar label="Storage Used" zerofs="7.6 GB" juicefs="239 GB" ratio={31.6} type="resource" />
              <ComparisonBar label="API Operations" zerofs="8k" juicefs="898k" ratio={112} type="resource" />
            </div>
          </TabPanel>

          <TabPanel>
            <div className="space-y-6">
              <div className="prose prose-zinc dark:prose-invert max-w-none">
                <h3>Operations Per Second</h3>
                <p>Higher is better. Tests measure sustainable operation rates.</p>
              </div>

              <SimpleTable
                data={benchmarkData.throughput}
                columns={[
                  { key: 'test', label: 'Test' },
                  { key: 'zerofs', label: 'ZeroFS (ops/sec)' },
                  { key: 'juicefs', label: 'JuiceFS (ops/sec)' },
                  {
                    key: 'ratio',
                    label: 'Difference',
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
                <p>Lower is better. Time per individual operation in milliseconds.</p>
              </div>

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
        </TabPanels>
      </TabGroup>
    </div>
  )
}
