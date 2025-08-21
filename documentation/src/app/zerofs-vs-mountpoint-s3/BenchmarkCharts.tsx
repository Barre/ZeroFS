'use client'

import React from 'react'
import { Tab, TabGroup, TabList, TabPanel, TabPanels } from '@headlessui/react'
import clsx from 'clsx'

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

function ComparisonBar({ label, zerofs, mountpoint, ratio, type = 'performance' }: {
  label: string
  zerofs: string
  mountpoint: string
  ratio: number | string
  type?: 'performance' | 'resource'
}) {
  const parseValue = (val: string) => {
    if (val === 'Failed' || val === 'N/A') return 0
    if (val.includes('h')) {
      // Extract hours (e.g., "~2h" -> 2 * 3600)
      const hours = parseFloat(val.replace(/[^0-9.]/g, ''))
      return hours * 3600
    }
    if (val.includes('m') && val.includes('s')) {
      // Extract minutes and seconds (e.g., "12m 27s")
      const parts = val.split(/[ms]/).filter(p => p.trim())
      const minutes = parseFloat(parts[0]) || 0
      const seconds = parseFloat(parts[1]) || 0
      return minutes * 60 + seconds
    }
    if (val.includes('s')) {
      // Extract seconds (e.g., "13.5s" -> 13.5)
      return parseFloat(val.replace(/[^0-9.]/g, ''))
    }
    // Default parsing for other values
    const num = parseFloat(val.replace(/[^0-9.]/g, ''))
    return isNaN(num) ? 0 : num
  }

  const zerofsVal = parseValue(zerofs)
  const mountpointVal = parseValue(mountpoint)

  // For performance metrics (ops/s), higher is better
  // For resource metrics (operations), lower is better
  const isHigherBetter = type === 'performance'
  const maxVal = Math.max(zerofsVal, mountpointVal)

  // Calculate bar widths proportionally (responsive max width)
  const maxBarWidth = typeof window !== 'undefined'
    ? window.innerWidth < 400 ? 80 : window.innerWidth < 500 ? 120 : window.innerWidth < 640 ? 150 : 200
    : 200
  const zerofsWidth = maxVal > 0 ? Math.max((zerofsVal / maxVal) * maxBarWidth, 5) : 5
  const mountpointWidth = maxVal > 0 ? Math.max((mountpointVal / maxVal) * maxBarWidth, 5) : 5

  return (
    <div className="space-y-2">
      <div className="flex flex-col sm:flex-row sm:justify-between gap-1 text-sm">
        <span className="font-medium text-zinc-700 dark:text-zinc-300">{label}</span>
        {typeof ratio === 'number' && ratio > 1.5 && (
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            Mountpoint: {ratio}x {isHigherBetter ? 'slower' : 'more'}
          </span>
        )}
      </div>
      <div className="space-y-3 text-xs">
        <div className="space-y-1">
          <div className="flex flex-col min-[400px]:flex-row min-[400px]:items-center gap-1 min-[400px]:gap-2">
            <span className="w-20 shrink-0 min-[400px]:text-right text-zinc-600 dark:text-zinc-400">ZeroFS</span>
            <div className="flex items-center gap-2 min-w-0">
              <div
                className="h-6 bg-blue-500 dark:bg-blue-600 rounded shrink-0"
                style={{ width: `${zerofsWidth}px` }}
              />
              <span className="font-mono text-zinc-700 dark:text-zinc-300 whitespace-nowrap min-[400px]:truncate">{zerofs}</span>
            </div>
          </div>
        </div>
        <div className="space-y-1">
          <div className="flex flex-col min-[400px]:flex-row min-[400px]:items-center gap-1 min-[400px]:gap-2">
            <span className="w-20 shrink-0 min-[400px]:text-right text-zinc-600 dark:text-zinc-400">Mountpoint</span>
            <div className="flex items-center gap-2 min-w-0">
              <div
                className="h-6 bg-amber-500 dark:bg-amber-600 rounded shrink-0"
                style={{ width: `${mountpointWidth}px` }}
              />
              <span className="font-mono text-zinc-700 dark:text-zinc-300 whitespace-nowrap min-[400px]:truncate">{mountpoint}</span>
            </div>
          </div>
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
        <div className="not-prose border-b border-zinc-200 dark:border-zinc-700">
          <TabList className="-mb-px flex flex-wrap gap-x-4 text-xs font-medium">
            {tabs.map((tab) => (
              <Tab
                key={tab.key}
                className={({ selected }) =>
                  clsx(
                    'border-b-2 py-2.5 transition focus:outline-none',
                    selected
                      ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                      : 'border-transparent text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-300'
                  )
                }
              >
                {tab.name}
              </Tab>
            ))}
          </TabList>
        </div>

        <TabPanels className="mt-6">
          <TabPanel className="space-y-6">
            <div className="prose prose-zinc dark:prose-invert max-w-none">
              <h3>Performance at a Glance</h3>
            </div>

            <div className="space-y-4 rounded-lg border border-zinc-200 bg-zinc-50 p-6 dark:border-zinc-700 dark:bg-zinc-800/50">
              <h4 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Key Performance Differences</h4>
              <ComparisonBar label="Sequential Writes (higher is better)" zerofs="664 ops/s" mountpoint="0.7 ops/s" ratio={948} type="performance" />
              <ComparisonBar label="Empty Files (higher is better)" zerofs="889 ops/s" mountpoint="0.09 ops/s" ratio={9874} type="performance" />
              <ComparisonBar label="Random Reads (higher is better)" zerofs="1001 ops/s" mountpoint="3.2 ops/s" ratio={313} type="performance" />
              <ComparisonBar label="TAR Extract Time (lower is better)" zerofs="13.5s" mountpoint="~2h" ratio={533} type="resource" />
              <ComparisonBar label="S3 API Operations (lower is better)" zerofs="0.6k" mountpoint="14.6k" ratio={23} type="resource" />
            </div>
          </TabPanel>

          <TabPanel>
            <div className="space-y-6">
              <div className="prose prose-zinc dark:prose-invert max-w-none">
                <h3>Operations Per Second</h3>
                <p>Tests measure sustainable operation rates. Note: Some operations are not supported by Mountpoint-s3.</p>
              </div>

              <div className="inline-flex items-center gap-2 rounded-lg bg-green-50 dark:bg-green-950/30 px-3 py-1.5 text-xs font-medium text-green-700 dark:text-green-400">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                </svg>
                Higher is better
              </div>

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

              <div className="inline-flex items-center gap-2 rounded-lg bg-blue-50 dark:bg-blue-950/30 px-3 py-1.5 text-xs font-medium text-blue-700 dark:text-blue-400">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
                </svg>
                Lower is better
              </div>

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

              <div className="inline-flex items-center gap-2 rounded-lg bg-green-50 dark:bg-green-950/30 px-3 py-1.5 text-xs font-medium text-green-700 dark:text-green-400">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                </svg>
                Higher is better
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

              <div className="inline-flex items-center gap-2 rounded-lg bg-blue-50 dark:bg-blue-950/30 px-3 py-1.5 text-xs font-medium text-blue-700 dark:text-blue-400">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
                </svg>
                Lower time is better
              </div>

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

              <div className="inline-flex items-center gap-2 rounded-lg bg-blue-50 dark:bg-blue-950/30 px-3 py-1.5 text-xs font-medium text-blue-700 dark:text-blue-400">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
                </svg>
                Lower is better
              </div>

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
        </TabPanels>
      </TabGroup>
    </div>
  )
}
