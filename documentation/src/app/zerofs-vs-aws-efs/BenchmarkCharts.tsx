'use client'

import React from 'react'
import { Tab, TabGroup, TabList, TabPanel, TabPanels } from '@headlessui/react'
import clsx from 'clsx'

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

function ComparisonBar({ label, zerofs, efs, ratio, type = 'performance' }: {
  label: string
  zerofs: string
  efs: string
  ratio: number
  type?: 'performance' | 'cost'
}) {
  // Parse numeric values from strings
  const parseValue = (val: string) => {
    const num = parseFloat(val.replace(/[^0-9.]/g, ''))
    return isNaN(num) ? 0 : num
  }

  const zerofsVal = parseValue(zerofs)
  const efsVal = parseValue(efs)

  // For performance metrics (ops/s), higher is better
  // For cost metrics, lower is better
  const isHigherBetter = type === 'performance'
  const maxVal = Math.max(zerofsVal, efsVal)

  // Calculate bar widths proportionally
  const maxBarWidth = typeof window !== 'undefined'
    ? window.innerWidth < 400 ? 80 : window.innerWidth < 500 ? 120 : window.innerWidth < 640 ? 150 : 200
    : 200
  const zerofsWidth = Math.max((zerofsVal / maxVal) * maxBarWidth, 5)
  const efsWidth = Math.max((efsVal / maxVal) * maxBarWidth, 5)

  const ratioText = ratio > 1
    ? `ZeroFS: ${ratio.toFixed(1)}x ${type === 'performance' ? 'faster' : type === 'cost' ? 'cheaper' : 'faster'}`
    : ratio < 1
      ? `EFS: ${(1 / ratio).toFixed(1)}x ${type === 'performance' ? 'faster' : type === 'cost' ? 'cheaper' : 'faster'}`
      : 'Equal'

  return (
    <div className="space-y-2">
      <div className="flex flex-col sm:flex-row sm:justify-between gap-1 text-sm">
        <span className="font-medium text-zinc-700 dark:text-zinc-300">{label}</span>
        {ratio !== 1 && (
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            {ratioText}
          </span>
        )}
      </div>
      <div className="space-y-3 text-xs">
        <div className="space-y-1">
          <div className="flex flex-col min-[400px]:flex-row min-[400px]:items-center gap-1 min-[400px]:gap-2">
            <span className="w-14 shrink-0 min-[400px]:text-right text-zinc-600 dark:text-zinc-400">ZeroFS</span>
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
            <span className="w-14 shrink-0 min-[400px]:text-right text-zinc-600 dark:text-zinc-400">EFS</span>
            <div className="flex items-center gap-2 min-w-0">
              <div
                className="h-6 bg-amber-500 dark:bg-amber-600 rounded shrink-0"
                style={{ width: `${efsWidth}px` }}
              />
              <span className="font-mono text-zinc-700 dark:text-zinc-300 whitespace-nowrap min-[400px]:truncate">{efs}</span>
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
    { name: 'Real-World', key: 'realworld' },
    { name: 'Cost', key: 'cost' },
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
              <ComparisonBar label="Sequential Writes (higher is better)" zerofs="231 ops/s" efs="108 ops/s" ratio={2.1} type="performance" />
              <ComparisonBar label="Data Modifications (higher is better)" zerofs="309 ops/s" efs="182 ops/s" ratio={1.7} type="performance" />
              <ComparisonBar label="File Append (higher is better)" zerofs="301 ops/s" efs="91 ops/s" ratio={3.3} type="performance" />
              <ComparisonBar label="Empty Files (higher is better)" zerofs="295 ops/s" efs="196 ops/s" ratio={1.5} type="performance" />
              <ComparisonBar label="Empty Directories (higher is better)" zerofs="447 ops/s" efs="219 ops/s" ratio={2.0} type="performance" />
              <ComparisonBar label="Random Reads (higher is better)" zerofs="416 ops/s" efs="1243 ops/s" ratio={0.33} type="performance" />
              <ComparisonBar label="Git Clone (lower is better)" zerofs="3.9s" efs="5.0s" ratio={1.3} type="performance" />
              <ComparisonBar label="Cargo Build (lower is better)" zerofs="5m 45s" efs="4m 56s" ratio={0.86} type="performance" />
              <ComparisonBar label="TAR Extract (lower is better)" zerofs="40s" efs="88s" ratio={2.2} type="performance" />
              <ComparisonBar label="Storage Cost per GB (lower is better)" zerofs="2.3¢" efs="30¢" ratio={13} type="cost" />
            </div>
          </TabPanel>

          <TabPanel>
            <div className="space-y-6">
              <div className="prose prose-zinc dark:prose-invert max-w-none">
                <h3>Operations Per Second</h3>
                <p>Higher values indicate better performance.</p>
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

              <div className="inline-flex items-center gap-2 rounded-lg bg-blue-50 dark:bg-blue-950/30 px-3 py-1.5 text-xs font-medium text-blue-700 dark:text-blue-400">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
                </svg>
                Lower is better
              </div>

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
        </TabPanels>
      </TabGroup>
    </div>
  )
}
