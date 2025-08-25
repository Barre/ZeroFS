'use client'

import React from 'react'
import { Tab, TabGroup, TabList, TabPanel, TabPanels } from '@headlessui/react'
import clsx from 'clsx'

export function MetricCard({ label, value, unit, color = 'blue' }: {
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

export function ComparisonBar({ 
  label, 
  zerofsValue, 
  comparisonValue, 
  zerofsLabel = 'ZeroFS',
  comparisonLabel,
  ratio, 
  type = 'performance',
  comparisonColor = 'amber'
}: {
  label: string
  zerofsValue: string
  comparisonValue: string
  zerofsLabel?: string
  comparisonLabel: string
  ratio: number | string
  type?: 'performance' | 'cost' | 'resource'
  comparisonColor?: 'amber' | 'red'
}) {
  // Parse numeric values from strings
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

  const zerofsVal = parseValue(zerofsValue)
  const comparisonVal = parseValue(comparisonValue)

  // For performance metrics (ops/s), higher is better
  // For cost/resource metrics, lower is better
  const isHigherBetter = type === 'performance'
  const maxVal = Math.max(zerofsVal, comparisonVal)

  // Calculate bar widths proportionally
  const maxBarWidth = typeof window !== 'undefined'
    ? window.innerWidth < 400 ? 80 : window.innerWidth < 500 ? 120 : window.innerWidth < 640 ? 150 : 200
    : 200
  const zerofsWidth = maxVal > 0 ? Math.max((zerofsVal / maxVal) * maxBarWidth, 5) : 5
  const comparisonWidth = maxVal > 0 ? Math.max((comparisonVal / maxVal) * maxBarWidth, 5) : 5

  let ratioText = ''
  if (typeof ratio === 'number') {
    if (ratio > 1) {
      const descriptor = type === 'performance' ? 'faster' : type === 'cost' ? 'cheaper' : type === 'resource' ? 'less' : 'better'
      ratioText = `ZeroFS: ${ratio.toFixed(ratio >= 10 ? 0 : 1)}x ${descriptor}`
    } else if (ratio < 1) {
      const descriptor = type === 'performance' ? 'faster' : type === 'cost' ? 'cheaper' : type === 'resource' ? 'less' : 'better'
      ratioText = `${comparisonLabel}: ${(1 / ratio).toFixed((1 / ratio) >= 10 ? 0 : 1)}x ${descriptor}`
    } else {
      ratioText = 'Equal'
    }
  }

  // For alternative ratio text when comparing against a competitor
  if (typeof ratio === 'number' && ratio > 1.5 && type === 'performance') {
    const isSlower = zerofsVal < comparisonVal
    if (!isSlower) {
      ratioText = `${comparisonLabel}: ${ratio}x ${isHigherBetter ? 'slower' : 'more'}`
    }
  }

  const comparisonColorClasses = {
    amber: 'bg-amber-500 dark:bg-amber-600',
    red: 'bg-red-500 dark:bg-red-600'
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-col sm:flex-row sm:justify-between gap-1 text-sm">
        <span className="font-medium text-zinc-700 dark:text-zinc-300">{label}</span>
        {ratioText && (
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            {ratioText}
          </span>
        )}
      </div>
      <div className="space-y-3 text-xs">
        <div className="space-y-1">
          <div className="flex flex-col min-[400px]:flex-row min-[400px]:items-center gap-1 min-[400px]:gap-2">
            <span className="w-14 shrink-0 min-[400px]:text-right text-zinc-600 dark:text-zinc-400" style={{ width: comparisonLabel === 'Mountpoint' ? '5rem' : '3.5rem' }}>{zerofsLabel}</span>
            <div className="flex items-center gap-2 min-w-0">
              <div
                className="h-6 bg-blue-500 dark:bg-blue-600 rounded shrink-0"
                style={{ width: `${zerofsWidth}px` }}
              />
              <span className="font-mono text-zinc-700 dark:text-zinc-300 whitespace-nowrap min-[400px]:truncate">{zerofsValue}</span>
            </div>
          </div>
        </div>
        <div className="space-y-1">
          <div className="flex flex-col min-[400px]:flex-row min-[400px]:items-center gap-1 min-[400px]:gap-2">
            <span className="w-14 shrink-0 min-[400px]:text-right text-zinc-600 dark:text-zinc-400" style={{ width: comparisonLabel === 'Mountpoint' ? '5rem' : '3.5rem' }}>{comparisonLabel}</span>
            <div className="flex items-center gap-2 min-w-0">
              <div
                className={`h-6 rounded shrink-0 ${comparisonColorClasses[comparisonColor]}`}
                style={{ width: `${comparisonWidth}px` }}
              />
              <span className="font-mono text-zinc-700 dark:text-zinc-300 whitespace-nowrap min-[400px]:truncate">{comparisonValue}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export interface TableColumn {
  key: string
  label: string
  className?: (value: any) => string
}

export function SimpleTable({ data, columns }: {
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

export function BenchmarkTabs({ children, tabs }: {
  children: React.ReactNode
  tabs: Array<{ name: string; key: string }>
}) {
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
          {children}
        </TabPanels>
      </TabGroup>
    </div>
  )
}

export { TabPanel }

export function PerformanceIndicator({ type = 'higher' }: { type?: 'higher' | 'lower' }) {
  if (type === 'higher') {
    return (
      <div className="inline-flex items-center gap-2 rounded-lg bg-green-50 dark:bg-green-950/30 px-3 py-1.5 text-xs font-medium text-green-700 dark:text-green-400">
        <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
        </svg>
        Higher is better
      </div>
    )
  }
  
  return (
    <div className="inline-flex items-center gap-2 rounded-lg bg-blue-50 dark:bg-blue-950/30 px-3 py-1.5 text-xs font-medium text-blue-700 dark:text-blue-400">
      <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
      </svg>
      Lower is better
    </div>
  )
}