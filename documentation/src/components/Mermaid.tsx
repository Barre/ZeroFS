'use client'

import { useEffect, useRef, useState } from 'react'
import { useTheme } from 'next-themes'

export function Mermaid({ chart }: { chart: string }) {
  const ref = useRef<HTMLDivElement>(null)
  const [svg, setSvg] = useState<string>('')
  const { resolvedTheme } = useTheme()

  useEffect(() => {
    async function renderChart() {
      try {
        const mermaid = (await import('mermaid')).default

        const isDark = resolvedTheme === 'dark'

        mermaid.initialize({
          startOnLoad: true,
          theme: 'base',
          themeVariables: {
            // Primary colors - neutral grays instead of blue
            primaryColor: isDark ? 'rgba(255, 255, 255, 0.025)' : '#fafafa',
            primaryTextColor: isDark ? '#e5e7eb' : '#111827',
            primaryBorderColor: isDark ? 'rgba(255, 255, 255, 0.1)' : '#e5e7eb',

            // Lines and connections - match border styles
            lineColor: isDark ? 'rgba(255, 255, 255, 0.1)' : '#e5e7eb',

            // Background colors - match code block styling
            background: 'transparent',
            mainBkg: isDark ? '#18181b' : '#ffffff',
            secondBkg: isDark ? 'rgba(255, 255, 255, 0.025)' : '#f9fafb',
            tertiaryBkg: isDark ? 'rgba(255, 255, 255, 0.05)' : '#f3f4f6',

            // Text colors - match prose
            textColor: isDark ? '#d1d5db' : '#4b5563',
            taskTextColor: isDark ? '#e5e7eb' : '#111827',
            taskTextDarkColor: isDark ? '#e5e7eb' : '#111827',

            // Node colors - clean with subtle backgrounds
            nodeTextColor: isDark ? '#e5e7eb' : '#111827',
            defaultLinkColor: isDark ? 'rgba(255, 255, 255, 0.2)' : '#d1d5db',
            titleColor: isDark ? '#e5e7eb' : '#111827',
            edgeLabelBackground: isDark ? '#18181b' : '#ffffff',

            // Borders - match documentation
            nodeBorder: isDark ? 'rgba(255, 255, 255, 0.1)' : '#e5e7eb',
            clusterBorder: isDark ? 'rgba(255, 255, 255, 0.075)' : '#e5e7eb',
            defaultBorderColor: isDark ? 'rgba(255, 255, 255, 0.1)' : '#e5e7eb',

            // Special elements
            altBackground: isDark ? 'rgba(255, 255, 255, 0.025)' : '#f9fafb',
            errorBkgColor: isDark ? 'rgba(239, 68, 68, 0.1)' : '#fee2e2',
            errorTextColor: isDark ? '#f87171' : '#dc2626',

            // Typography - match documentation
            fontFamily: 'Inter, system-ui, -apple-system, sans-serif',
            fontSize: '14px',

            // Graph specific
            labelColor: isDark ? '#9ca3af' : '#6b7280',

            // Flowchart
            flowchartBackground: 'transparent',

            // Sequence diagram - match tag styling
            actorTextColor: isDark ? '#e5e7eb' : '#111827',
            actorLineColor: isDark ? 'rgba(255, 255, 255, 0.075)' : '#e5e7eb',
            signalColor: isDark ? '#9ca3af' : '#6b7280',
            sequenceNumberColor: isDark ? '#e5e7eb' : '#111827',

            // Additional sequence diagram colors
            activationBkgColor: isDark ? 'rgba(59, 130, 246, 0.1)' : '#dbeafe',
            activationBorderColor: isDark ? 'rgba(59, 130, 246, 0.2)' : '#93c5fd',
            sequenceBoxBkgColor: isDark ? 'rgba(255, 255, 255, 0.025)' : '#f9fafb',
            sequenceBoxBorderColor: isDark ? 'rgba(255, 255, 255, 0.1)' : '#e5e7eb',
            labelBoxBkgColor: isDark ? '#18181b' : '#ffffff',
            labelBoxBorderColor: isDark ? 'rgba(255, 255, 255, 0.1)' : '#e5e7eb',
            loopTextColor: isDark ? '#60a5fa' : '#3b82f6',
          },
          flowchart: {
            curve: 'basis',
            nodeSpacing: 50,
            rankSpacing: 50,
            padding: 15,
            useMaxWidth: true,
            subGraphTitleMargin: {
              top: 10,
              bottom: 5
            }
          },
          sequence: {
            actorMargin: 50,
            boxMargin: 10,
            boxTextMargin: 5,
            noteMargin: 10,
            messageMargin: 35,
            mirrorActors: false,
            useMaxWidth: true,
          }
        })

        const id = `mermaid-${Math.random().toString(36).substr(2, 9)}`
        const { svg } = await mermaid.render(id, chart)
        setSvg(svg)
      } catch (error) {
        console.error('Error rendering mermaid chart:', error)
      }
    }

    renderChart()
  }, [chart, resolvedTheme])

  return (
    <div className="my-8 overflow-visible">
      <div
        ref={ref}
        dangerouslySetInnerHTML={{ __html: svg }}
        className="mermaid-chart [&_svg]:mx-auto [&_svg]:max-w-full"
      />
    </div>
  )
}
