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
            // Primary colors - neutral warm grays instead of blue
            primaryColor: isDark ? 'rgba(255, 255, 255, 0.035)' : '#faf9f6',
            primaryTextColor: isDark ? '#f4f3ef' : '#1c1b17',
            primaryBorderColor: isDark ? 'rgba(255, 255, 255, 0.09)' : '#e7e5df',

            // Lines and connections - match border styles
            lineColor: isDark ? 'rgba(255, 255, 255, 0.09)' : '#e7e5df',

            // Background colors - match code block styling
            background: 'transparent',
            mainBkg: isDark ? '#111110' : '#ffffff',
            secondBkg: isDark ? 'rgba(255, 255, 255, 0.035)' : '#faf9f6',
            tertiaryBkg: isDark ? 'rgba(255, 255, 255, 0.06)' : '#f4f3ef',

            // Text colors - match prose
            textColor: isDark ? '#d3d0c7' : '#56544d',
            taskTextColor: isDark ? '#f4f3ef' : '#1c1b17',
            taskTextDarkColor: isDark ? '#f4f3ef' : '#1c1b17',

            // Node colors - clean with subtle backgrounds
            nodeTextColor: isDark ? '#f4f3ef' : '#1c1b17',
            defaultLinkColor: isDark ? 'rgba(255, 255, 255, 0.22)' : '#d3d0c7',
            titleColor: isDark ? '#f4f3ef' : '#1c1b17',
            edgeLabelBackground: isDark ? '#111110' : '#ffffff',

            // Borders - match documentation
            nodeBorder: isDark ? 'rgba(255, 255, 255, 0.09)' : '#e7e5df',
            clusterBorder: isDark ? 'rgba(255, 255, 255, 0.075)' : '#e7e5df',
            defaultBorderColor: isDark ? 'rgba(255, 255, 255, 0.09)' : '#e7e5df',

            // Special elements
            altBackground: isDark ? 'rgba(255, 255, 255, 0.035)' : '#faf9f6',
            errorBkgColor: isDark ? 'rgba(239, 68, 68, 0.1)' : '#fee2e2',
            errorTextColor: isDark ? '#f87171' : '#dc2626',

            // Typography - match documentation
            fontFamily: 'var(--font-sans), "Schibsted Grotesk", system-ui, sans-serif',
            fontSize: '14px',

            // Graph specific
            labelColor: isDark ? '#a8a69e' : '#6e6c64',

            // Flowchart
            flowchartBackground: 'transparent',

            // Sequence diagram - match tag styling
            actorTextColor: isDark ? '#f4f3ef' : '#1c1b17',
            actorLineColor: isDark ? 'rgba(255, 255, 255, 0.075)' : '#e7e5df',
            signalColor: isDark ? '#a8a69e' : '#6e6c64',
            sequenceNumberColor: isDark ? '#f4f3ef' : '#1c1b17',

            // Additional sequence diagram colors
            activationBkgColor: isDark ? 'rgba(77, 159, 255, 0.1)' : '#e2edfb',
            activationBorderColor: isDark ? 'rgba(77, 159, 255, 0.2)' : '#a9cdfb',
            sequenceBoxBkgColor: isDark ? 'rgba(255, 255, 255, 0.035)' : '#faf9f6',
            sequenceBoxBorderColor: isDark ? 'rgba(255, 255, 255, 0.09)' : '#e7e5df',
            labelBoxBkgColor: isDark ? '#111110' : '#ffffff',
            labelBoxBorderColor: isDark ? 'rgba(255, 255, 255, 0.09)' : '#e7e5df',
            loopTextColor: isDark ? '#4d9fff' : '#2e8bff',
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
