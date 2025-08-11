import { useTheme } from 'next-themes'
import { useEffect, useState } from 'react'

function SunIcon(props: React.ComponentPropsWithoutRef<'svg'>) {
  return (
    <svg viewBox="0 0 20 20" fill="none" aria-hidden="true" {...props}>
      <path d="M12.5 10a2.5 2.5 0 1 1-5 0 2.5 2.5 0 0 1 5 0Z" />
      <path
        strokeLinecap="round"
        d="M10 5.5v-1M13.182 6.818l.707-.707M14.5 10h1M13.182 13.182l.707.707M10 15.5v-1M6.11 13.889l.708-.707M4.5 10h1M6.11 6.111l.708.707"
      />
    </svg>
  )
}

function MoonIcon(props: React.ComponentPropsWithoutRef<'svg'>) {
  return (
    <svg viewBox="0 0 20 20" fill="none" aria-hidden="true" {...props}>
      <path d="M15.224 11.724a5.5 5.5 0 0 1-6.949-6.949 5.5 5.5 0 1 0 6.949 6.949Z" />
    </svg>
  )
}

export function ThemeToggle() {
  let { resolvedTheme, setTheme } = useTheme()
  let otherTheme = resolvedTheme === 'dark' ? 'light' : 'dark'
  let [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  return (
    <button
      type="button"
      className="group flex h-8 w-8 items-center justify-center rounded-lg shadow-sm shadow-zinc-800/5 ring-1 ring-zinc-900/5 transition hover:shadow-md hover:shadow-zinc-800/5 hover:ring-zinc-900/10 dark:ring-white/10 dark:hover:ring-white/20"
      aria-label={mounted ? `Switch to ${otherTheme} theme` : 'Toggle theme'}
      onClick={() => setTheme(otherTheme)}
    >
      <SunIcon className="h-5 w-5 stroke-zinc-900 transition group-hover:stroke-zinc-700 dark:hidden" />
      <MoonIcon className="hidden h-5 w-5 stroke-zinc-400 transition group-hover:stroke-white dark:block" />
    </button>
  )
}
