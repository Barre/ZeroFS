import { Button } from '@/components/Button'
import { Heading } from '@/components/Heading'

const guides = [
  {
    href: '/encryption',
    name: 'Encryption & Security',
    description: 'Learn how ZeroFS encrypts your data with XChaCha20-Poly1305.',
  },
  {
    href: '/configuration',
    name: 'Configuration',
    description: 'Configure S3 backends, caching, and network settings.',
  },
  {
    href: '/troubleshooting',
    name: 'Troubleshooting',
    description:
      'Debug common issues and understand error messages.',
  },
  {
    href: '/advanced-use-cases',
    name: 'Advanced Use Cases',
    description:
      'Build geo-distributed storage and tiered architectures.',
  },
]

export function Guides() {
  return (
    <div className="my-16 xl:max-w-none">
      <Heading level={2} id="guides">
        Guides
      </Heading>
      <div className="not-prose mt-4 grid grid-cols-1 gap-8 border-t border-zinc-900/5 pt-10 sm:grid-cols-2 xl:grid-cols-4 dark:border-white/5">
        {guides.map((guide) => (
          <div key={guide.href}>
            <h3 className="text-sm font-semibold text-zinc-900 dark:text-white">
              {guide.name}
            </h3>
            <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
              {guide.description}
            </p>
            <p className="mt-4">
              <Button href={guide.href} variant="text" arrow="right">
                Read more
              </Button>
            </p>
          </div>
        ))}
      </div>
    </div>
  )
}
