import glob from 'fast-glob'
import { type Metadata } from 'next'
import { Schibsted_Grotesk, Geist_Mono } from 'next/font/google'
import { Providers } from '@/app/providers'
import { Layout } from '@/components/Layout'
import { type Section } from '@/components/SectionProvider'
import PlausibleProvider from 'next-plausible'
import '@/styles/tailwind.css'
import '@/styles/mermaid.css'

const schibstedGrotesk = Schibsted_Grotesk({
  subsets: ['latin'],
  variable: '--font-schibsted',
  display: 'swap',
})

const geistMono = Geist_Mono({
  subsets: ['latin'],
  weight: ['400', '500', '600'],
  variable: '--font-geist-mono',
  display: 'swap',
})

export const metadata: Metadata = {
  title: {
    template: '%s - ZeroFS Documentation',
    default: 'ZeroFS - The Filesystem That Makes S3 Your Primary Storage',
  },
  description: 'ZeroFS documentation - Learn how to mount S3 as a high-performance filesystem with NFS and NBD support, encryption, and caching.',
  icons: {
    icon: [
      {
        url: '/docs/images/favicons/browser.png',
        type: 'image/png',
        sizes: '32x32',
      },
      {
        url: '/docs/images/favicons/Android.png',
        type: 'image/png',
        sizes: '196x196',
      },
    ],
    apple: [
      {
        url: '/docs/images/favicons/iPhone.png',
        type: 'image/png',
        sizes: '180x180',
      },
    ],
  },
}

export default async function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  let pages = await glob('**/*.mdx', { cwd: 'src/app' })
  let allSectionsEntries = (await Promise.all(
    pages.map(async (filename) => [
      '/' + filename.replace(/(^|\/)page\.mdx$/, ''),
      (await import(`./${filename}`)).sections,
    ]),
  )) as Array<[string, Array<Section>]>
  let allSections = Object.fromEntries(allSectionsEntries)

  return (
    <html
      lang="en"
      className={`h-full ${schibstedGrotesk.variable} ${geistMono.variable}`}
      suppressHydrationWarning
    >
      <head>
        <PlausibleProvider
          domain="zerofs.net"
          customDomain="https://p.merklemap.com"
        />
      </head>

      <body className="flex min-h-full bg-zinc-50 antialiased dark:bg-zinc-950">
        <Providers>
          <div className="w-full">
            <Layout allSections={allSections}>{children}</Layout>
          </div>
        </Providers>
      </body>
    </html>
  )
}
