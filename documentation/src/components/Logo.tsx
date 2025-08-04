import Image from 'next/image'
import colorLogo from '@/images/logos/color_logo_no_background.svg'
import whiteLogo from '@/images/logos/white_logo_no_background.svg'

export function Logo(props: React.ComponentPropsWithoutRef<typeof Image>) {
  return (
    <>
      <Image
        {...props}
        src={colorLogo}
        alt="Logo"
        className="dark:hidden"
        width={150}
        height={36}
      />
      <Image
        {...props}
        src={whiteLogo}
        alt="Logo"
        className="hidden dark:block"
        width={150}
        height={36}
      />
    </>
  )
}
