import { ChevronLeft, ChevronRight } from 'lucide-react'
import * as React from 'react'
import { DayPicker } from 'react-day-picker'
import 'react-day-picker/style.css'
import { cn } from '../../lib/cn'

function Calendar({
  className,
  classNames,
  showOutsideDays = true,
  ...props
}: React.ComponentProps<typeof DayPicker>) {
  return (
    <DayPicker
      showOutsideDays={showOutsideDays}
      className={cn('w-[276px] p-3 text-[12px] text-neutral-100', className)}
      classNames={{
        root: 'nt-calendar',
        months: 'flex flex-col gap-3',
        month: 'w-full space-y-3',
        month_caption: 'flex h-7 items-center justify-center px-8 text-[12px] font-medium text-white',
        nav: 'flex items-center justify-center gap-2',
        button_previous:
          'inline-flex h-6 w-6 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700',
        button_next:
          'inline-flex h-6 w-6 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700',
        chevron: 'hidden',
        weekdays: 'grid grid-cols-7 gap-px text-[10px] uppercase text-neutral-600',
        weekday: 'flex h-6 items-center justify-center font-normal',
        week: 'grid grid-cols-7 gap-px',
        day: 'relative h-8 w-8 p-0 text-center text-[12px]',
        day_button:
          'h-8 w-8 border border-transparent text-neutral-300 hover:border-neutral-700 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700',
        today: 'text-white',
        selected: 'bg-white text-black',
        range_start: 'bg-white text-black',
        range_end: 'bg-white text-black',
        range_middle: 'bg-white/[0.12] text-white',
        outside: 'text-neutral-700',
        disabled: 'text-neutral-800',
        hidden: 'invisible',
        ...classNames
      }}
      components={{
        Chevron: ({ orientation }) =>
          orientation === 'left' ? (
            <ChevronLeft size={13} strokeWidth={1.8} />
          ) : (
            <ChevronRight size={13} strokeWidth={1.8} />
          )
      }}
      {...props}
    />
  )
}

export { Calendar }
