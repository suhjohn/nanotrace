import { createContext, useContext } from 'react'
import type { Dispatch, ReactNode, SetStateAction } from 'react'

type AppShellState = {
  sidebarOpen: boolean
  setSidebarOpen: Dispatch<SetStateAction<boolean>>
}

const AppShellContext = createContext<AppShellState | null>(null)

export function AppShellProvider({
  children,
  value
}: {
  children: ReactNode
  value: AppShellState
}) {
  return <AppShellContext.Provider value={value}>{children}</AppShellContext.Provider>
}

export function useAppShell() {
  const state = useContext(AppShellContext)
  if (!state) throw new Error('useAppShell must be used within AppShell')
  return state
}
