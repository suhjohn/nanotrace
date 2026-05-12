declare global {
  interface ImportMetaEnv {
    readonly VITE_NANOTRACE_KEY?: string
    readonly VITE_NANOTRACE_TABLE?: string
    readonly VITE_NANOTRACE_URL?: string
  }
}

export {}
