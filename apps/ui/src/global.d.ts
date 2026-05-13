declare global {
  interface ImportMetaEnv {
    readonly VITE_NANOTRACE_API_KEY?: string
    readonly VITE_NANOTRACE_EVENT_INDEX_TABLE?: string
    readonly VITE_NANOTRACE_FACETS_TABLE?: string
    readonly VITE_NANOTRACE_TABLE?: string
    readonly VITE_NANOTRACE_URL?: string
  }
}

export {}
