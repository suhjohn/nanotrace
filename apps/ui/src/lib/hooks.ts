import { useEffect, useRef, useState, type Dispatch, type SetStateAction } from 'react'

type CookieStateOptions<T> = {
  cookieName: string
  initialValue: T | (() => T)
  maxAge?: number
  parse?: (value: string) => T
  path?: string
  sameSite?: 'Lax' | 'None' | 'Strict'
  serialize?: (value: T) => string
}

type IndexedDbStateOptions<T> = {
  databaseName?: string
  initialValue: T | (() => T)
  key: string
  parse?: (value: string) => T
  serialize?: (value: T) => string
  storeName?: string
}

const DEFAULT_MAX_AGE = 60 * 60 * 24 * 365
const DEFAULT_DATABASE_NAME = 'nanotrace-ui-state'
const DEFAULT_STORE_NAME = 'state'

export function clamp(value: number, min = 0, max = 1) {
  return Math.min(max, Math.max(min, value))
}

function defaultParse<T>(value: string) {
  return JSON.parse(value) as T
}

function defaultSerialize<T>(value: T) {
  return JSON.stringify(value)
}

function resolveInitialValue<T>(initialValue: T | (() => T)) {
  return typeof initialValue === 'function' ? (initialValue as () => T)() : initialValue
}

function readCookie(cookieName: string) {
  const prefix = `${encodeURIComponent(cookieName)}=`
  for (const part of document.cookie.split('; ')) {
    if (part.startsWith(prefix)) {
      return decodeURIComponent(part.slice(prefix.length))
    }
  }
  return null
}

function writeCookie<T>({
  cookieName,
  maxAge,
  path,
  sameSite,
  serialize,
  value
}: {
  cookieName: string
  maxAge: number
  path: string
  sameSite: NonNullable<CookieStateOptions<T>['sameSite']>
  serialize: (value: T) => string
  value: T
}) {
  document.cookie = [
    `${encodeURIComponent(cookieName)}=${encodeURIComponent(serialize(value))}`,
    `Max-Age=${maxAge}`,
    `Path=${path}`,
    `SameSite=${sameSite}`
  ].join('; ')
}

function deleteCookie({
  cookieName,
  path,
  sameSite
}: {
  cookieName: string
  path: string
  sameSite: NonNullable<CookieStateOptions<unknown>['sameSite']>
}) {
  document.cookie = [
    `${encodeURIComponent(cookieName)}=`,
    'Max-Age=0',
    `Path=${path}`,
    `SameSite=${sameSite}`
  ].join('; ')
}

export function useCookieState<T>({
  cookieName,
  initialValue,
  maxAge = DEFAULT_MAX_AGE,
  parse = defaultParse,
  path = '/',
  sameSite = 'Lax',
  serialize = defaultSerialize
}: CookieStateOptions<T>): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState<T>(() => resolveInitialValue(initialValue))
  const stateRef = useRef(state)

  useEffect(() => {
    const storedValue = readCookie(cookieName)
    if (storedValue === null) return

    try {
      const nextState = parse(storedValue)
      stateRef.current = nextState
      setState(nextState)
    } catch {
      deleteCookie({ cookieName, path, sameSite })
    }
  }, [cookieName, parse, path, sameSite])

  stateRef.current = state

  function setCookieState(value: SetStateAction<T>) {
    const nextState =
      typeof value === 'function' ? (value as (current: T) => T)(stateRef.current) : value
    stateRef.current = nextState
    setState(nextState)
    writeCookie({ cookieName, maxAge, path, sameSite, serialize, value: nextState })
  }

  return [state, setCookieState]
}

function openStateDatabase({ databaseName, storeName }: { databaseName: string; storeName: string }) {
  return new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(databaseName, 1)

    request.onupgradeneeded = () => {
      const database = request.result
      if (!database.objectStoreNames.contains(storeName)) {
        database.createObjectStore(storeName)
      }
    }
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result)
  })
}

async function readState({ databaseName, key, storeName }: { databaseName: string; key: string; storeName: string }) {
  const database = await openStateDatabase({ databaseName, storeName })
  return new Promise<string | null>((resolve, reject) => {
    const request = database.transaction(storeName, 'readonly').objectStore(storeName).get(key)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(typeof request.result === 'string' ? request.result : null)
  }).finally(() => database.close())
}

async function writeState({
  databaseName,
  key,
  storeName,
  value
}: {
  databaseName: string
  key: string
  storeName: string
  value: string
}) {
  const database = await openStateDatabase({ databaseName, storeName })
  return new Promise<void>((resolve, reject) => {
    const request = database.transaction(storeName, 'readwrite').objectStore(storeName).put(value, key)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve()
  }).finally(() => database.close())
}

export function useIndexedDbState<T>({
  databaseName = DEFAULT_DATABASE_NAME,
  initialValue,
  key,
  parse = defaultParse,
  serialize = defaultSerialize,
  storeName = DEFAULT_STORE_NAME
}: IndexedDbStateOptions<T>): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState<T>(() => resolveInitialValue(initialValue))
  const stateRef = useRef(state)

  useEffect(() => {
    if (typeof indexedDB === 'undefined') return

    let cancelled = false
    stateRef.current = resolveInitialValue(initialValue)
    setState(stateRef.current)

    void readState({ databaseName, key, storeName })
      .then(storedValue => {
        if (cancelled || storedValue === null) return
        const nextState = parse(storedValue)
        stateRef.current = nextState
        setState(nextState)
      })
      .catch(() => {})

    return () => {
      cancelled = true
    }
  }, [databaseName, initialValue, key, parse, storeName])

  stateRef.current = state

  function setIndexedDbState(value: SetStateAction<T>) {
    const nextState =
      typeof value === 'function' ? (value as (current: T) => T)(stateRef.current) : value
    stateRef.current = nextState
    setState(nextState)

    if (typeof indexedDB === 'undefined') return

    void writeState({
      databaseName,
      key,
      storeName,
      value: serialize(nextState)
    }).catch(() => {})
  }

  return [state, setIndexedDbState]
}
