import { AsyncLocalStorage } from 'node:async_hooks'
import type { CommonFields } from './types.js'

const storage = new AsyncLocalStorage<CommonFields>()

export function withContext<T>(context: CommonFields, fn: () => T): T {
  return storage.run({ ...storage.getStore(), ...context }, fn)
}

export function currentContext(): CommonFields {
  return { ...storage.getStore() }
}

export function contextStorage() {
  return storage
}
