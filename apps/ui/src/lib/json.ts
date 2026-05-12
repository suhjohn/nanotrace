export type JsonValue = null | string | number | boolean | JsonObject | JsonValue[]

export type JsonObject = {
  [key: string]: JsonValue | undefined
}
