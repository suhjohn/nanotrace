import { createFileRoute } from '@tanstack/react-router'
import { LogsRoute, parseObservatorySearch } from './index'

export const Route = createFileRoute('/logs')({
  validateSearch: parseObservatorySearch,
  component: LogsRouteComponent
})

function LogsRouteComponent() {
  return <LogsRoute search={Route.useSearch()} />
}
