import { createFileRoute } from '@tanstack/react-router'
import { ObservatoryHome, parseObservatorySearch } from '../index'

export const Route = createFileRoute('/$field/$value')({
  validateSearch: parseObservatorySearch,
  component: GroupRoute
})

function GroupRoute() {
  const params = Route.useParams()
  const search = Route.useSearch()
  return (
    <ObservatoryHome
      eventFilterSearchText={search.filter}
      routeSelection={{ field: params.field, value: params.value }}
      searchCustomRangeEnd={search.rangeEnd}
      searchCustomRangeStart={search.rangeStart}
      searchTimeRange={search.timeRange}
      selectedEventId={search.eventId ?? ''}
    />
  )
}
