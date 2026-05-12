import { createNanotrace, udpTransport } from '../src/index.js'

const nt = createNanotrace({
  transport: udpTransport(),
  service: 'checkout-api',
  environment: 'test'
})

nt.withContext({ tenantId: 'tenant_1', userId: 'user_1' }, () => {
  nt.info('checkout started', { loggerName: 'checkout' })
  const span = nt.span('POST /checkout')
  try {
    nt.counter('checkout.attempts')
    nt.httpServerRequest({
      method: 'POST',
      route: '/checkout',
      statusCode: 200,
      durationMs: 42
    })
    span.end({ spanStatusCode: 'ok' })
  } catch (error) {
    span.end({ spanStatusCode: 'error', isError: 1 })
    throw error
  }
  nt.track('Checkout Completed', { revenue: 99, currency: 'USD' })
})

await nt.flush()
