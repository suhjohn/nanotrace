import { createNanotrace, udpTransport } from '../src/index.js'

const nt = createNanotrace({
  transport: udpTransport(),
  service: 'checkout-api',
  environment: 'test'
})

await nt.withContext({ tenantId: 'tenant_1', userId: 'user_1' }, async () => {
  await nt.info('checkout started', { loggerName: 'checkout' })
  await nt.span('POST /checkout', async () => {
    await nt.counter('checkout.attempts')
    await nt.httpServerRequest({
      method: 'POST',
      route: '/checkout',
      statusCode: 200,
      durationMs: 42
    })
  })
  await nt.track('Checkout Completed', { revenue: 99, currency: 'USD' })
})
