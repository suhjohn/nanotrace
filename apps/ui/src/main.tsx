import { createRoot } from 'react-dom/client'

function App() {
  return (
    <main>
      <h1>Nanotrace</h1>
      <p>Ingest UI shell. Trace explorer migrates here next.</p>
    </main>
  )
}

createRoot(document.getElementById('root')!).render(<App />)
