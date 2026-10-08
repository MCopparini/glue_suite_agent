# glue_suite_agent

`gs-agent`: l'agente GlueSuite installato presso il cliente. Si installa **una volta per macchina**,
si attiva con un token generato in GlueSuite e da quel momento fa girare i connettori assegnati da GlueSuite
(3CAD, SAP...), parlando con l'integration hub attraverso RabbitMQ.

I contratti con l'hub (busta, comandi, heartbeat, enrollment) sono in
[`@mcopparini/gs-contracts`](https://github.com/MCopparini/glue_suite_contracts).

## Uso

```bash
npm install
npm run build

# attivazione (una volta sola, con il token mostrato da GlueSuite alla creazione dell'agente)
node dist/cli.js enroll --hub https://<hub> --token gsat_...

# avvio
node dist/cli.js run

# identita' dell'agente (nessun segreto)
node dist/cli.js status
```

Opzione comune `--data-dir <cartella>` (oppure `GS_AGENT_DATA_DIR`). Default:
Windows `%ProgramData%\GlueSuite\agent`, Linux `/var/lib/gs-agent` (root) o `~/.gs-agent`.

## Cosa resta sulla macchina

| File | Contenuto |
|---|---|
| `private.key` | Chiave privata X25519 dell'agente: non lascia mai la macchina |
| `identity.json` | Identità, indirizzo del broker, credenziali cifrate con la chiave dell'agente |
| `secrets.json` | Segreti dei connettori (es. password del DB 3CAD), ciascuno cifrato con la chiave dell'agente |
| `state.json` | Ultime configurazioni ricevute dall'hub |

GlueSuite conserva la configurazione dei connettori ma **non i segreti**: dell'agente conosce solo
presenza e impronta HMAC dei segreti, riportate nell'heartbeat.

## Stato

Versione 0.1: attivazione, connessione al broker con riconnessione automatica, heartbeat, comandi
(configurazione, start/pausa/stop, consegna segreti, log, livello di log). I moduli dei connettori non ci sono
ancora: un connettore abilitato risulta in errore "modulo non disponibile".
