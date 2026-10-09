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

## Installazione come servizio

**Linux (systemd)** - utente dedicato `gs-agent`, codice in `/opt/gs-agent`, dati in `/var/lib/gs-agent`,
unità con le protezioni di systemd (l'agente scrive solo nella sua cartella dati):

```bash
sudo bash install/linux/install.sh --hub https://<hub> --token gsat_...
journalctl -u gs-agent -f
```

**Windows** - servizio tramite [WinSW](https://github.com/winsw/winsw) (scaricare `WinSW-x64.exe` dalla release
ufficiale), account `LocalService`, dati in `%ProgramData%\GlueSuite\agent` accessibili solo a SYSTEM,
Administrators e LocalService. Da PowerShell come amministratore:

```powershell
.\install\windows\install.ps1 -WinSWPath C:\Downloads\WinSW-x64.exe -Hub https://<hub> -Token gsat_...
```

Entrambi gli script si possono rilanciare per aggiornare il codice: l'attivazione resta.

## Sincronizzazione dai sistemi collegati

I moduli che leggono da un sistema (es. l'import notturno degli articoli da SAP) si pianificano con
`schedule` nella configurazione del connettore, in formato cron a 5 campi e ora locale della macchina
(`"0 2 * * *"` = tutte le notti alle 2). Da GlueSuite si può anche lanciare **sincronizza ora**
(`delta`, solo le novità) o **rileggi tutto** (`full`): `POST /integration/connectors/:id/sync`.
Ultima e prossima esecuzione, esito e dati in attesa si vedono nel runtime del connettore.

**Nessun dato letto va perso.** Ogni dato letto viene salvato su disco (`spool/`) prima di essere inviato
e cancellato solo quando il broker l'ha preso in carico. Se il broker o la rete non rispondono, i dati
restano su disco e partono appena possibile, nello stesso ordine e con lo stesso identificativo
(l'hub scarta eventuali doppioni). Importante per i sistemi che considerano un dato consegnato appena
letto, come i change pointer di SAP.

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
