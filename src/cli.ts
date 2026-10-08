#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { AgentStore, defaultDataDir } from './store.js';
import { enroll } from './enroll.js';
import { AgentRuntime } from './runtime.js';
import { AGENT_VERSION } from './version.js';
import { log } from './logger.js';

const USAGE = `gs-agent ${AGENT_VERSION}

Uso:
  gs-agent enroll --hub <url> --token <gsat_...> [--force]   attiva l'agente (una volta sola)
  gs-agent run                                               avvia l'agente
  gs-agent status                                            mostra l'identita' dell'agente

Opzioni comuni:
  --data-dir <cartella>   dati locali (default: ${defaultDataDir()}, oppure GS_AGENT_DATA_DIR)
`;

async function main() {
    const { values, positionals } = parseArgs({
        allowPositionals: true,
        options: {
            hub: { type: 'string' },
            token: { type: 'string' },
            force: { type: 'boolean', default: false },
            'data-dir': { type: 'string' },
            help: { type: 'boolean', short: 'h', default: false },
        },
    });
    const command = positionals[0];
    if (values.help || !command) {
        console.log(USAGE);
        return;
    }

    const store = new AgentStore(values['data-dir'] ?? defaultDataDir());

    switch (command) {
        case 'enroll': {
            if (!values.hub || !values.token) throw new Error('servono --hub e --token');
            await enroll(store, values.hub, values.token, { force: values.force });
            return;
        }
        case 'status': {
            if (!store.isEnrolled()) {
                console.log(`agente non attivato (cartella dati: ${store.dir})`);
                return;
            }
            const { sealedCredentials, hmacKey, ...identity } = store.identity();
            console.log(JSON.stringify({ dataDir: store.dir, version: AGENT_VERSION, ...identity }, null, 2));
            return;
        }
        case 'run': {
            const runtime = new AgentRuntime(store);
            const shutdown = async () => {
                await runtime.stop();
                process.exit(0);
            };
            process.on('SIGINT', shutdown);
            process.on('SIGTERM', shutdown);
            await runtime.start();
            return;
        }
        default:
            throw new Error(`comando sconosciuto: ${command}\n\n${USAGE}`);
    }
}

main().catch(err => {
    log.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
});
