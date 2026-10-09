import { mkdir, readdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import type { GsEnvelope, RecordResultPayload } from '@mcopparini/gs-contracts';
import type { ConnectorModule, ModuleContext } from './types.js';

// Modulo "file" (banco di prova): fa da sistema esterno con due cartelle.
//   inboxDir   i .json depositati qui vengono inviati all'hub (poi spostati in inbox/processed o inbox/failed)
//   outboxDir  i dati ricevuti dall'hub vengono scritti qui come <tipo>__<codice>.json
// Configurazione: { inboxDir, outboxDir, messageType = "item.upsert", codeField = "code", pollSec = 5 }

const VERSION = '0.1.0';
const safe = (s: string) => s.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 120);

function getPath(obj: any, path: string): unknown {
    return path.split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);
}

export function createFileModule(ctx: ModuleContext): ConnectorModule {
    const inbox = resolve(String(ctx.config.inboxDir ?? ''));
    const outbox = resolve(String(ctx.config.outboxDir ?? ''));
    const messageType = String(ctx.config.messageType ?? 'item.upsert');
    const codeField = String(ctx.config.codeField ?? 'code');
    const pollMs = Math.max(1, Number(ctx.config.pollSec ?? 5)) * 1000;
    let timer: NodeJS.Timeout | undefined;
    let scanning = false;

    if (!ctx.config.inboxDir || !ctx.config.outboxDir) throw new Error('inboxDir e outboxDir sono obbligatori');
    if (inbox === outbox) throw new Error('inboxDir e outboxDir devono essere cartelle diverse');

    const stamp = () => new Date().toISOString().replace(/[:.]/g, '-');

    async function scan() {
        if (scanning) return;
        scanning = true;
        try {
            const files = (await readdir(inbox, { withFileTypes: true }))
                .filter(f => f.isFile() && f.name.toLowerCase().endsWith('.json'))
                .map(f => f.name)
                .sort();
            for (const name of files) {
                const path = join(inbox, name);
                let payload: unknown;
                try {
                    payload = JSON.parse(await readFile(path, 'utf8'));
                } catch (err) {
                    ctx.log.warn(`${name}: JSON non valido, spostato in failed/ (${(err as Error).message})`);
                    await rename(path, join(inbox, 'failed', `${stamp()}__${name}`));
                    continue;
                }
                // codice del dato nel "sistema": se manca, nessun codice esterno (mai il nome del file)
                const code = getPath(payload, codeField);
                await ctx.emit(messageType, payload, code !== undefined && code !== null && String(code) ? String(code) : undefined);
                await rename(path, join(inbox, 'processed', `${stamp()}__${name}`));
                ctx.log.info(`${name} inviato all'hub (${messageType})`);
            }
        } catch (err) {
            ctx.log.error(`lettura di ${inbox} fallita: ${(err as Error).message}`);
        } finally {
            scanning = false;
        }
    }

    return {
        version: VERSION,

        async start() {
            await mkdir(join(inbox, 'processed'), { recursive: true });
            await mkdir(join(inbox, 'failed'), { recursive: true });
            await mkdir(outbox, { recursive: true });
            timer = setInterval(() => void scan(), pollMs);
            ctx.log.info(`in ascolto su ${inbox} (ogni ${pollMs / 1000}s), scrittura in ${outbox}`);
            void scan();
        },

        async stop() {
            if (timer) clearInterval(timer);
            timer = undefined;
        },

        async handle(env: GsEnvelope): Promise<RecordResultPayload> {
            const code = getPath(env.payload, codeField);
            const externalCode = code !== undefined && code !== null && String(code) ? String(code) : undefined;
            const name = `${safe(env.type)}__${safe(externalCode ?? env.id)}.json`;
            const tmp = join(outbox, `.${name}.tmp`);
            await writeFile(tmp, JSON.stringify(env.payload, null, 2), 'utf8');
            await rename(tmp, join(outbox, name));   // scrittura atomica: chi legge non vede file a meta'
            ctx.log.info(`ricevuto ${env.type} ${externalCode ?? ''} -> ${name}`);
            return { ok: true, ...(externalCode ? { externalCode } : {}) };
        },
    };
}
