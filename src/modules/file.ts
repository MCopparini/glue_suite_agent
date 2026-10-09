import { mkdir, readdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import type { GsEnvelope, RecordResultPayload, SyncMode } from '@mcopparini/gs-contracts';
import type { ConnectorModule, ModuleContext } from './types.js';

// Modulo "file" (banco di prova): fa da sistema esterno con due cartelle.
//   inboxDir   i .json depositati qui vengono inviati all'hub (poi spostati in inbox/processed o inbox/failed)
//   outboxDir  i dati ricevuti dall'hub vengono scritti qui come <tipo>__<codice>.json
// Configurazione: { inboxDir, outboxDir, messageType = "item.upsert", codeField = "code", pollSec = 5, schedule? }
//   senza schedule  la inbox si legge ogni pollSec secondi
//   con schedule    la inbox si legge solo agli orari pianificati (come un import notturno) o su richiesta
// Sincronizzazione: delta = legge la inbox; full = "rileggi tutto": rimanda anche l'archivio processed/.

const VERSION = '0.2.0';
const safe = (s: string) => s.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 120);

function getPath(obj: any, path: string): unknown {
    return path.split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);
}

export function createFileModule(ctx: ModuleContext): ConnectorModule {
    const inbox = resolve(String(ctx.config.inboxDir ?? ''));
    const outbox = resolve(String(ctx.config.outboxDir ?? ''));
    const processed = join(inbox, 'processed');
    const messageType = String(ctx.config.messageType ?? 'item.upsert');
    const codeField = String(ctx.config.codeField ?? 'code');
    const pollMs = Math.max(1, Number(ctx.config.pollSec ?? 5)) * 1000;
    const scheduled = !!ctx.config.schedule;
    let timer: NodeJS.Timeout | undefined;
    let scanning: Promise<number> | null = null;

    if (!ctx.config.inboxDir || !ctx.config.outboxDir) throw new Error('inboxDir e outboxDir sono obbligatori');
    if (inbox === outbox) throw new Error('inboxDir e outboxDir devono essere cartelle diverse');

    const stamp = () => new Date().toISOString().replace(/[:.]/g, '-');
    const jsonFiles = async (dir: string) => (await readdir(dir, { withFileTypes: true }))
        .filter(f => f.isFile() && f.name.toLowerCase().endsWith('.json'))
        .map(f => f.name)
        .sort();

    // codice del dato nel "sistema": se manca, nessun codice esterno (mai il nome del file)
    async function send(payload: unknown) {
        const code = getPath(payload, codeField);
        await ctx.emit(messageType, payload, code !== undefined && code !== null && String(code) ? String(code) : undefined);
    }

    async function scanInbox(): Promise<number> {
        let count = 0;
        for (const name of await jsonFiles(inbox)) {
            const path = join(inbox, name);
            let payload: unknown;
            try {
                payload = JSON.parse(await readFile(path, 'utf8'));
            } catch (err) {
                ctx.log.warn(`${name}: JSON non valido, spostato in failed/ (${(err as Error).message})`);
                await rename(path, join(inbox, 'failed', `${stamp()}__${name}`));
                continue;
            }
            // ctx.emit salva il dato su disco prima di inviarlo: spostarlo in processed/ e' sicuro anche se il broker non c'e'
            await send(payload);
            await rename(path, join(processed, `${stamp()}__${name}`));
            ctx.log.info(`${name} inviato all'hub (${messageType})`);
            count++;
        }
        return count;
    }

    // Una lettura alla volta: le richieste che arrivano durante una lettura la attendono
    function scan(): Promise<number> {
        if (!scanning) {
            scanning = scanInbox()
                .catch(err => { ctx.log.error(`lettura di ${inbox} fallita: ${(err as Error).message}`); throw err; })
                .finally(() => { scanning = null; });
        }
        return scanning;
    }

    return {
        version: VERSION,

        async start() {
            await mkdir(processed, { recursive: true });
            await mkdir(join(inbox, 'failed'), { recursive: true });
            await mkdir(outbox, { recursive: true });
            if (scheduled) {
                ctx.log.info(`inbox ${inbox} letta agli orari pianificati (${ctx.config.schedule}), scrittura in ${outbox}`);
            } else {
                timer = setInterval(() => void scan().catch(() => {}), pollMs);
                ctx.log.info(`in ascolto su ${inbox} (ogni ${pollMs / 1000}s), scrittura in ${outbox}`);
                void scan().catch(() => {});
            }
        },

        async stop() {
            if (timer) clearInterval(timer);
            timer = undefined;
        },

        async sync(mode: SyncMode) {
            let count = 0;
            if (mode === 'full') {
                // "rileggi tutto": l'intero archivio gia' elaborato, poi le novita'
                for (const name of await jsonFiles(processed)) {
                    try {
                        await send(JSON.parse(await readFile(join(processed, name), 'utf8')));
                        count++;
                    } catch (err) {
                        ctx.log.warn(`${name}: non riletto (${(err as Error).message})`);
                    }
                }
                ctx.log.info(`rilettura completa: ${count} dati dall'archivio`);
            }
            return { count: count + await scan() };
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
