import amqplib from 'amqplib';
import type { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, RecoveringChannelModel } from 'amqplib';
import {
    AGENT_SYSTEM, EXCHANGES, MESSAGE_TYPES, agentControlQueue, collectSecretRefs, connectorQueue, createEnvelope,
    createReply, ingressRoutingKey, parseEnvelope, resolveSecretRefs, resultType,
    type CommandResultPayload, type ConnectorConfigPayload, type ConnectorReport, type ConnectorRuntimeState,
    type ConnectorSetSecretPayload, type GsEnvelope, type HeartbeatPayload, type LogLevel, type LogLine,
    type LogsStreamPayload, type LogsTailPayload, type RecordResultPayload, type SetLogLevelPayload
} from '@mcopparini/gs-contracts';
import { MODULES, type ConnectorModule } from './modules/index.js';
import { AgentStore, type AgentIdentity } from './store.js';
import { AGENT_VERSION } from './version.js';
import { log } from './logger.js';

// Runtime dell'agente: connessione al broker con le proprie credenziali, comandi dalla coda
// agent.<code>, heartbeat periodico verso l'hub, moduli dei connettori.
// Ogni connettore segue lo stato voluto inviato dall'hub (connector.config):
//   enabled   modulo avviato con la configurazione (segreti risolti dall'archivio locale)
//             e consumo della coda dati conn.<id>
//   paused    modulo fermo e coda non consumata: i dati restano in coda
//   disabled  modulo fermo
// Un tipo senza modulo in questa versione dell'agente risulta in errore.

interface ConnectorState {
    config: ConnectorConfigPayload | null;
    desired: 'enabled' | 'paused' | 'disabled';
    processed: number;
    failed: number;
    lastError?: string;
    state: ConnectorRuntimeState;
    module?: ConnectorModule;
    runningVersion?: number;
    dataChannel?: Channel;
    queue?: Promise<void>;   // le riconciliazioni di un connettore sono in serie
}

type PersistedState = Record<string, { config: ConnectorConfigPayload | null; desired: ConnectorState['desired'] }>;

const LOG_LEVELS: LogLevel[] = ['error', 'warn', 'info', 'debug'];
const MAX_STREAM_SEC = 900;

export class AgentRuntime {
    private readonly identity: AgentIdentity;
    private readonly connectors = new Map<string, ConnectorState>();
    private model?: RecoveringChannelModel;
    private pub?: ConfirmChannel;
    private heartbeatTimer?: NodeJS.Timeout;
    private readonly startedAt = new Date();
    private stopping = false;
    private logStream?: { stop: () => void };

    constructor(private readonly store: AgentStore) {
        this.identity = store.identity();
        const saved = store.loadState<PersistedState>({});
        for (const [id, s] of Object.entries(saved)) {
            this.connectors.set(id, { config: s.config, desired: s.desired, processed: 0, failed: 0, state: 'stopped' });
        }
    }

    private brokerUrl() {
        const { broker } = this.identity;
        const u = new URL(broker.url);
        u.username = encodeURIComponent(broker.username);
        u.password = encodeURIComponent(this.store.brokerPassword());
        u.pathname = '/' + encodeURIComponent(broker.vhost);
        return u.toString();
    }

    async start() {
        const { agentCode, broker } = this.identity;
        log.info(`gs-agent ${AGENT_VERSION} - agente ${agentCode}, broker ${broker.url} vhost ${broker.vhost}`);

        this.model = await amqplib.connect(this.brokerUrl(), {
            recovery: { maxDelay: 30_000, setup: (m: ChannelModel) => this.setup(m) }
        });
        this.model.on('disconnect', err => log.warn(`broker disconnesso (${err.message}), riconnessione in corso`));
        this.model.on('connect', () => log.info('collegato al broker'));
        this.model.on('error', err => log.error(`errore broker: ${err.message}`));

        // riparte con le ultime configurazioni ricevute, senza attendere l'hub
        for (const id of this.connectors.keys()) await this.reconcile(id);

        const intervalMs = Math.max(5, this.identity.heartbeatIntervalSec) * 1000;
        this.heartbeatTimer = setInterval(() => void this.sendHeartbeat(), intervalMs);
        await this.sendHeartbeat();
    }

    // A ogni (ri)connessione: canale di pubblicazione e consumer della coda di controllo.
    // Le code le crea l'hub: l'agente non ha permessi di configure.
    private async setup(model: ChannelModel) {
        const pub = await model.createConfirmChannel();
        pub.on('error', err => log.error(`canale di pubblicazione: ${err.message}`));
        pub.on('close', () => { if (this.pub === pub) this.pub = undefined; });
        this.pub = pub;

        const ch = await model.createChannel();
        ch.on('error', err => log.error(`canale comandi: ${err.message}`));
        await ch.prefetch(10);
        await ch.consume(agentControlQueue(this.identity.agentCode), msg => {
            if (msg) void this.onControl(ch, msg);
        });

        // dopo una riconnessione i canali dati sono chiusi: si riaprono per i connettori in esecuzione
        for (const [id, s] of this.connectors) {
            if (s.module) {
                s.dataChannel = undefined;
                await this.startData(model, id, s).catch(err => log.error(`coda dati non riaperta: ${(err as Error).message}`, id));
            }
        }
    }

    //#region moduli dei connettori

    private reconcile(id: string): Promise<void> {
        const s = this.connector(id);
        const next = (s.queue ?? Promise.resolve()).then(() => this.doReconcile(id, s)).catch(err => {
            s.state = 'error';
            s.lastError = (err as Error).message;
            log.error(`riconciliazione fallita: ${s.lastError}`, id);
        });
        s.queue = next;
        return next;
    }

    private async doReconcile(id: string, s: ConnectorState) {
        const cfg = s.config;
        const want = s.desired === 'enabled' && !!cfg && cfg.configVersion > 0;
        if (want && s.module && s.runningVersion === cfg!.configVersion) {
            if (!s.dataChannel && this.model) await this.startData(this.model, id, s);
            return;
        }

        await this.stopModule(id, s);
        if (!want) {
            s.state = s.desired === 'paused' ? 'paused' : 'stopped';
            s.lastError = undefined;
            return;
        }

        const factory = MODULES[cfg!.typeCode];
        if (!factory) {
            s.state = 'error';
            s.lastError = `modulo "${cfg!.typeCode}" non disponibile in questa versione dell'agente`;
            return;
        }
        try {
            s.state = 'starting';
            const config = resolveSecretRefs(cfg!.config, ref => this.store.getSecret(id, ref));
            const module = factory({
                connectorId: id,
                typeCode: cfg!.typeCode,
                config,
                log: {
                    error: m => log.error(m, id), warn: m => log.warn(m, id),
                    info: m => log.info(m, id), debug: m => log.debug(m, id),
                },
                emit: (type, payload, externalId) => this.emitData(id, cfg!.typeCode, type, payload, externalId),
            });
            await module.start();
            s.module = module;
            s.runningVersion = cfg!.configVersion;
            if (this.model) await this.startData(this.model, id, s);
            s.state = 'running';
            s.lastError = undefined;
            log.info(`modulo ${cfg!.typeCode} ${module.version} avviato (configurazione v${cfg!.configVersion})`, id);
        } catch (err) {
            await this.stopModule(id, s);
            s.state = 'error';
            s.lastError = (err as Error).message;
            log.error(`avvio del modulo fallito: ${s.lastError}`, id);
        }
    }

    private async stopModule(id: string, s: ConnectorState) {
        const ch = s.dataChannel;
        s.dataChannel = undefined;
        await ch?.close().catch(() => {});   // i messaggi non confermati tornano in coda
        const module = s.module;
        s.module = undefined;
        s.runningVersion = undefined;
        if (module) {
            await module.stop().catch(err => log.warn(`arresto del modulo: ${(err as Error).message}`, id));
            log.info('modulo fermato', id);
        }
    }

    // Coda dati del connettore (conn.<id>): dati in arrivo dall'hub, gia' nel formato nativo.
    private async startData(source: Pick<ChannelModel, 'createChannel'>, id: string, s: ConnectorState) {
        const ch = await source.createChannel();
        ch.on('error', err => log.error(`canale dati: ${err.message}`, id));
        ch.on('close', () => { if (s.dataChannel === ch) s.dataChannel = undefined; });
        await ch.prefetch(5);
        await ch.consume(connectorQueue(id), msg => {
            if (msg) void this.onData(ch, id, s, msg);
        });
        s.dataChannel = ch;
    }

    private async onData(ch: Channel, id: string, s: ConnectorState, msg: ConsumeMessage) {
        let env: GsEnvelope;
        try {
            env = parseEnvelope(msg.content);
            if (env.domainId !== this.identity.domainId || env.target?.connectorId !== id) throw new Error('dato non destinato a questo connettore');
        } catch (err) {
            log.error(`dato scartato: ${(err as Error).message}`, id);
            ch.nack(msg, false, false);
            return;
        }
        const module = s.module;
        if (!module) {
            ch.nack(msg, false, true);   // modulo in arresto: il dato torna in coda
            return;
        }

        let result: RecordResultPayload;
        try {
            result = await module.handle(env);
            if (result.ok) s.processed++; else s.failed++;
        } catch (err) {
            result = { ok: false, error: (err as Error).message };
            s.failed++;
            log.warn(`${env.type} non elaborato: ${result.error}`, id);
        }
        try {
            await this.publish(createReply(env, {
                type: resultType(env.type), schemaVersion: 1,
                source: { system: s.config!.typeCode, connectorId: id },
                payloadFormat: 'canonical', payload: result
            }));
            ch.ack(msg);
        } catch (err) {
            log.error(`esito di ${env.type} non inviato: ${(err as Error).message}`, id);
            ch.nack(msg, false, true);
        }
    }

    // Dato nativo verso l'hub (gs.ingress), a nome del connettore.
    private async emitData(id: string, typeCode: string, type: string, payload: unknown, externalId?: string) {
        await this.publish(createEnvelope({
            kind: 'event', type, schemaVersion: 1, domainId: this.identity.domainId,
            source: { system: typeCode, connectorId: id, ...(externalId ? { externalId: String(externalId).slice(0, 200) } : {}) },
            payloadFormat: `native:${typeCode}`, payload
        }));
    }

    //#endregion

    private async publish(env: GsEnvelope) {
        const ch = this.pub;
        if (!ch) throw new Error('broker non collegato');
        await new Promise<void>((resolve, reject) => {
            ch.publish(EXCHANGES.ingress, ingressRoutingKey(env), Buffer.from(JSON.stringify(env)), {
                persistent: true,
                contentType: 'application/json',
                messageId: env.id,
                correlationId: env.correlationId,
                type: env.type,
                timestamp: Math.floor(Date.parse(env.occurredAt) / 1000),
                appId: 'gs-agent',
                // il broker verifica che coincida con l'utente autenticato: l'hub sa con certezza chi scrive
                userId: this.identity.broker.username,
            }, err => err ? reject(err instanceof Error ? err : new Error(String(err))) : resolve());
        });
    }

    // Log live su gs.logs: messaggi non persistenti, senza conferma; se il broker non c'e' si perdono.
    private publishLog(line: LogLine) {
        const ch = this.pub;
        if (!ch) return;
        const env = createEnvelope({ ...this.envelopeBase(), kind: 'event', type: 'agent.log', payload: line });
        try {
            ch.publish(EXCHANGES.logs, `${this.identity.agentCode}.${line.connectorId ?? 'agent'}`, Buffer.from(JSON.stringify(env)), {
                persistent: false, contentType: 'application/json', messageId: env.id, type: env.type,
                appId: 'gs-agent', userId: this.identity.broker.username,
            });
        } catch { /* canale chiuso: la riga si perde (e non la si logga, per non creare un ciclo) */ }
    }

    private startLogStream(p: LogsStreamPayload) {
        const durationSec = Math.min(Math.max(Number(p?.durationSec) || 300, 10), MAX_STREAM_SEC);
        const minLevel = LOG_LEVELS.indexOf(p?.level ?? 'debug');
        if (minLevel < 0) throw new Error(`livello non valido: ${p?.level}`);
        this.logStream?.stop();   // un nuovo stream sostituisce il precedente

        const off = log.onLine(line => {
            if (LOG_LEVELS.indexOf(line.level) > minLevel) return;
            if (p?.connectorId && line.connectorId !== p.connectorId) return;
            this.publishLog(line);
        });
        const timer = setTimeout(() => stop(), durationSec * 1000);
        const stop = () => {
            off();
            clearTimeout(timer);
            if (this.logStream === handle) this.logStream = undefined;
        };
        const handle = { stop };
        this.logStream = handle;
        log.info(`log live attivi per ${durationSec}s${p?.connectorId ? ` (connettore ${p.connectorId})` : ''}`);
        return { durationSec, until: new Date(Date.now() + durationSec * 1000).toISOString() };
    }

    private envelopeBase() {
        return {
            schemaVersion: 1,
            domainId: this.identity.domainId,
            source: { system: AGENT_SYSTEM, connectorId: this.identity.agentCode },
            payloadFormat: 'canonical' as const,
        };
    }

    //#region heartbeat

    private connectorReport(connectorId: string, s: ConnectorState): ConnectorReport {
        const refs = s.config ? collectSecretRefs(s.config.config) : [];
        const secrets = this.store.secretReports(connectorId, refs);
        return {
            connectorId,
            state: s.state,
            configVersion: s.config?.configVersion ?? null,
            moduleVersion: s.module?.version ?? null,
            processed: s.processed,
            failed: s.failed,
            ...(s.lastError ? { lastError: s.lastError } : {}),
            secrets
        };
    }

    private async sendHeartbeat() {
        if (this.stopping) return;
        const payload: HeartbeatPayload = {
            agentVersion: AGENT_VERSION,
            startedAt: this.startedAt.toISOString(),
            uptimeSec: Math.round((Date.now() - this.startedAt.getTime()) / 1000),
            connectors: [...this.connectors].map(([id, s]) => this.connectorReport(id, s))
        };
        try {
            await this.publish(createEnvelope({ ...this.envelopeBase(), kind: 'event', type: MESSAGE_TYPES.heartbeat, payload }));
            log.debug('heartbeat inviato');
        } catch (err) {
            log.warn(`heartbeat non inviato: ${(err as Error).message}`);
        }
    }

    //#endregion

    //#region comandi

    private async onControl(ch: Channel, msg: ConsumeMessage) {
        let env: GsEnvelope;
        try {
            env = parseEnvelope(msg.content);
            if (env.domainId !== this.identity.domainId || env.target?.agentCode !== this.identity.agentCode) {
                throw new Error('comando non destinato a questo agente');
            }
        } catch (err) {
            log.error(`comando scartato: ${(err as Error).message}`);
            ch.nack(msg, false, false);
            return;
        }

        let result: CommandResultPayload;
        try {
            result = { ok: true, data: await this.execute(env) };
        } catch (err) {
            result = { ok: false, error: (err as Error).message };
            log.warn(`comando ${env.type} fallito: ${result.error}`, env.target?.connectorId);
        }

        try {
            await this.publish(createReply(env, { ...this.envelopeBase(), type: `${env.type}.result`, payload: result }));
            ch.ack(msg);
        } catch (err) {
            log.error(`risposta a ${env.type} non inviata: ${(err as Error).message}`);
            ch.nack(msg, false, true);
        }
    }

    private connector(connectorId: string | undefined): ConnectorState {
        if (!connectorId) throw new Error('connectorId mancante');
        let s = this.connectors.get(connectorId);
        if (!s) {
            s = { config: null, desired: 'disabled', processed: 0, failed: 0, state: 'stopped' };
            this.connectors.set(connectorId, s);
        }
        return s;
    }

    private persist() {
        const state: PersistedState = {};
        for (const [id, s] of this.connectors) state[id] = { config: s.config, desired: s.desired };
        this.store.saveState(state);
    }

    private async execute(env: GsEnvelope): Promise<unknown> {
        const p = env.payload as any;
        switch (env.type) {
            case MESSAGE_TYPES.connectorConfig: {
                const cfg = p as ConnectorConfigPayload;
                const s = this.connector(cfg.connectorId);
                s.config = cfg;
                s.desired = cfg.desiredState;
                this.persist();
                log.info(`configurazione v${cfg.configVersion} ricevuta (stato voluto: ${cfg.desiredState})`, cfg.connectorId);
                await this.reconcile(cfg.connectorId);
                return { configVersion: cfg.configVersion, state: s.state };
            }
            case MESSAGE_TYPES.connectorStart:
            case MESSAGE_TYPES.connectorPause:
            case MESSAGE_TYPES.connectorStop: {
                const s = this.connector(p?.connectorId);
                s.desired = env.type === MESSAGE_TYPES.connectorStart ? 'enabled' : env.type === MESSAGE_TYPES.connectorPause ? 'paused' : 'disabled';
                this.persist();
                log.info(`stato voluto: ${s.desired}`, p.connectorId);
                await this.reconcile(p.connectorId);
                return { desired: s.desired, state: s.state };
            }
            case MESSAGE_TYPES.connectorSetSecret: {
                const sec = p as ConnectorSetSecretPayload;
                const s = this.connector(sec.connectorId);
                this.store.setSecret(sec.connectorId, sec.ref, sec.sealed);
                log.info(`segreto "${sec.ref}" aggiornato`, sec.connectorId);
                // il modulo in esecuzione riparte con il segreto nuovo (o parte, se mancava)
                s.runningVersion = undefined;
                void this.reconcile(sec.connectorId);
                return { ref: sec.ref };
            }
            case MESSAGE_TYPES.logsTail: {
                const t = p as LogsTailPayload;
                return { lines: log.tail(Number(t?.lines) || 200, t?.connectorId) };
            }
            case MESSAGE_TYPES.logsStream:
                return this.startLogStream(p as LogsStreamPayload);
            case MESSAGE_TYPES.setLogLevel: {
                const l = p as SetLogLevelPayload;
                if (!LOG_LEVELS.includes(l?.level)) throw new Error(`livello non valido: ${l?.level}`);
                log.setLevel(l.level);
                return { level: l.level };
            }
            default:
                throw new Error(`comando non supportato: ${env.type}`);
        }
    }

    //#endregion

    async stop() {
        this.stopping = true;
        if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
        this.logStream?.stop();
        for (const [id, s] of this.connectors) await this.stopModule(id, s);
        await this.model?.close().catch(() => {});
        log.info('agente fermato');
    }
}
