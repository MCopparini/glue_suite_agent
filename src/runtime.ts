import amqplib from 'amqplib';
import type { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, RecoveringChannelModel } from 'amqplib';
import {
    AGENT_SYSTEM, EXCHANGES, MESSAGE_TYPES, agentControlQueue, collectSecretRefs, createEnvelope, createReply,
    ingressRoutingKey, parseEnvelope,
    type CommandResultPayload, type ConnectorConfigPayload, type ConnectorReport, type ConnectorRuntimeState,
    type ConnectorSetSecretPayload, type GsEnvelope, type HeartbeatPayload, type LogLevel, type LogsTailPayload,
    type SetLogLevelPayload
} from '@mcopparini/gs-contracts';
import { AgentStore, type AgentIdentity } from './store.js';
import { AGENT_VERSION } from './version.js';
import { log } from './logger.js';

// Runtime dell'agente: connessione al broker con le proprie credenziali, comandi dalla coda
// agent.<code>, heartbeat periodico verso l'hub.
// I moduli dei connettori (3cad, sap...) non ci sono ancora: un connettore abilitato risulta
// in errore "modulo non disponibile" finche' il modulo non viene installato.

interface ConnectorState {
    config: ConnectorConfigPayload | null;
    desired: 'enabled' | 'paused' | 'disabled';
    processed: number;
    failed: number;
    lastError?: string;
}

type PersistedState = Record<string, { config: ConnectorConfigPayload | null; desired: ConnectorState['desired'] }>;

const LOG_LEVELS: LogLevel[] = ['error', 'warn', 'info', 'debug'];

export class AgentRuntime {
    private readonly identity: AgentIdentity;
    private readonly connectors = new Map<string, ConnectorState>();
    private model?: RecoveringChannelModel;
    private pub?: ConfirmChannel;
    private heartbeatTimer?: NodeJS.Timeout;
    private readonly startedAt = new Date();
    private stopping = false;

    constructor(private readonly store: AgentStore) {
        this.identity = store.identity();
        const saved = store.loadState<PersistedState>({});
        for (const [id, s] of Object.entries(saved)) {
            this.connectors.set(id, { config: s.config, desired: s.desired, processed: 0, failed: 0 });
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
    }

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
        let state: ConnectorRuntimeState = 'stopped';
        let lastError = s.lastError;
        if (s.desired === 'paused') state = 'paused';
        if (s.desired === 'enabled') {
            state = 'error';
            lastError = `modulo "${s.config?.typeCode ?? '?'}" non disponibile in questa versione dell'agente`;
        }
        return {
            connectorId,
            state,
            configVersion: s.config?.configVersion ?? null,
            moduleVersion: null,
            processed: s.processed,
            failed: s.failed,
            ...(lastError ? { lastError } : {}),
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
            s = { config: null, desired: 'disabled', processed: 0, failed: 0 };
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
                return { configVersion: cfg.configVersion };
            }
            case MESSAGE_TYPES.connectorStart:
            case MESSAGE_TYPES.connectorPause:
            case MESSAGE_TYPES.connectorStop: {
                const s = this.connector(p?.connectorId);
                s.desired = env.type === MESSAGE_TYPES.connectorStart ? 'enabled' : env.type === MESSAGE_TYPES.connectorPause ? 'paused' : 'disabled';
                this.persist();
                log.info(`stato voluto: ${s.desired}`, p.connectorId);
                return { desired: s.desired };
            }
            case MESSAGE_TYPES.connectorSetSecret: {
                const sec = p as ConnectorSetSecretPayload;
                this.connector(sec.connectorId);
                this.store.setSecret(sec.connectorId, sec.ref, sec.sealed);
                log.info(`segreto "${sec.ref}" aggiornato`, sec.connectorId);
                return { ref: sec.ref };
            }
            case MESSAGE_TYPES.logsTail: {
                const t = p as LogsTailPayload;
                return { lines: log.tail(Number(t?.lines) || 200, t?.connectorId) };
            }
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
        await this.model?.close().catch(() => {});
        log.info('agente fermato');
    }
}
