import { hostname, release } from 'node:os';
import {
    ENROLLMENT_PATH, ENROLLMENT_TOKEN_PREFIX, generateAgentKeyPair, open,
    type EnrollmentRequest, type EnrollmentResponse
} from '@mcopparini/gs-contracts';
import { AgentStore } from './store.js';
import { AGENT_VERSION } from './version.js';
import { log } from './logger.js';

// Attivazione: genera la coppia di chiavi, la registra sull'hub con il token monouso,
// salva identita' e credenziali (che restano cifrate con la chiave appena generata).
export async function enroll(store: AgentStore, hubUrl: string, token: string, opts: { force?: boolean } = {}) {
    if (!token.startsWith(ENROLLMENT_TOKEN_PREFIX)) throw new Error(`token non valido: deve iniziare con "${ENROLLMENT_TOKEN_PREFIX}"`);
    if (store.isEnrolled() && !opts.force) {
        throw new Error(`agente gia' attivato in ${store.dir}: usa --force per rifare l'attivazione`);
    }
    const hub = new URL(hubUrl);
    if (hub.protocol !== 'https:' && !['localhost', '127.0.0.1', '[::1]'].includes(hub.hostname)) {
        throw new Error('l\'hub deve essere raggiunto in HTTPS (http solo verso localhost, per sviluppo)');
    }

    const keys = generateAgentKeyPair();
    const request: EnrollmentRequest = {
        token,
        publicKey: keys.publicKey,
        agentVersion: AGENT_VERSION,
        host: { hostname: hostname(), platform: process.platform, arch: process.arch, osRelease: release() }
    };

    const res = await fetch(new URL(ENROLLMENT_PATH, hub), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(request),
        signal: AbortSignal.timeout(30_000)
    });
    const body = await res.json().catch(() => null) as any;
    if (!res.ok) throw new Error(`attivazione rifiutata dall'hub (${res.status}): ${body?.message ?? 'risposta non valida'}`);

    const response = body as EnrollmentResponse;
    open(keys.privateKeyPem, response.sealedCredentials); // le credenziali devono essere leggibili con la nostra chiave

    store.saveEnrollment({ ...response, hubUrl: hub.origin, enrolledAt: new Date().toISOString() }, keys.privateKeyPem);
    log.info(`agente attivato: ${response.agentCode} (dominio ${response.domainId}), broker ${response.broker.url} vhost ${response.broker.vhost}`);
    return response;
}
