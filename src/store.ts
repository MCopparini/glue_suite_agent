import { chmodSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { createHmac, randomBytes } from 'node:crypto';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { open, type EnrollmentResponse, type SealedBox, type SecretReport } from '@mcopparini/gs-contracts';

// Dati locali dell'agente (una cartella, permessi ristretti):
//   identity.json  identita' e credenziali broker (cifrate con la chiave dell'agente)
//   private.key    chiave privata X25519: non lascia mai questa macchina
//   secrets.json   segreti dei connettori, ciascuno cifrato con la chiave dell'agente
//   state.json     configurazioni ricevute dall'hub (per ripartire senza attendere l'hub)
// TODO: su Windows proteggere private.key con DPAPI; oggi vale solo la ACL della cartella.

export function defaultDataDir(): string {
    if (process.env.GS_AGENT_DATA_DIR) return process.env.GS_AGENT_DATA_DIR;
    if (process.platform === 'win32') return join(process.env.ProgramData ?? 'C:\\ProgramData', 'GlueSuite', 'agent');
    if (process.getuid?.() === 0) return '/var/lib/gs-agent';
    return join(homedir(), '.gs-agent');
}

export interface AgentIdentity extends Omit<EnrollmentResponse, 'sealedCredentials'> {
    hubUrl: string;
    enrolledAt: string;
    sealedCredentials: SealedBox;
    hmacKey: string;   // chiave locale per le impronte dei segreti (base64)
}

function writeSecure(path: string, content: string) {
    const tmp = `${path}.tmp`;
    writeFileSync(tmp, content, { mode: 0o600 });
    renameSync(tmp, path);
    try { chmodSync(path, 0o600); } catch { /* Windows: permessi dalla ACL della cartella */ }
}

function readJson<T>(path: string, fallback: T): T {
    if (!existsSync(path)) return fallback;
    return JSON.parse(readFileSync(path, 'utf8')) as T;
}

export class AgentStore {
    constructor(readonly dir: string) {
        mkdirSync(dir, { recursive: true, mode: 0o700 });
    }

    private path(name: string) {
        return join(this.dir, name);
    }

    isEnrolled() {
        return existsSync(this.path('identity.json')) && existsSync(this.path('private.key'));
    }

    saveEnrollment(identity: Omit<AgentIdentity, 'hmacKey'>, privateKeyPem: string) {
        writeSecure(this.path('private.key'), privateKeyPem);
        writeSecure(this.path('identity.json'), JSON.stringify({ ...identity, hmacKey: randomBytes(32).toString('base64') }, null, 2));
    }

    identity(): AgentIdentity {
        if (!this.isEnrolled()) throw new Error(`agente non attivato (cartella dati: ${this.dir}): esegui "gs-agent enroll"`);
        return readJson<AgentIdentity>(this.path('identity.json'), null as never);
    }

    privateKey(): string {
        return readFileSync(this.path('private.key'), 'utf8');
    }

    brokerPassword(): string {
        const { password } = JSON.parse(open(this.privateKey(), this.identity().sealedCredentials).toString('utf8'));
        return password;
    }

    //#region segreti dei connettori

    private secrets(): Record<string, Record<string, SealedBox>> {
        return readJson(this.path('secrets.json'), {});
    }

    setSecret(connectorId: string, ref: string, sealed: SealedBox) {
        open(this.privateKey(), sealed); // verifica che sia leggibile prima di salvarlo
        const all = this.secrets();
        (all[connectorId] ??= {})[ref] = sealed;
        writeSecure(this.path('secrets.json'), JSON.stringify(all, null, 2));
    }

    getSecret(connectorId: string, ref: string): string | undefined {
        const sealed = this.secrets()[connectorId]?.[ref];
        return sealed ? open(this.privateKey(), sealed).toString('utf8') : undefined;
    }

    // Per l'heartbeat: presenza e impronta HMAC (con chiave locale), mai il valore.
    secretReports(connectorId: string, refs: string[]): SecretReport[] {
        const key = Buffer.from(this.identity().hmacKey, 'base64');
        return refs.map(ref => {
            const value = this.getSecret(connectorId, ref);
            return value === undefined
                ? { ref, present: false }
                : { ref, present: true, fingerprint: 'hmac-sha256:' + createHmac('sha256', key).update(`${connectorId}\0${ref}\0${value}`).digest('hex').slice(0, 32) };
        });
    }

    //#endregion

    loadState<T>(fallback: T): T {
        return readJson(this.path('state.json'), fallback);
    }

    saveState(state: unknown) {
        writeSecure(this.path('state.json'), JSON.stringify(state, null, 2));
    }
}
