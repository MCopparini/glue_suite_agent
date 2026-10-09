import { mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { GsEnvelope } from '@mcopparini/gs-contracts';

// Spool su disco dei dati letti dai sistemi collegati, finche' il broker non li ha presi in carico.
// Serve perche' alcuni sistemi (es. i change pointer SAP) considerano un dato consegnato appena letto:
// se in quel momento il broker o la rete non ci sono, il dato non deve perdersi.
// Un file per messaggio, in ordine di arrivo; l'id della busta resta lo stesso nei ritentativi,
// quindi l'hub riconosce e scarta eventuali doppioni.

export class Spool {
    constructor(private readonly root: string) {
        mkdirSync(root, { recursive: true, mode: 0o700 });
    }

    private dir(connectorId: string) {
        const d = join(this.root, connectorId.replace(/[^a-z0-9_-]/g, '_'));
        mkdirSync(d, { recursive: true, mode: 0o700 });
        return d;
    }

    // Scrittura atomica: un file a meta' non viene mai rinviato
    save(connectorId: string, env: GsEnvelope): string {
        const dir = this.dir(connectorId);
        const name = `${Date.now().toString().padStart(15, '0')}_${env.id}.json`;
        const tmp = join(dir, `.${name}.tmp`);
        writeFileSync(tmp, JSON.stringify(env), { mode: 0o600 });
        renameSync(tmp, join(dir, name));
        return join(dir, name);
    }

    remove(path: string) {
        rmSync(path, { force: true });
    }

    // In attesa, in ordine di arrivo
    pending(connectorId?: string): { connectorId: string, path: string }[] {
        const ids = connectorId ? [connectorId] : readdirSync(this.root, { withFileTypes: true }).filter(d => d.isDirectory()).map(d => d.name);
        const out: { connectorId: string, path: string }[] = [];
        for (const id of ids) {
            const dir = this.dir(id);
            for (const f of readdirSync(dir).filter(n => n.endsWith('.json')).sort()) out.push({ connectorId: id, path: join(dir, f) });
        }
        return out;
    }

    count(connectorId: string): number {
        return readdirSync(this.dir(connectorId)).filter(n => n.endsWith('.json')).length;
    }

    read(path: string): GsEnvelope {
        return JSON.parse(readFileSync(path, 'utf8'));
    }
}
