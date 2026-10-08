import type { LogLevel, LogLine } from '@mcopparini/gs-contracts';

// Logger dell'agente: console + buffer circolare in memoria, da cui l'hub legge le ultime righe
// (comando agent.logs.tail). I valori dei segreti non devono mai arrivare qui.

const ORDER: Record<LogLevel, number> = { error: 0, warn: 1, info: 2, debug: 3 };
const BUFFER_SIZE = 2000;

export class Logger {
    private level: LogLevel = 'info';
    private readonly buffer: LogLine[] = [];
    private readonly listeners = new Set<(line: LogLine) => void>();

    setLevel(level: LogLevel) {
        this.level = level;
    }

    getLevel() {
        return this.level;
    }

    error(msg: string, connectorId?: string) { this.write('error', msg, connectorId); }
    warn(msg: string, connectorId?: string) { this.write('warn', msg, connectorId); }
    info(msg: string, connectorId?: string) { this.write('info', msg, connectorId); }
    debug(msg: string, connectorId?: string) { this.write('debug', msg, connectorId); }

    private write(level: LogLevel, msg: string, connectorId?: string) {
        if (ORDER[level] > ORDER[this.level]) return;
        const line: LogLine = { ts: new Date().toISOString(), level, msg, ...(connectorId ? { connectorId } : {}) };
        this.buffer.push(line);
        if (this.buffer.length > BUFFER_SIZE) this.buffer.shift();
        const prefix = connectorId ? `[${connectorId}] ` : '';
        (level === 'error' ? console.error : console.log)(`${line.ts} ${level.toUpperCase().padEnd(5)} ${prefix}${msg}`);
        for (const listener of this.listeners) listener(line);
    }

    tail(lines: number, connectorId?: string): LogLine[] {
        const filtered = connectorId ? this.buffer.filter(l => l.connectorId === connectorId) : this.buffer;
        return filtered.slice(-Math.max(1, Math.min(lines, BUFFER_SIZE)));
    }

    onLine(listener: (line: LogLine) => void) {
        this.listeners.add(listener);
        return () => this.listeners.delete(listener);
    }
}

export const log = new Logger();
