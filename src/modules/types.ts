import type { GsEnvelope, RecordResultPayload } from '@mcopparini/gs-contracts';

// Interfaccia di un modulo connettore. Il modulo fa solo il trasporto verso il proprio sistema
// (leggere, scrivere, convertire XML/paginazione...): NON traduce verso il modello canonico,
// lo fa l'hub con i mapper. I dati viaggiano nel formato nativo "native:<tipo>".

export interface ModuleLogger {
    error(msg: string): void;
    warn(msg: string): void;
    info(msg: string): void;
    debug(msg: string): void;
}

export interface ModuleContext {
    connectorId: string;
    typeCode: string;
    config: Record<string, any>;   // configurazione con i segreti gia' risolti (solo in memoria)
    log: ModuleLogger;
    // Invia all'hub un dato nativo; externalId = codice del dato nel sistema collegato.
    emit(messageType: string, payload: unknown, externalId?: string): Promise<void>;
}

export interface ConnectorModule {
    readonly version: string;
    start(): Promise<void>;
    stop(): Promise<void>;
    // Dato in arrivo dall'hub (gia' nel formato nativo): esito e codice usato nel sistema.
    handle(env: GsEnvelope): Promise<RecordResultPayload>;
}

export type ModuleFactory = (ctx: ModuleContext) => ConnectorModule;
