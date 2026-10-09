import type { ModuleFactory } from './types.js';
import { createFileModule } from './file.js';

// Moduli disponibili in questa versione dell'agente, per tipo di connettore.
// I moduli 3cad e sap arriveranno qui con la stessa interfaccia.
export const MODULES: Record<string, ModuleFactory> = {
    file: createFileModule,
};

export type { ConnectorModule, ModuleContext, ModuleFactory } from './types.js';
