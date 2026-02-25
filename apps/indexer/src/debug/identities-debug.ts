/**
 * Debug script: list `core:hasIdentity` entries for an agent in the KB graph.
 *
 * Usage:
 *   cd apps/indexer
 *   pnpm exec tsx src/debug/identities-debug.ts
 *   pnpm exec tsx src/debug/identities-debug.ts 1 26433
 *
 * (Set GRAPHDB_* env vars as needed; defaults exist for hosted GraphDB.)
 */
import 'dotenv/config';
import { getGraphdbConfigFromEnv, queryGraphdbWithContext } from '../graphdb/graphdb-http.js';

const chainId = process.argv[2] ? Number(process.argv[2]) : 1;
const agentId = process.argv[3] ? String(process.argv[3]) : '26433';

if (!Number.isFinite(chainId) || chainId <= 0) {
  console.error('[identities-debug] invalid chainId', { value: process.argv[2] });
  process.exitCode = 1;
} else if (!agentId || !/^\d+$/.test(agentId)) {
  console.error('[identities-debug] invalid agentId', { value: process.argv[3] });
  process.exitCode = 1;
} else {
  const AGENT_IRI = `https://www.agentictrust.io/id/agent/${Math.trunc(chainId)}/${agentId}`;
  const KB_CTX = `https://www.agentictrust.io/graph/data/subgraph/${Math.trunc(chainId)}`;

  const sparql = `
PREFIX core: <https://agentictrust.io/ontology/core#>

SELECT ?identity ?type ?did
WHERE {
  VALUES ?agent { <${AGENT_IRI}> }
  GRAPH <${KB_CTX}> {
    ?agent core:hasIdentity ?identity .
    OPTIONAL { ?identity a ?type . }
    OPTIONAL {
      ?identity core:hasIdentifier ?ident .
      ?ident core:protocolIdentifier ?did .
    }
  }
}
ORDER BY ?identity ?type ?did
`.trim();

  async function run(): Promise<void> {
    const { baseUrl, repository, auth } = getGraphdbConfigFromEnv();
    const res = await queryGraphdbWithContext(baseUrl, repository, auth, sparql);
    const bindings = res?.results?.bindings ?? [];
    console.log('[identities-debug]', {
      graphdb: baseUrl,
      repository,
      kbCtx: KB_CTX,
      agentIri: AGENT_IRI,
      bindings: bindings.map((b: any) => ({
        identity: b?.identity?.value,
        type: b?.type?.value,
        did: b?.did?.value,
      })),
    });
  }

  run().catch((e) => {
    console.error('[identities-debug] failed', e);
    process.exitCode = 1;
  });
}

