import { ensureRepositoryExistsOrThrow, getGraphdbConfigFromEnv, queryGraphdb, updateGraphdb, uploadTurtleToRepository } from '../graphdb-http.js';

function analyticsContext(chainId: number): string {
  return `https://www.agentictrust.io/graph/data/analytics/${chainId}`;
}

function chainContext(chainId: number): string {
  return `https://www.agentictrust.io/graph/data/subgraph/${chainId}`;
}

function nowSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

function ttlPrefixes(): string {
  return [
    '@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .',
    '@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .',
    '@prefix prov: <http://www.w3.org/ns/prov#> .',
    '@prefix core: <https://agentictrust.io/ontology/core#> .',
    '@prefix analytics: <https://agentictrust.io/ontology/core/analytics#> .',
    '',
  ].join('\n');
}

function asNumBinding(b: any): number | null {
  const raw = b?.value;
  if (raw == null) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function asStrBinding(b: any): string | null {
  const v = b?.value;
  return typeof v === 'string' && v.trim() ? v : null;
}

/**
 * Materialize a single-valued `analytics:bestRank` integer on each agent in the chain analytics context.
 *
 * Why:
 * - `kbAgents(orderBy: bestRank)` currently has to compute top-K by joining points + ATI + createdAtTime.
 * - If we precompute a rank integer, GraphDB can page with a simple numeric ORDER BY.
 */
export async function materializeBestRankIndexForChain(
  chainId: number,
  opts?: { force?: boolean; minAgeSeconds?: number; pageSize?: number; maxRanks?: number },
): Promise<{ chainId: number; agentCount: number; computedAt: number; skipped: boolean; maxRanks: number }> {
  const cId = Number.isFinite(chainId) ? Math.trunc(chainId) : 0;
  if (!cId) return { chainId: cId, agentCount: 0, computedAt: 0, skipped: true, maxRanks: 0 };

  const { baseUrl, repository, auth } = getGraphdbConfigFromEnv();
  await ensureRepositoryExistsOrThrow(baseUrl, repository, auth);

  const analyticsCtxIri = analyticsContext(cId);
  const ctxIri = chainContext(cId);
  const computedAt = nowSeconds();
  const minAgeSeconds = Math.max(0, Math.trunc(opts?.minAgeSeconds ?? 6 * 60 * 60)); // 6h default
  // GraphDB/HTTP effectively truncates SELECT result sets around ~200 bindings.
  // Page at/below that to allow deterministic OFFSET paging.
  const pageSize = Math.max(50, Math.min(200, Math.trunc(opts?.pageSize ?? 200)));
  const maxRanks = Math.max(200, Math.min(250_000, Math.trunc(opts?.maxRanks ?? 10_000)));
  const indexIri = `https://www.agentictrust.io/id/agent-best-rank-index/${cId}`;

  if (!opts?.force && minAgeSeconds > 0) {
    const check = [
      'PREFIX analytics: <https://agentictrust.io/ontology/core/analytics#>',
      'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>',
      'SELECT (MAX(xsd:integer(?_at)) AS ?at) WHERE {',
      `  GRAPH <${analyticsCtxIri}> {`,
      `    OPTIONAL { <${indexIri}> analytics:bestRankComputedAt ?_at . }`,
      '  }',
      '}',
      'LIMIT 1',
      '',
    ].join('\n');
    const res = await queryGraphdb(baseUrl, repository, auth, check);
    const at = asNumBinding(res?.results?.bindings?.[0]?.at);
    if (at != null && computedAt - at < minAgeSeconds) {
      return { chainId: cId, agentCount: 0, computedAt: at, skipped: true, maxRanks };
    }
  }

  // Clear previous bestRank values (single-valued invariant).
  const del = [
    'PREFIX analytics: <https://agentictrust.io/ontology/core/analytics#>',
    `WITH <${analyticsCtxIri}>`,
    'DELETE { ?agent analytics:bestRank ?r . }',
    'WHERE  { ?agent analytics:bestRank ?r . }',
    ';',
    `WITH <${analyticsCtxIri}>`,
    `DELETE { <${indexIri}> ?p ?o . }`,
    `WHERE  { <${indexIri}> ?p ?o . }`,
    '',
  ].join('\n');
  await updateGraphdb(baseUrl, repository, auth, del);

  const agentPrefix = `https://www.agentictrust.io/id/agent/${cId}/`;
  let agentCount = 0;
  const seenAgents = new Set<string>();

  // Seek-pagination cursor over the rank sort keys:
  // ORDER BY DESC(points) DESC(ati) DESC(createdAtTime) ASC(agentIriStr)
  // OFFSET has been observed to be ignored/truncated in some GraphDB deployments; seek avoids duplicates.
  let lastP: number | null = null;
  let lastA: number | null = null;
  let lastT: number | null = null;
  let lastS: string | null = null;

  for (;;) {
    const seekFilter =
      lastS != null && lastP != null && lastA != null && lastT != null
        ? [
            '  FILTER(',
            `    (?p < ${lastP}) ||`,
            `    (?p = ${lastP} && ?a < ${lastA}) ||`,
            `    (?p = ${lastP} && ?a = ${lastA} && ?t < ${lastT}) ||`,
            `    (?p = ${lastP} && ?a = ${lastA} && ?t = ${lastT} && ?s > "${lastS.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}")`,
            '  )',
          ].join('\n')
        : '';

    const sparql = [
      'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>',
      'PREFIX core: <https://agentictrust.io/ontology/core#>',
      'PREFIX analytics: <https://agentictrust.io/ontology/core/analytics#>',
      '',
      'SELECT ?agent ?p ?a ?t ?s WHERE {',
      `  GRAPH <${ctxIri}> {`,
      '    ?agent a core:AIAgent .',
      `    FILTER(STRSTARTS(STR(?agent), "${agentPrefix}"))`,
      `    BIND(STRAFTER(STR(?agent), "${agentPrefix}") AS ?agentId)`,
      '    FILTER(REGEX(?agentId, "^[0-9]+$"))',
      '    OPTIONAL { ?agent core:createdAtTime ?createdAtTime }',
      '  }',
      `  GRAPH <${analyticsCtxIri}> {`,
      `    BIND(IRI(CONCAT("https://www.agentictrust.io/id/agent-trust-ledger-score/${cId}/", ?agentId)) AS ?tls)`,
      '    OPTIONAL { ?tls analytics:totalPoints ?points }',
      `    BIND(IRI(CONCAT("https://www.agentictrust.io/id/agent-trust-index/${cId}/", ?agentId)) AS ?atiIri)`,
      '    OPTIONAL { ?atiIri analytics:overallScore ?ati }',
      '  }',
      '  BIND(COALESCE(xsd:integer(?points), 0) AS ?p)',
      '  BIND(COALESCE(xsd:integer(?ati), 0) AS ?a)',
      '  BIND(COALESCE(xsd:integer(?createdAtTime), 0) AS ?t)',
      '  BIND(STR(?agent) AS ?s)',
      seekFilter ? seekFilter : '',
      '}',
      'ORDER BY',
      '  DESC(?p)',
      '  DESC(?a)',
      '  DESC(?t)',
      '  ASC(?s)',
      `LIMIT ${pageSize}`,
      '',
    ].join('\n');

    const res = await queryGraphdb(baseUrl, repository, auth, sparql);
    const bindings = Array.isArray(res?.results?.bindings) ? res.results.bindings : [];
    if (!bindings.length) break;

    const lines: string[] = [ttlPrefixes()];
    let progressed = false;
    for (let i = 0; i < bindings.length; i++) {
      const agentIri = asStrBinding(bindings[i]?.agent);
      if (!agentIri) continue;
      if (seenAgents.has(agentIri)) continue;
      seenAgents.add(agentIri);
      const rank = agentCount + 1;
      if (rank > maxRanks) break;
      lines.push(`<${agentIri}> analytics:bestRank ${rank} .`);
      agentCount += 1;
      progressed = true;
    }
    const turtle = lines.join('\n');
    if (turtle.trim()) {
      await uploadTurtleToRepository(baseUrl, repository, auth, { context: analyticsCtxIri, turtle });
    }

    if (agentCount >= maxRanks) break;
    if (!progressed) break;

    // Advance cursor to the last binding in this page (even if it was a duplicate; seek filter is based on sort keys).
    const last = bindings[bindings.length - 1];
    const p = asNumBinding(last?.p);
    const a = asNumBinding(last?.a);
    const t = asNumBinding(last?.t);
    const s = asStrBinding(last?.s);
    if (p == null || a == null || t == null || !s) break;
    lastP = Math.trunc(p);
    lastA = Math.trunc(a);
    lastT = Math.trunc(t);
    lastS = s;
  }

  // Record computedAt marker.
  const marker = [
    ttlPrefixes(),
    `<${indexIri}> a prov:Entity ;`,
    `  analytics:bestRankComputedAt ${computedAt} ;`,
    `  analytics:bestRankMaxRank ${maxRanks} ;`,
    `  analytics:bestRankRankedAgentCount ${agentCount} .`,
    '',
  ].join('\n');
  await uploadTurtleToRepository(baseUrl, repository, auth, { context: analyticsCtxIri, turtle: marker });

  return { chainId: cId, agentCount, computedAt, skipped: false, maxRanks };
}

