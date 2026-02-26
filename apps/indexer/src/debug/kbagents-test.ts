import 'dotenv/config';
import { kbAgentsQuery } from '../graphdb/kb-queries.js';

async function run(): Promise<void> {
  // Make sure to set GRAPHDB_* env vars before running.
  const chainId = 1;
  const graphdbCtx = { label: 'debug-kbAgentsQuery', requestId: `debug-${Date.now()}`, timings: [] as any[] };
  const { rows, total, hasMore } = await kbAgentsQuery(
    {
      where: { chainId },
      first: 200,
      skip: 0,
      orderBy: 'bestRank',
      orderDirection: 'DESC',
    },
    graphdbCtx,
  );

  const timings = Array.isArray((graphdbCtx as any).timings) ? (graphdbCtx as any).timings : [];
  const byLabel = new Map<string, number>();
  for (const t of timings) byLabel.set(String(t?.label ?? ''), (byLabel.get(String(t?.label ?? '')) ?? 0) + 1);

  console.log('[debug][kbAgentsQuery]', { chainId, total, hasMore, rowCount: rows.length, timingCount: timings.length });
  console.log('[debug][kbAgentsQuery][timings]', Object.fromEntries(Array.from(byLabel.entries()).sort((a, b) => b[1] - a[1]).slice(0, 15)));
  console.log(
    '[debug][kbAgentsQuery][sample]',
    rows.slice(0, 3).map((r) => ({
      iri: r.iri,
      uaid: r.uaid,
      agentName: r.agentName,
      createdAtTime: r.createdAtTime,
      trustLedgerTotalPoints: r.trustLedgerTotalPoints,
      trustLedgerBadgeCount: r.trustLedgerBadgeCount,
      trustLedgerBadgesLength: r.trustLedgerBadges?.length ?? 0,
      trustLedgerBadges: (r.trustLedgerBadges ?? []).slice(0, 3),
      atiOverallScore: r.atiOverallScore,
    })),
  );
}

run().catch((e) => {
  console.error('[debug][kbAgentsQuery] failed', e);
  process.exitCode = 1;
});

