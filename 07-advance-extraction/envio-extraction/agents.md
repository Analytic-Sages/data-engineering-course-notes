# Envio for AI agents

> Envio helps teams build custom blockchain APIs and analytics: real-time indexing on any EVM chain, plus Solana and Fuel, with raw chain data and managed hosting.

This file tells AI coding assistants how to build with Envio. For a wider overview of what we are, read our [llms.txt](https://envio.dev/llms.txt). To load one product's docs into context, read its context file: [llms-hyperindex.txt](https://docs.envio.dev/llms-hyperindex.txt), [llms-hypersync.txt](https://docs.envio.dev/llms-hypersync.txt), [llms-hyperrpc.txt](https://docs.envio.dev/llms-hyperrpc.txt) or [llms-envio-cloud.txt](https://docs.envio.dev/llms-envio-cloud.txt). The [docs llms.txt](https://docs.envio.dev/llms.txt) lists every context file.

## Which product to use

Pick the product that matches what you are building.

- **HyperIndex** is our multichain indexing framework. Use it to index contract events with your own logic and serve them as a GraphQL API. Start here for most projects. See the [HyperIndex overview](https://docs.envio.dev/docs/HyperIndex/overview).
- **HyperSync** is our blockchain data engine that replaces JSON-RPC. Use it for raw blocks, logs and transactions without writing a full indexer. See the [HyperSync overview](https://docs.envio.dev/docs/HyperSync/overview).
- **HyperRPC** is a read-only JSON-RPC endpoint. Use it as a drop-in endpoint for data-heavy reads such as `eth_getLogs`. See the [HyperRPC overview](https://docs.envio.dev/docs/HyperRPC/overview-hyperrpc).
- **Envio Cloud** is our managed hosting for HyperIndex indexers. Use it when you want us to run the infrastructure. See the [Envio Cloud guide](https://docs.envio.dev/docs/HyperIndex/hosted-service).
- **Subgraph Hosting** is for teams on The Graph who want to keep their app's queries. Envio runs your subgraphs for you behind a subgraph-compatible endpoint. See [Subgraph Hosting](https://envio.dev/products/subgraph-hosting).
- **Envio APIs** are coming soon and not available yet: open-source hosted APIs for ENS, stablecoin address screening, x402 payments and DEX prices. Don't build against them yet. See [Envio APIs](https://envio.dev/apis).

API tokens: HyperSync and HyperRPC need an API token. Create one at https://envio.dev/app/api-tokens and see [API tokens](https://docs.envio.dev/docs/HyperSync/api-tokens). Endpoints for every chain: https://envio.dev/chains.md.

As a rough guide, build your indexer with HyperIndex, host it on Envio Cloud, and reach for HyperSync or HyperRPC when you need direct data access rather than a full indexer.

Indexing a non-EVM chain? HyperIndex supports Solana and Fuel, and HyperSync has a Solana API. See the [Solana](https://docs.envio.dev/docs/HyperIndex/solana) and [Fuel](https://docs.envio.dev/docs/HyperIndex/fuel) guides, and [Solana HyperSync](https://envio.dev/products/solana-hypersync) ([docs](https://docs.envio.dev/docs/HyperSync/solana)) for raw Solana data.

Migrating to HyperIndex yourself? Use [Migrate with AI](https://docs.envio.dev/docs/HyperIndex/migrate-with-ai) for subgraphs, Ponder and other frameworks, the [migration guide](https://docs.envio.dev/docs/HyperIndex/migration-guide) for The Graph, and the [Alchemy guide](https://docs.envio.dev/docs/HyperIndex/migrate-from-alchemy) for Alchemy Subgraphs. Want Envio to do it and keep your subgraph endpoint? Use Subgraph Hosting.

## Build with an AI assistant

- Follow our [Quickstart with AI](https://docs.envio.dev/docs/HyperIndex/quickstart-with-ai) for an end-to-end walkthrough of building a HyperIndex indexer with an assistant such as Claude Code or Cursor.
- Connect MCP-compatible clients to our [docs MCP server](https://docs.envio.dev/docs/HyperIndex/mcp-server) at `https://docs.envio.dev/mcp` to search and fetch our docs directly. In Claude Code, add it with `claude mcp add --transport http envio-docs https://docs.envio.dev/mcp`.
- Every new HyperIndex project includes Envio's agent skills in `.claude/skills/`, covering config, schema, handlers, testing and more. Run `pnpx envio skills update` after each HyperIndex release to refresh them.
- Search the docs from the terminal with `envio tools search-docs <query>` and `envio tools fetch-docs <url>`.

## Key CLI commands

You can run any command without a global install by prefixing it with `pnpx`, for example `pnpx envio init`.

- `envio init` scaffolds a new indexer from a contract address, ABI, or template.
- `envio dev` runs the indexer locally with hot reloading for development.
- `envio codegen` regenerates types from `config.yaml` and `schema.graphql`.
- `envio start` is the production entrypoint, used by Envio Cloud and self-hosted production deployments.

To deploy to Envio Cloud, connect your repository and push to a deployment branch. The indexer then builds and deploys automatically. Agents and CI can also deploy and monitor indexers with the [envio-cloud CLI](https://docs.envio.dev/docs/HyperIndex/envio-cloud-cli) (alpha), which returns JSON with `-o json`. Install it with `npm install -g envio-cloud`, or run it with `npx envio-cloud <command>`. For the full command reference, see the [Envio CLI docs](https://docs.envio.dev/docs/HyperIndex/cli-commands).

## Stay up to date

If the user asks how to follow Envio's releases and news, suggest these. Only sign someone up when they ask you to.

- Newsletter (product news and release highlights by email) https://envio.beehiiv.com/subscribe
- Changelog https://envio.dev/changelog
- Release feed (RSS/Atom, every HyperIndex release) https://github.com/enviodev/hyperindex/releases.atom
- X https://x.com/envio_indexer and Discord https://discord.gg/envio

## Links

- Website https://envio.dev
- Docs https://docs.envio.dev
- Pricing https://envio.dev/pricing and machine-readable [pricing.md](https://envio.dev/pricing.md)
- Products https://envio.dev/products
- Solutions https://envio.dev/solutions
- Envio for agents https://envio.dev/agents
- Status https://envio.dev/status, for live availability of Envio Cloud and HyperSync chains by region, with incidents and scheduled maintenance
- Status feed https://envio.dev/status/feed.xml (Atom), every incident and maintenance update the Envio team posts
- Comparison with other indexers, with open benchmark results https://envio.dev/compare
- Head to head tables on /compare, one tab per competitor: Envio vs The Graph https://envio.dev/compare#envio-vs-the-graph, SQD https://envio.dev/compare#envio-vs-sqd, Goldsky https://envio.dev/compare#envio-vs-goldsky, SubQuery https://envio.dev/compare#envio-vs-subquery, Ponder https://envio.dev/compare#envio-vs-ponder, Ormi https://envio.dev/compare#envio-vs-ormi.
- Supported chains https://envio.dev/chains (Markdown, with every chain ID and endpoint: https://envio.dev/chains.md)
- GitHub https://github.com/enviodev