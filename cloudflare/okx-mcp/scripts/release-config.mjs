import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// The only deploy-specific values come from the accepted Git tree and a
// protected GitHub environment secret containing the *fingerprint*, never the
// Research bearer. Existing OAuth KV, Durable Object and bindings survive.
export function releaseConfig(config, sourceTree, researchFingerprint) {
  if (!/^[0-9a-f]{40}$/.test(sourceTree)) throw Error('invalid release source tree');
  if (!config || typeof config !== 'object' || Array.isArray(config)
      || config.name !== 'okx-cloudflare-mcp' || config.main !== 'src/index.ts') {
    throw Error('unexpected Worker configuration');
  }
  const vars = config.vars;
  if (!vars || typeof vars !== 'object' || Array.isArray(vars)
      || !/^[0-9a-f]{64}$/.test(vars.RUNTIME_TOKEN_SHA256)) {
    throw Error('required runtime token fingerprint missing or invalid');
  }
  if (typeof researchFingerprint !== 'string'
      || !/^[0-9a-f]{64}$/.test(researchFingerprint)) {
    throw Error('required protected Research runtime fingerprint missing or invalid');
  }
  if (researchFingerprint === vars.RUNTIME_TOKEN_SHA256) {
    throw Error('Research and Engine runtime fingerprints must be distinct');
  }
  if (Object.hasOwn(vars, 'RELEASE_SOURCE_TREE')) {
    throw Error('release tree must be owned exclusively by exact deploy workflow');
  }
  if (Object.hasOwn(vars, 'RESEARCH_RUNTIME_TOKEN_SHA256')) {
    throw Error('Research fingerprint must be owned exclusively by protected deploy environment');
  }
  if (!Array.isArray(config.kv_namespaces)
      || !config.kv_namespaces.some((binding) => binding.binding === 'OAUTH_KV' && binding.id)
      || !Array.isArray(config.durable_objects?.bindings)
      || !config.durable_objects.bindings.some((binding) => binding.name === 'RUNTIME' && binding.class_name === 'RuntimeSession')) {
    throw Error('required Worker bindings missing');
  }
  return { ...config, vars: {
    ...vars, RELEASE_SOURCE_TREE: sourceTree,
    RESEARCH_RUNTIME_TOKEN_SHA256: researchFingerprint,
  } };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv.length !== 4) throw Error('expected exactly source-tree and output-path');
  const sourceTree = process.argv[2];
  const canonical = JSON.parse(readFileSync('wrangler.jsonc', 'utf8'));
  const config = releaseConfig(canonical, sourceTree,
    process.env.OKX_RESEARCH_RUNTIME_TOKEN_SHA256);
  writeFileSync(process.argv[3], JSON.stringify(config, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  console.log('RELEASE_CONFIG_BINDINGS=VERIFIED');
}
