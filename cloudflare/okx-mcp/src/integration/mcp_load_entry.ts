// Test-only local HTTP -> actual MCP router -> actual SQLite Durable Object.
// Never imported/exported by the production entrypoint. OAuth is deliberately
// replaced with a fixed test scope; this fixture does NOT attest OAuth.
import {mcpApi} from '../mcp.js';
import {RuntimeSession} from '../runtime_session.js';

export {RuntimeSession};

type FixtureEnv = {
 RUNTIME: {
  idFromName(name:string):unknown;
  get(id:unknown):{fetch(request:Request):Promise<Response>};
 };
};

export default {
 fetch(request:Request,env:FixtureEnv):Promise<Response>{
  if(new URL(request.url).pathname==='/mcp')
   return mcpApi.fetch(request,env as Parameters<typeof mcpApi.fetch>[1],{
    auth:{scope:['mcp:use']},
   });
  return env.RUNTIME.get(env.RUNTIME.idFromName('windows-primary')).fetch(request);
 },
};
