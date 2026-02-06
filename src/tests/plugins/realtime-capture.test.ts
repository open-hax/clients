import test from 'ava';

import { RealtimeCapturePlugin } from '../../plugins/realtime-capture/index.js';

type FetchCall = {
  readonly url: string;
  readonly body: string;
};

const setupEnv = (): void => {
  process.env.OPENPLANNER_EVENTS_ENABLED = '1';
  process.env.OPENPLANNER_EVENTS_ENDPOINT = 'http://127.0.0.1:8788/api/openplanner/v1/events';
  delete process.env.OPENPLANNER_EVENTS_AUTH_TOKEN;
};

test.serial('realtime capture forwards message.updated events to OpenPlanner', async (t) => {
  setupEnv();

  const calls: FetchCall[] = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (input, init): Promise<Response> => {
    calls.push({
      url: String(input),
      body: String(init?.body ?? ''),
    });
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  const plugin = await RealtimeCapturePlugin(
    {
      client: {
        session: {
          message: async () => ({
            data: {
              info: { id: 'msg-1', role: 'assistant' },
              parts: [{ type: 'text', text: 'hello from stream' }],
            },
          }),
        },
      },
    } as Parameters<typeof RealtimeCapturePlugin>[0],
  );

  try {
    await plugin.event?.({
      event: {
        type: 'message.updated',
        properties: {
          info: {
            id: 'msg-1',
            sessionID: 'ses-1',
            time: { created: 1_700_000_000_000 },
          },
        },
      },
    });

    t.is(calls.length, 1);
    t.is(calls[0]?.url, 'http://127.0.0.1:8788/api/openplanner/v1/events');

    const payload = JSON.parse(calls[0]?.body ?? '') as {
      events: Array<{
        schema: string;
        kind: string;
        source_ref?: {
          session?: string;
          message?: string;
        };
        text?: string;
      }>;
    };

    t.is(payload.events.length, 1);
    t.is(payload.events[0]?.schema, 'openplanner.event.v1');
    t.is(payload.events[0]?.kind, 'message.updated');
    t.is(payload.events[0]?.source_ref?.session, 'ses-1');
    t.is(payload.events[0]?.source_ref?.message, 'msg-1');
    t.is(payload.events[0]?.text, 'hello from stream');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test.serial('realtime capture skips non-message events', async (t) => {
  setupEnv();

  let called = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    called += 1;
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  const plugin = await RealtimeCapturePlugin({} as Parameters<typeof RealtimeCapturePlugin>[0]);

  try {
    await plugin.event?.({ event: { type: 'session.updated', properties: { info: { id: 'ses-1' } } } });
    t.is(called, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
