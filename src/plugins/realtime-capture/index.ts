// SPDX-License-Identifier: GPL-3.0-only
// Real-time Capture Plugin
// Streams OpenCode message events to OpenPlanner in near real time

import type { Plugin } from '@opencode-ai/plugin';
import { tool } from '@opencode-ai/plugin/tool';
import { createHash } from 'node:crypto';

type JsonRecord = Record<string, unknown>;

type OpenPlannerEventEnvelopeV1 = {
  schema: 'openplanner.event.v1';
  id: string;
  ts: string;
  source: string;
  kind: string;
  source_ref?: {
    session?: string;
    message?: string;
  };
  text?: string;
  meta?: JsonRecord;
  extra?: JsonRecord;
};

type OpenPlannerEventIngestRequest = {
  events: OpenPlannerEventEnvelopeV1[];
};

type MessagePart = {
  type?: string;
  text?: string;
};

type MessageSnapshot = {
  info?: {
    id?: string;
    role?: string;
  };
  parts?: readonly MessagePart[];
};

type MessageLookupClient = {
  session?: {
    message?: (input: {
      path: {
        id: string;
        messageID: string;
      };
    }) => Promise<{ data?: MessageSnapshot }>;
  };
};

type EventInfo = {
  id?: string;
  sessionID?: string;
  time?: {
    created?: number;
  };
};

type EventPart = {
  sessionID?: string;
  messageID?: string;
};

type OpenCodeEvent = {
  type: string;
  properties?: {
    info?: EventInfo;
    part?: EventPart;
  };
};

type OpenCodeEventInput = {
  event?: OpenCodeEvent;
};

type StreamConfig = {
  endpoint: string;
  authToken: string | null;
  source: string;
  enabled: boolean;
};

function parseEnabled(value: string | undefined): boolean {
  if (!value) return true;
  const normalized = value.trim().toLowerCase();
  return normalized !== '0' && normalized !== 'false' && normalized !== 'off';
}

function loadStreamConfig(): StreamConfig {
  const endpoint =
    process.env.OPENPLANNER_EVENTS_ENDPOINT ??
    'http://127.0.0.1:8788/api/openplanner/v1/events';
  const authToken = process.env.OPENPLANNER_EVENTS_AUTH_TOKEN ?? null;
  const source = process.env.OPENPLANNER_EVENTS_SOURCE ?? 'opencode.realtime-capture';
  const enabled = parseEnabled(process.env.OPENPLANNER_EVENTS_ENABLED);

  return { endpoint, authToken, source, enabled };
}

function isMessageEventType(type: string): boolean {
  return type === 'message.updated' || type === 'message.removed';
}

function extractSessionId(event: OpenCodeEvent): string | undefined {
  return event.properties?.info?.sessionID ?? event.properties?.part?.sessionID;
}

function extractMessageId(event: OpenCodeEvent): string | undefined {
  return event.properties?.info?.id ?? event.properties?.part?.messageID;
}

function eventTimestampIso(event: OpenCodeEvent): string {
  const ms = event.properties?.info?.time?.created;
  if (typeof ms === 'number' && Number.isFinite(ms)) {
    return new Date(ms).toISOString();
  }
  return new Date().toISOString();
}

function buildEventId(event: OpenCodeEvent, sessionId: string, messageId: string, ts: string): string {
  const seed = `${event.type}|${sessionId}|${messageId}|${ts}`;
  const digest = createHash('sha256').update(seed).digest('hex').slice(0, 24);
  return `opencode-${digest}`;
}

async function resolveMessageText(
  client: MessageLookupClient | undefined,
  sessionId: string,
  messageId: string,
): Promise<{ text?: string; role?: string }> {
  if (!client?.session?.message) return {};

  try {
    const response = await client.session.message({
      path: { id: sessionId, messageID: messageId },
    });
    const snapshot = response.data;

    if (!snapshot) return {};

    const text = (snapshot.parts ?? [])
      .filter((part) => part.type === 'text' && typeof part.text === 'string')
      .map((part) => part.text?.trim() ?? '')
      .filter((value) => value.length > 0)
      .join('\n')
      .trim();

    return {
      text: text.length > 0 ? text : undefined,
      role: snapshot.info?.role,
    };
  } catch {
    return {};
  }
}

async function toEnvelope(
  event: OpenCodeEvent,
  source: string,
  client: MessageLookupClient | undefined,
): Promise<OpenPlannerEventEnvelopeV1 | null> {
  const sessionId = extractSessionId(event);
  const messageId = extractMessageId(event);
  if (!sessionId || !messageId) return null;

  const ts = eventTimestampIso(event);
  const resolved = await resolveMessageText(client, sessionId, messageId);

  return {
    schema: 'openplanner.event.v1',
    id: buildEventId(event, sessionId, messageId, ts),
    ts,
    source,
    kind: event.type,
    source_ref: {
      session: sessionId,
      message: messageId,
    },
    text: resolved.text,
    meta: {
      role: resolved.role,
      eventType: event.type,
    },
    extra: {
      properties: event.properties,
    },
  };
}

async function postToOpenPlanner(config: StreamConfig, envelope: OpenPlannerEventEnvelopeV1): Promise<void> {
  const payload: OpenPlannerEventIngestRequest = {
    events: [envelope],
  };

  const headers: Record<string, string> = {
    'content-type': 'application/json',
  };
  if (config.authToken) {
    headers.authorization = `Bearer ${config.authToken}`;
  }

  const response = await fetch(config.endpoint, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const bodyText = await response.text();
    throw new Error(`OpenPlanner ingest failed (${response.status}): ${bodyText}`);
  }
}

/**
 * Create indexing state
 */
const createIndexingState = () => {
  const stateRef = {
    isStreaming: false,
    startTime: null as Date | null,
    eventsCount: 0,
    forwardedCount: 0,
    skippedCount: 0,
    errorsCount: 0,
  };

  const getState = () => ({ ...stateRef });
  const setStreaming = (isStreaming: boolean) => {
    stateRef.isStreaming = isStreaming;
  };
  const setStartTime = (startTime: Date | null) => {
    stateRef.startTime = startTime;
  };
  const incrementEvents = () => {
    stateRef.eventsCount += 1;
  };
  const incrementForwarded = () => {
    stateRef.forwardedCount += 1;
  };
  const incrementSkipped = () => {
    stateRef.skippedCount += 1;
  };
  const incrementErrors = () => {
    stateRef.errorsCount += 1;
  };
  const resetStats = () => {
    stateRef.eventsCount = 0;
    stateRef.forwardedCount = 0;
    stateRef.skippedCount = 0;
    stateRef.errorsCount = 0;
  };

  return {
    getState,
    setStreaming,
    setStartTime,
    incrementEvents,
    incrementForwarded,
    incrementSkipped,
    incrementErrors,
    resetStats,
  };
};

/**
 * Real-time Indexing Plugin
 */
export const RealtimeCapturePlugin: Plugin = async (pluginContext: unknown) => {
  const config = loadStreamConfig();
  const state = createIndexingState();

  state.setStreaming(config.enabled);
  state.setStartTime(config.enabled ? new Date() : null);

  const lookupClient =
    pluginContext && typeof pluginContext === 'object' && 'client' in pluginContext
      ? (pluginContext.client as MessageLookupClient | undefined)
      : undefined;

  return {
    tool: {
      'get-indexing-status': tool({
        description: 'Get current status of real-time OpenPlanner message streaming',
        args: {},
        async execute() {
          try {
            const currentStateInfo = state.getState();
            const duration = currentStateInfo.startTime
              ? Math.round((Date.now() - currentStateInfo.startTime.getTime()) / 1000)
              : 0;

            return JSON.stringify(
              {
                isStreaming: currentStateInfo.isStreaming,
                startTime: currentStateInfo.startTime?.toISOString(),
                duration,
                eventsSeen: currentStateInfo.eventsCount,
                eventsForwarded: currentStateInfo.forwardedCount,
                eventsSkipped: currentStateInfo.skippedCount,
                errors: currentStateInfo.errorsCount,
                endpoint: config.endpoint,
                source: config.source,
                enabled: config.enabled,
              },
              null,
              2,
            );
          } catch (error) {
            return JSON.stringify(
              {
                success: false,
                error: error instanceof Error ? error.message : String(error),
              },
              null,
              2,
            );
          }
        },
      }),
      'reset-indexing-status': tool({
        description: 'Reset real-time OpenPlanner streaming counters',
        args: {},
        async execute() {
          state.resetStats();
          return JSON.stringify({ ok: true }, null, 2);
        },
      }),
    },

    async event(input: OpenCodeEventInput) {
      const event = input.event;
      if (!event || !state.getState().isStreaming) return;

      state.incrementEvents();

      if (!isMessageEventType(event.type)) {
        state.incrementSkipped();
        return;
      }

      try {
        const envelope = await toEnvelope(event, config.source, lookupClient);
        if (!envelope) {
          state.incrementSkipped();
          return;
        }

        await postToOpenPlanner(config, envelope);
        state.incrementForwarded();
      } catch (error) {
        state.incrementErrors();
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`Failed to forward OpenCode event to OpenPlanner: ${message}`);
      }
    },

    async unload() {},
  };
};

export { RealtimeCapturePlugin as default };
